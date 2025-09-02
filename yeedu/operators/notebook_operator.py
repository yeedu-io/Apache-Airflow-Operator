import copy
import json
import logging
import threading
import time
import uuid
import websocket
import ssl
import rel
import signal
from datetime import datetime, timezone
from airflow.exceptions import AirflowException
from yeedu.hooks.yeedu import YeeduHook


class YeeduNotebookRunOperator:
    content_status = None
    error_value = None

    def __init__(
        self,
        base_url,
        workspace_id,
        notebook_id,
        tenant_id,
        connection_id,
        token_variable_name,
        restapi_port,
        logger=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.base_url = base_url
        self.workspace_id = workspace_id
        self.notebook_id = notebook_id
        self.tenant_id = tenant_id
        self.connection_id = connection_id
        self.token_variable_name = token_variable_name
        self.restapi_port = restapi_port
        self.notebook_cells = {}
        self.notebook_executed = True
        self.run_id = None
        self.cell_output_data = []
        self.execution_times = {}
        self.notebook_json = {}
        self.error_name = None
        self.ws = None
        self.executionCount = 0
        self.hook: YeeduHook = YeeduHook(
            conf_id=self.notebook_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
        )
        self.timeout = (30, 60)
        self.log = logger

    def create_notebook_instance(self):
        try:
            post_url = self.base_url + \
                f'workspace/{self.workspace_id}/notebook/run'
            data = {'notebook_id': self.notebook_id}

            response = self.hook._api_request("POST", post_url, data)

            status_code = response.status_code

            self.log.debug(f"Create Notebook - Status Code: {status_code}")

            if status_code == 200:
                self.log.debug(
                    f"Create Notebook - Response: {response.json()}")

                self.run_id = response.json().get("run_id")

                notebook_run_url = f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}/run/{self.run_id}/run-metrics?type=notebook".replace(
                    f":{self.restapi_port}/api/v1", ""
                )
                self.log.info(
                    "Check Yeedu notebook run status and logs here " + notebook_run_url
                )
                self.get_active_notebook_instances()
                self.wait_for_kernel_status(skip_sleep=False)
                self.get_websocket_token()
                return
            else:
                raise Exception(response.text)
        except Exception as e:
            self.log.error(
                f"An error occurred during create notebook instance: {e}")
            raise e

    def check_notebook_instance_status(self):
        try:
            check_notebook_status_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/run/{self.run_id}"
            )

            self.log.debug(
                f"Checking notebook instance status of notebook id: {self.run_id}")

            status = None

            notebook_status_response = self.hook._api_request(
                "GET", url=check_notebook_status_url
            )

            if notebook_status_response.status_code == 200:
                status = notebook_status_response.json().get("run_status")
                self.log.debug(f"Notebook instance status: {status}")
                return status
            else:
                raise Exception(
                    f"Failed to get notebook instance status received status code: {notebook_status_response.status_code}"
                )
        except Exception as e:
            self.log.error(
                f"An error occurred while checking notebook instance status: {e}"
            )
            raise e

    def get_active_notebook_instances(self):
        TERMINAL_STATES = {"TERMINATED", "STOPPED", "ERROR"}
        MAX_ATTEMPTS = 60
        DELAY_SECONDS = 5
        get_params = {
            "notebook_ids": self.notebook_id,
            "run_status": "RUNNING",
            "isActive": "true"
        }
        url = f"{self.base_url}workspace/{self.workspace_id}/notebook/runs"
        attempts_failure = 0
        try:
            while True:
                try:
                    # Use the hook's session and headers
                    response = self.hook.session.get(
                        url, headers=self.hook.get_headers(), params=get_params)
                    status_code = response.status_code
                    self.log.debug(
                        f"Get Active Notebooks - Status Code: {status_code}")
                    if status_code == 200:
                        self.log.debug(
                            f"Get Active Notebooks - Response: {response.json()}")
                        return response.json()['data'][0]['run_id']
                    if status_code == 404:
                        self.log.info(
                            f"Notebook is not yet running. Retrying after {DELAY_SECONDS} seconds...")
                        time.sleep(DELAY_SECONDS)
                        notebook_status = self.check_notebook_instance_status()
                        if notebook_status in TERMINAL_STATES:
                            notebook_run_url = (
                                f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}"
                                f"/spark/run/{self.run_id}/run-logs?log_type=stderr"
                            ).replace(f":{self.restapi_port}/api/v1", "")
                            raise AirflowException(
                                f"Notebook is in {notebook_status} state.\n"
                                f"Please check notebook logs for detailed error: {notebook_run_url}"
                            )
                        attempts_failure = 0
                        continue
                    attempts_failure += 1
                    self.log.error(
                        f"Unexpected response status: {status_code} "
                        f"(attempt {attempts_failure}/{MAX_ATTEMPTS})"
                    )
                    self.log.info(
                        f"Sleeping for {DELAY_SECONDS} seconds before retrying...")
                    if attempts_failure >= MAX_ATTEMPTS:
                        raise Exception(
                            f"Max retry attempts reached for status code {status_code}")
                    time.sleep(DELAY_SECONDS)
                except AirflowException as e:
                    raise
                except Exception as e:
                    attempts_failure += 1
                    self.log.error(
                        f"Request failed due to exception (attempt {attempts_failure}/{MAX_ATTEMPTS}): {str(e)}"
                    )
                    self.log.info(
                        f"Sleeping for {DELAY_SECONDS} seconds before retrying...")
                    if attempts_failure >= MAX_ATTEMPTS:
                        raise Exception(
                            f"Continuous API failure reached the threshold after multiple attempts - {str(e)}")
                    time.sleep(DELAY_SECONDS)
        except Exception as e:
            self.log.error(
                f"An error occurred during get_active_notebook_instances: {str(e)}")
            raise e

    def wait_for_kernel_status(self, skip_sleep=False):
        try:
            kernel_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/run/{self.run_id}/kernel/startOrGetStatus"
            )
            max_retries = 3
            if skip_sleep is False:
                self.log.info("Notebook is starting. Please wait....")
                time.sleep(10)

            for retry in range(1, max_retries + 1):
                kernel_response = self.hook._api_request("POST", kernel_url)
                kernel_info = kernel_response.json().get("kernel_info", {})
                kernel_status = kernel_info.get("kernel_status")
                self.log.info(
                    f"Kernel status attempt {retry}/{max_retries}: {kernel_status}"
                )
                if self.check_kernel_status(kernel_status):
                    self.log.info(
                        "Kernel status matched the desired status.")
                    break
                if retry == max_retries:
                    self.log.warning(
                        f"Kernel status did not match the desired status after {max_retries} retries."
                    )
                    raise Exception(
                        f"Kernel status did not match the desired status after {max_retries} retries."
                    )
                else:
                    self.log.info(
                        f"Retrying in 10 seconds... (Retry {retry}/{max_retries})"
                    )
                    time.sleep(10)
        except Exception as e:
            self.log.error(
                f"An error occurred while checking kernel status: {e}")
            raise e

    def check_kernel_status(self, status):
        return status in ["idle", "starting", "busy"]

    def get_websocket_token(self):
        try:
            # Use the hook's headers instead of importing from module level
            token = self.hook.get_headers().get("Authorization").split(" ")[1]
            proxy_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/run/{self.run_id}/kernel/ws"
            )
            proxy_response = self.hook._api_request(
                "GET",
                url=proxy_url,
                params={"yeedu_session": token},
            )
            if proxy_response.status_code == 200:
                self.log.debug(
                    f"WebSocket Token Response: {proxy_response.json()}")
                websocket_url = (
                    self.base_url
                    + f"workspace/{self.workspace_id}/notebook/run/{self.run_id}/kernel/ws/yeedu_session/{token}"
                )
                websocket_url = websocket_url.replace("http://", "ws://").replace(
                    "https://", "wss://"
                )
                return websocket_url
            else:
                raise Exception(
                    f"Failed to get WebSocket token. Status code: {proxy_response.status_code} messsgae: {proxy_response.text}"
                )
        except Exception as e:
            self.log.error(
                f"An error occurred while getting WebSocket token: {e}")

    def get_notebook_file_id(self):
        try:
            get_notebook_url = (
                self.base_url + f"workspace/{self.workspace_id}/notebook"
            )
            notebook_conf_response = self.hook._api_request(
                "GET",
                get_notebook_url,
                params={"notebook_id": self.notebook_id},
            )
            if notebook_conf_response.status_code == 200:
                notebook_file_id = notebook_conf_response.json().get("notebook_file_id")
                notebook_language = notebook_conf_response.json().get(
                    "spark_job_type", {}).get("language")
                return notebook_file_id, notebook_language
            else:
                error_msg = f"Failed to get notebook configuration. Status code: {notebook_conf_response.status_code} message: {notebook_conf_response.text}"
                self.log.error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            self.log.error(
                f"An error occurred while getting notebook configuration: {e}")
            raise e

    def get_notebook_code_from_file(self, notebook_file_id):
        try:
            get_notebook_url = (
                self.base_url + f"workspace/file/download"
            )
            notebook_download_response = self.hook._api_request(
                "GET",
                get_notebook_url,
                params={
                    "file_id": notebook_file_id,
                    "workspace_id": self.workspace_id
                },
            )
            if notebook_download_response.status_code == 200:
                notebook_download_response_json = json.loads(
                    notebook_download_response.text)
                return notebook_download_response_json
            else:
                error_msg = f"Failed to download notebook file. Status code: {notebook_download_response.status_code} message: {notebook_download_response.text}"
                self.log.error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            self.log.error(
                f"An error occurred while downloading notebook file: {e}")
            raise e

    def stop_notebook(self):
        try:
            self.close_websocket_connection()
            stop_notebook_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/run/kill/{self.run_id}"
            )
            self.log.debug(f"Stopping notebook instance id: {self.run_id}")
            notebook_stop_response = self.hook._api_request(
                "POST", stop_notebook_url)
            self.log.info(
                f"Stop Notebook - Response Status code: {notebook_stop_response.status_code}"
            )
            if notebook_stop_response.status_code == 201:
                self.log.debug(
                    f"Stop Notebook - Response: {notebook_stop_response.json()}")
                time.sleep(20)
                if self.check_notebook_instance_status() == "STOPPED":
                    self.log.info(
                        f"Notebook instance id: {self.run_id} stopped successfully."
                    )
                return notebook_stop_response
            else:
                self.log.error(
                    f"Failed to stop notebook. Status code: {notebook_stop_response.status_code}, Message: {notebook_stop_response.text}"
                )
                raise Exception(
                    f"Failed to stop notebook. Status code: {notebook_stop_response.status_code}, Message: {notebook_stop_response.text}"
                )
        except Exception as e:
            self.log.error(
                f"An error occurred while stopping notebook: {e}")
            raise e

    def calculate_cell_run_time(self, start_time: str, end_time: str) -> str:
        try:
            if not start_time or not end_time:
                self.log.warning("Start time or end time is None or empty.")
                return ""

            start_dt = datetime.fromisoformat(
                start_time.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))

            duration = end_dt - start_dt
            total_seconds = duration.total_seconds()

            if total_seconds < 0:
                return ''

            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)
            seconds = int(total_seconds % 60)
            milliseconds = int((total_seconds - int(total_seconds)) * 1000)

            if hours >= 1:
                return f"{hours}h {minutes}m {seconds}s"
            elif minutes >= 1:
                return f"{minutes}m {seconds}s"
            elif seconds >= 1:
                return f"{seconds}s"
            elif milliseconds >= 1:
                return f"{(milliseconds / 1000):.3f}s"
            else:
                return ''
        except Exception as e:
            self.log.error(f"Failed to calculate duration: {e}")
            return ""

    def clear_notebook_cell_outputs(self):
        try:
            for cell in self.notebook_json.get("cells", []):
                # Clear outputs
                cell["outputs"] = []

                # Clear execution metadata
                cell_metadata = cell.setdefault("metadata", {})
                cell_metadata.pop("startTime", None)
                cell_metadata.pop("endTime", None)
                cell_metadata.pop("lastRunTime", None)
                cell["metadata"] = cell_metadata

            self.log.info(
                "Cleared all previous outputs and execution metadata from notebook cells.")

            # Persist the cleared notebook
            update_cell_url = (
                f"{self.base_url}workspace/{self.workspace_id}/notebook/{self.notebook_id}/update"
            )

            update_cells_response = self.hook._api_request(
                "POST", update_cell_url, self.notebook_json
            )

            self.log.info(
                f"Notebook clear-output update response: {update_cells_response.status_code}")

            if update_cells_response.status_code == 201:
                self.log.info("Notebook cells cleared successfully.")
            else:
                raise Exception(
                    f"Failed to clear notebook cells. Status code: {update_cells_response.status_code}, Message: {update_cells_response.text}"
                )

        except Exception as e:
            self.log.error(
                f"An error occurred while clearing notebook cells: {e}")
            raise

    def update_notebook_cells(self):
        try:
            if not self.cell_output_data:
                self.log.info("No cell output data to update")
                return

            if not self.notebook_json or "cells" not in self.notebook_json:
                self.log.warning(
                    "notebook_json not initialized correctly, skipping update")
                return

            msg_id_to_update = self.cell_output_data[0]["msg_id"]
            skip_outputs = False
            MAX_JSON_SIZE = 30 * 1024 * 1024  # 30 MB
            for cell in self.notebook_json["cells"]:
                if cell.get("cell_uuid") == msg_id_to_update:
                    timing_info = self.execution_times.get(
                        msg_id_to_update, {})
                    start_time = timing_info.get("startTime")
                    end_time = timing_info.get("endTime")
                    run_time = self.calculate_cell_run_time(
                        start_time, end_time)
                    self.log.info(
                        f"Cell execution time for message id ({msg_id_to_update}) : {run_time} ")
                    if start_time:
                        cell["metadata"]["startTime"] = start_time
                    if end_time:
                        cell["metadata"]["endTime"] = end_time
                    cell["metadata"]["lastRunTime"] = run_time
                    if not skip_outputs:
                        cell["outputs"] = copy.deepcopy(self.cell_output_data)

                        # Check size after adding outputs
                        current_size = len(json.dumps(
                            self.notebook_json).encode("utf-8"))
                        if current_size > MAX_JSON_SIZE:
                            self.log.warning(
                                "Notebook JSON size exceeds 30MB, trimming output...")

                            # Remove outputs one by one until within limit
                            trimmed_outputs = []
                            for out in cell["outputs"]:
                                trimmed_outputs.append(out)
                                cell["outputs"] = trimmed_outputs
                                size_with_this = len(
                                    json.dumps(self.notebook_json).encode(
                                        "utf-8")
                                )
                                if size_with_this > MAX_JSON_SIZE:
                                    # remove the last output that caused overflow
                                    trimmed_outputs.pop()
                                    skip_outputs = True
                                    break

                            # Add truncation message
                            trunc_msg = {
                                "output_type": "text",
                                "Celloutput": "Output has been truncated to comply with 30MB limit",
                            }
                            trimmed_outputs.append(trunc_msg)
                            cell["outputs"] = trimmed_outputs
                            while (
                                len(json.dumps(self.notebook_json).encode("utf-8"))
                                > MAX_JSON_SIZE
                                and len(trimmed_outputs) > 1
                            ):
                                # remove more outputs if needed (keep only trunc msg)
                                trimmed_outputs.pop(-2)
                                cell["outputs"] = trimmed_outputs

                    self.cell_output_data.clear()
            for cell in self.notebook_json["cells"]:
                cell.pop("msg_id", None)
                for output in cell.get("outputs", []):
                    output.pop("msg_id", None)
                    output.setdefault("output_type", "text")
            update_cell_url = (
                f"{self.base_url}workspace/{self.workspace_id}/notebook/{self.notebook_id}/update"
            )
            update_cells_response = self.hook._api_request(
                "POST", update_cell_url, self.notebook_json
            )

            self.log.info(
                f"Notebook update response status: {update_cells_response.status_code}")

            if update_cells_response.status_code == 201:
                self.log.info("Notebook cells updated successfully.")
                return update_cells_response
            else:
                raise Exception(
                    f"Failed to update notebook cells. Status code: {update_cells_response.status_code}, Message: {update_cells_response.text}"
                )
        except Exception as e:
            self.log.error(
                f"An error occurred while updating notebook cells: {e}")
            raise

    def exit_notebook(self, exit_reason):
        try:
            if self.notebook_executed:
                return 0
            self.log.info(f"Notebook exited. Reason: {exit_reason}")
            self.notebook_cells.clear()
            if self.check_notebook_instance_status() in ["SUBMITTED", "RUNNING"]:
                self.stop_notebook()
        except Exception as e:
            self.log.error(f"Failed to exit notebook: {e}")
            raise e

    def set_execution_count(self, msg_id):
        try:
            for cell in self.notebook_json["cells"]:
                if cell.get("cell_uuid") == msg_id:
                    metadata = cell.setdefault("metadata", {})
                    metadata["executionCount"] = self.executionCount
                    break
        except Exception as e:
            self.log.error(
                f"Failed to set executionCount for cell {msg_id}: {e}")

    def format_error_output(self, traceback):
        """Helper function to format error output as a single string"""
        error_parts = []
        if traceback:
            error_parts.extend(traceback)
        return "\n".join(error_parts)

    def on_message(self, ws, message):
        try:
            response = json.loads(message)
            msg_type = response.get("msg_type", "")
            msg_id = response["parent_header"]["msg_id"]
            self.log.debug(f"Response content: {response}")
            self.log.info(
                f"Received message of type: {msg_type} with message id: ({msg_id})")

            self.wait_for_kernel_status(skip_sleep=True)

            if msg_type == "execute_result":
                content = response.get("content", {})
                self.log.debug(
                    f"Execute result content of message id: ({msg_id}) - {content}")
                plain_data = content.get("data", {}).get("text/plain", "")
                html_data = content.get("data", {}).get("text/html", "")
                image_data = content.get("data", {}).get("image/png")
                if html_data:
                    self.cell_output_data.append({
                        "msg_id": msg_id,
                        "output_type": "html",
                        "Celloutput": html_data
                    })
                if not html_data and plain_data:
                    self.log.debug(
                        f"Execution Result-text/plain :\n{plain_data}")
                    self.cell_output_data.append({
                        "msg_id": msg_id,
                        "output_type": "text",
                        "Celloutput": plain_data
                    })
                if image_data:
                    image_resp_url = f"data:image/png;base64,{image_data}"
                    self.cell_output_data.append({
                        "msg_id": msg_id,
                        "output_type": "image",
                        "Celloutput": image_resp_url,
                    })

            elif msg_type == "error":
                content = response.get("content", {})
                self.error_name = content.get("ename", "")
                self.error_value = content.get("evalue", "")
                traceback = content.get("traceback", [])

                if traceback:
                    formatted_error_output = self.format_error_output(
                        traceback)
                elif self.error_name == 'CleanExit':
                    formatted_error_output = ''
                else:
                    formatted_error_output = f"{self.error_name}: {self.error_value}"

                # Add error output to cell_output_data
                self.cell_output_data.append({
                    "msg_id": msg_id,
                    "output_type": "error",
                    "Celloutput": formatted_error_output
                })

                self.log.error(
                    f"Error for message id ({msg_id}): {self.error_name} - {self.error_value}")

                if traceback:
                    self.log.error("Traceback:")
                    for tb in traceback:
                        self.log.error(tb)

            elif msg_type == "execute_input":
                content = response.get("content", {})
                code_input = content.get("code", "")
                start_time = datetime.now(timezone.utc).isoformat(
                    timespec='milliseconds').replace('+00:00', 'Z')
                self.execution_times[msg_id] = {"startTime": start_time}
                self.log.debug(
                    f"Started code cell execution for message id ({msg_id}):\n{code_input} ")

            elif msg_type == "stream":
                content = response.get("content", {})
                text_value = content.get("text", "")
                msg_id = response["parent_header"]["msg_id"]
                self.log.debug(
                    f"Stream for message id ({msg_id})")
                self.cell_output_data.append({
                    "msg_id": msg_id,
                    "output_type": "text",
                    "Celloutput": text_value
                })

            elif msg_type == "display_data":
                content = response.get("content", {})
                self.log.debug(f"Display Data: {content}")
                img_resp = response.get("content", {}).get(
                    "data", {}).get("image/png")
                text_resp = (
                    response.get("content", {}).get(
                        "data", {}).get("text/plain")
                )
                if img_resp:
                    image_url = f"data:image/png;base64,{img_resp}"
                    self.cell_output_data.append({
                        "msg_id": msg_id,
                        "output_type": "image",
                        "Celloutput": image_url
                    })
                if text_resp:
                    self.cell_output_data.append({
                        "msg_id": msg_id,
                        "output_type": "text",
                        "Celloutput": text_resp
                    })

            elif msg_type == "status":
                execution_state = response.get(
                    "content", {}).get("execution_state", "")
                if execution_state == "idle" and msg_id:
                    end_time = datetime.now(timezone.utc).isoformat(
                        timespec='milliseconds').replace('+00:00', 'Z')
                    self.execution_times.setdefault(
                        msg_id, {})["endTime"] = end_time
                    if self.cell_output_data:
                        self.update_notebook_cells()
                    elif response.get("parent_header", {}).get("msg_type", {}) != "kernel_info_request":
                        self.log.debug(
                            f"No cell output data for message id ({msg_id}), adding empty output")
                        self.cell_output_data.append({
                            "msg_id": msg_id,
                            "output_type": "text",
                            "Celloutput": ''
                        })
                        self.update_notebook_cells()

            elif msg_type == "execute_reply":
                content = response.get("content", {})
                self.content_status = content.get("status", "")
                self.error_name = content.get("ename", "")

                self.log.debug(
                    f"Execute reply content for message id ({msg_id}) : {content}")

                if self.content_status == "ok":
                    try:
                        self.executionCount += 1
                        self.set_execution_count(msg_id)
                        self.log.debug(
                            "Cell execution successful, removing from queue")
                        self.notebook_cells = [
                            cell
                            for cell in self.notebook_cells
                            if cell.get("msg_id") != msg_id
                        ]
                        self.log.debug(
                            f"Notebook cells array length after removing cell with message id ({msg_id}): {len(self.notebook_cells)}"
                        )
                    except ValueError:
                        pass

                elif self.content_status == "error":
                    self.error_value = content.get("evalue", "")
                    traceback = content.get("traceback", [])
                    self.log.debug(
                        "Setting notebook executed flag to False due to error status.")
                    self.notebook_executed = False

                    if traceback:
                        formatted_error_output = self.format_error_output(
                            traceback)
                    elif self.error_name == 'CleanExit':
                        formatted_error_output = ''
                    else:
                        formatted_error_output = f"{self.error_name}: {self.error_value}"

                    # Add error output to cell_output_data
                    self.cell_output_data.append({
                        "msg_id": msg_id,
                        "output_type": "error",
                        "Celloutput": formatted_error_output
                    })

                    self.log.error(
                        f"Error for message id ({msg_id}): {self.error_name} - {self.error_value}")

                    if traceback:
                        self.log.error("Traceback: ")
                        for tb in traceback:
                            self.log.error(tb)

                    end_time = datetime.now(timezone.utc).isoformat(
                        timespec='milliseconds').replace('+00:00', 'Z')
                    self.execution_times.setdefault(
                        msg_id, {})["endTime"] = end_time

                    self.update_notebook_cells()

                    self.exit_notebook(
                        f"Exiting due to 'error' status in 'execute_reply' message type. The cell with message ID ({msg_id}) failed with error: {self.error_name} - {self.error_value}."
                    )

                elif self.content_status == "aborted":
                    self.log.warning(
                        f"Cell execution was aborted for message id ({msg_id})")
                    self.log.debug(
                        "Setting notebook executed flag to False due to cell abort.")
                    self.notebook_executed = False

                else:
                    raise Exception(
                        f"Invalid self.content_status: {self.content_status}"
                    )

        except Exception as e:
            self.log.error(f"Unsupported message type encountered: {e}")
            if self.check_notebook_instance_status() not in ["STOPPED", "TERMINATED", "ERROR"]:
                self.exit_notebook(
                    f"Exiting due to unsupported message type: {e}")

            raise e

    def on_error(self, ws, error):
        self.log.error(f"WebSocket encountered an error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        self.log.info(
            f"WebSocket closed with status code: {close_status_code} and message: {close_msg}")

    def on_open(self, ws):
        self.log.info("WebSocket opened")

    def close_websocket_connection(self):
        if self.ws:
            if self.ws.sock and self.ws.sock.connected:
                self.log.info("Closing the active WebSocket connection")
                self.ws.close()
                self.log.info("WebSocket connection closed")
            else:
                self.log.info("No active WebSocket connections")
        else:
            self.log.info("WebSocket instance is not initialized")

    def test_websocket_connection_with_retry(self, ws_url, max_duration=600):
        """
        Test WebSocket connection with retry logic for up to 10 minutes.

        Args:
            ws_url: WebSocket URL to test
            max_duration: Maximum time to retry in seconds (default: 600 = 10 minutes)

        Returns:
            bool: True if connection successful, False if all retries failed

        Raises:
            Exception: For non-recoverable errors (auth failures, invalid URL, etc.)
        """
        start_time = time.time()
        retry_delay = 5  # Start with 5 seconds
        max_retry_delay = 60  # Cap at 60 seconds
        attempt = 0

        while (time.time() - start_time) < max_duration:
            attempt += 1
            connection_successful = False
            connection_event = threading.Event()
            error_message = None
            test_ws = None

            def test_on_open(ws):
                nonlocal connection_successful
                self.log.info(
                    f"WebSocket test connection successful on attempt {attempt}")
                connection_successful = True
                connection_event.set()

            def test_on_error(ws, error):
                nonlocal error_message
                error_message = str(error)
                self.log.error(
                    f"WebSocket test connection error on attempt {attempt}: {error}")
                connection_event.set()

            def test_on_close(ws, close_status_code, close_msg):
                connection_event.set()

            try:
                self.log.info(
                    f"Testing WebSocket connection (attempt {attempt})...")
                test_ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=test_on_open,
                    on_error=test_on_error,
                    on_close=test_on_close
                )

                def run_test():
                    if self.hook.YEEDU_AIRFLOW_VERIFY_SSL == "true":
                        test_ws.run_forever(
                            sslopt={
                                "cert_reqs": ssl.CERT_REQUIRED,
                                "ca_certs": self.hook.YEEDU_SSL_CERT_FILE,
                            },
                            skip_utf8_validation=True
                        )
                    elif self.hook.YEEDU_AIRFLOW_VERIFY_SSL == "false":
                        test_ws.run_forever(
                            sslopt={"cert_reqs": ssl.CERT_NONE},
                            skip_utf8_validation=True
                        )

                # Run test in thread with timeout
                test_thread = threading.Thread(target=run_test)
                test_thread.daemon = True
                test_thread.start()

                # Wait for connection result or timeout
                # 30 second timeout per attempt
                if connection_event.wait(timeout=30):
                    if connection_successful:
                        self.log.info(
                            "WebSocket connection test successful")
                        # Safely close the connection
                        if test_ws:
                            try:
                                test_ws.close()
                            except Exception as close_error:
                                self.log.error(
                                    f"Error closing test websocket: {close_error}")
                        return True
                    else:
                        # Check for non-recoverable errors
                        if error_message and any(msg in error_message.lower() for msg in
                                                 ['401', '403', 'unauthorized', 'forbidden', 'invalid url', 'invalid uri']):
                            raise Exception(
                                f"Non-recoverable WebSocket error: {error_message}")
                else:
                    self.log.warning(
                        f"WebSocket test connection timed out on attempt {attempt}")
                    # Cleanup the websocket if timeout occurs
                    if test_ws:
                        try:
                            test_ws.close()
                        except Exception as close_error:
                            self.log.error(
                                f"Error closing test websocket after timeout: {close_error}")

            except Exception as e:
                self.log.error(f"WebSocket test connection exception: {e}")
                # Re-raise non-recoverable errors
                if any(msg in str(e).lower() for msg in
                       ['401', '403', 'unauthorized', 'forbidden', 'invalid url', 'invalid uri']):
                    raise

            # Calculate time remaining
            time_elapsed = time.time() - start_time
            time_remaining = max_duration - time_elapsed

            if time_remaining <= 0:
                break

            # Calculate next retry delay with exponential backoff
            actual_delay = min(retry_delay, time_remaining)
            self.log.info(f"Retrying in {actual_delay} seconds... "
                          f"(Time elapsed: {int(time_elapsed)}s, Time remaining: {int(time_remaining)}s)")
            time.sleep(actual_delay)

            # Exponential backoff
            retry_delay = min(retry_delay * 2, max_retry_delay)

        self.log.error(
            f"WebSocket connection failed after {attempt} attempts over {int(time.time() - start_time)} seconds")
        return False

    def connect_websocket(self):
        """
        Modified connect_websocket method with retry logic
        """
        ws_url = self.get_websocket_token()
        if not ws_url:
            self.log.error("Unable to retrieve WebSocket URL. Exiting...")
            raise Exception("Failed to get WebSocket URL")

        # Test connection with retry before establishing persistent connection
        self.log.info(
            "Testing WebSocket connection before establishing persistent connection...")
        if not self.test_websocket_connection_with_retry(ws_url):
            raise Exception(
                "Failed to establish WebSocket connection after 10 minutes of retrying")

        # Connection test passed, proceed with actual connection
        self.log.info("Establishing persistent WebSocket connection...")
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        def run_forever_in_thread():
            if self.hook.YEEDU_AIRFLOW_VERIFY_SSL == "true":
                self.ws.run_forever(
                    sslopt={
                        "cert_reqs": ssl.CERT_REQUIRED,
                        "ca_certs": self.hook.YEEDU_SSL_CERT_FILE,
                    },
                    reconnect=5,
                )
            elif self.hook.YEEDU_AIRFLOW_VERIFY_SSL == "false":
                self.ws.run_forever(
                    sslopt={"cert_reqs": ssl.CERT_NONE}, reconnect=5)

        thread = threading.Thread(target=run_forever_in_thread)
        thread.start()
        return self.ws

    def send_execute_request(self, ws, code, session_id, msg_id):
        try:
            start_time = datetime.now(timezone.utc).isoformat(
                timespec='milliseconds').replace('+00:00', 'Z')

            # If code is in an array of string format then join it into a single string
            if isinstance(code, list):
                code = "".join(code)

            execute_request = {
                "header": {
                    "msg_type": "execute_request",
                    "msg_id": msg_id,
                    "username": "username",
                    "session": session_id,
                    "date": start_time,
                    "version": "5.3",
                },
                "metadata": {},
                "content": {
                    "code": code,
                    "silent": False,
                    "store_history": True,
                    "user_expressions": {},
                    "allow_stdin": False,
                    # A boolean flag, which, if True, aborts the execution queue if an exception is encountered.
                    # If False, queued execute_requests will execute even if this request generates an exception.
                    # Reference Link: https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute
                    "stop_on_error": True,
                },
                "buffers": [],
                "parent_header": {},
                "channel": "shell",
            }

            execute_request_json = json.dumps(execute_request)
            self.log.debug(
                f"Sending execute request for cell with message id ({msg_id}): {execute_request_json}")
            ws.send(execute_request_json)
        except Exception as e:
            self.log.error(f"Error while sending execute request: {e}")
            raise e

    def signal_handler(sig, frame):
        print("Signal received, aborting...")
        rel.abort()

    def execute(self, context: dict):
        try:
            self.hook.yeedu_login(context)
            self.create_notebook_instance()
            signal.signal(signal.SIGINT, self.signal_handler)
            self.ws = self.connect_websocket()
            rel.dispatch()
            time.sleep(5)

            notebook_file_id, notebook_language = self.get_notebook_file_id()

            notebook_download_response = self.get_notebook_code_from_file(
                notebook_file_id)

            self.notebook_json = notebook_download_response
            self.notebook_cells = notebook_download_response.get("cells", [])

            self.clear_notebook_cell_outputs()

            session_id = str(uuid.uuid4())

            self.log.debug(
                f"Starting execution of {len(self.notebook_cells)} cells")

            for i, cell in enumerate(self.notebook_cells):

                code = cell.get("source", "")
                msg_id = cell.setdefault("cell_uuid", str(uuid.uuid4()))

                self.log.debug(
                    f"Sending execution request for cell {i+1}/{len(self.notebook_cells)} (message id: {msg_id})")

                if notebook_language.upper() == "SQL":
                    code = f"%%sql\n{code}"

                self.send_execute_request(self.ws, code, session_id, msg_id)
                cell["msg_id"] = msg_id

            while len(self.notebook_cells) > 0:

                if not self.notebook_executed:
                    self.log.error(
                        "Cell execution failed, stopping execution")
                    break

                time.sleep(10)

                self.log.info(
                    f"Waiting for {len(self.notebook_cells)} cell(s) to finish execution.")

                self.wait_for_kernel_status(skip_sleep=True)
                for cell in self.notebook_cells:
                    self.log.debug(
                        f'Waiting for this cell id ({cell.get("cell_uuid")}) to finish execution.')

                notebook_status = self.check_notebook_instance_status()

                ws_connected = self.ws and self.ws.sock and self.ws.sock.connected

                if len(self.notebook_cells) != 0 and (notebook_status == "STOPPED" or not ws_connected):
                    self.log.debug(
                        "Setting notebook executed flag to False due to connection loss.")
                    self.notebook_executed = False
                    raise AirflowException(
                        "Connection is lost without executing all the cells. please check logs for more details"
                    )

                if notebook_status in ["STOPPED", "TERMINATED", "ERROR"]:
                    self.log.debug(
                        "Setting notebook executed flag to False.")
                    self.notebook_executed = False
                    break

            self.log.debug(f"notebook executed: {self.notebook_executed}")

            if self.notebook_executed:
                time.sleep(5)
                self.stop_notebook()
                return 0
            elif self.check_notebook_instance_status() not in ["STOPPED", "TERMINATED", "ERROR"]:
                self.log.debug(
                    "Exiting notebook due to cell execution failure.")
                self.exit_notebook(
                    f"Cell execution failed with error: {self.error_name} - {self.error_value}"
                )

            self.close_websocket_connection()

            if self.content_status == "error" or self.notebook_executed is False:
                # Kept to catch the error raised from dbutils.notebook.exit()
                if self.error_name is not None and self.error_name == "CleanExit":
                    self.log.info(
                        f"Notebook execution completed with exit message: '{self.error_value}'")
                else:
                    raise AirflowException(
                        f"{self.error_name} - {self.error_value}")

            notebook_status = self.check_notebook_instance_status()

            if notebook_status in ["TERMINATED", "ERROR"]:
                notebook_run_url = f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}/spark/run/{self.run_id}/run-logs?log_type=stderr".replace(
                    f":{self.restapi_port}/api/v1", ""
                )
                raise AirflowException(
                    f"Notebook is in {notebook_status} state. \n Please check notebook logs for detailed error:{notebook_run_url}"
                )

        except Exception as e:
            self.log.error(f"Notebook execution failed with error:  {e}")
            raise e
        finally:
            self.close_websocket_connection()
            self.log.info("WebSocket connection closed in finally block.")
            if self.run_id is not None:
                self.log.debug(
                    "Setting notebook executed flag to False in finally block.")
                self.notebook_executed = False
                if self.check_notebook_instance_status() not in [
                    "STOPPED",
                    "TERMINATED",
                    "ERROR",
                    "STOPPING"
                ]:
                    self.log.info("Exiting notebook from finally block.")
                    self.exit_notebook(f"Exiting notebook from finally block.")
            # Only logout for LDAP or AAD
            try:
                auth_type = self.hook.get_auth_type()
                if auth_type in ["LDAP", "AAD"]:
                    self.hook.yeedu_logout(context)
            except Exception as e:
                self.log.warning(f"Logout skipped or failed: {e}")
