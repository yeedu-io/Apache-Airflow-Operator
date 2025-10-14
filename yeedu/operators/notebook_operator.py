import copy
import json
import socket
import threading
import time
import uuid
import websocket
import ssl
import rel
import signal
import re
from datetime import datetime, timezone
from airflow.exceptions import AirflowException
from yeedu.hooks.yeedu import YeeduHook


class YeeduNotebookRunOperator:
    content_status = None
    error_value = None

    def __init__(
        self,
        base_url: str,
        workspace_id: int,
        notebook_id: int,
        tenant_id: str,
        connection_id: str,
        token_variable_name: str,
        restapi_port: int,
        arguments: str = None,
        conf: list = None,
        cluster_ids: list = None,
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
        self.arguments = arguments
        self.conf = conf
        self.cluster_ids = cluster_ids or []
        self.notebook_cells = {}
        self.notebook_executed = True
        self.run_id = None
        self.cell_output_data = []
        self.execution_times = {}
        self.notebook_json = {}
        # Cluster bump tracking
        # Start at -1 to indicate using notebook's default cluster
        self.current_cluster_index = -1
        self.should_bump_cluster = False
        self.cluster_bumped_successfully = False
        # Bump pattern from job operator
        self._BUMP_PATTERN = re.compile(
            r'exit code:?\s*(?:137|143|139|134|52)|'
            r'oomkilled|outofmemoryerror|java heap space|'
            r'gc overhead limit exceeded|killed process|'
            r'sigkill|sigterm|sigsegv|segmentation fault|sigabrt|'
            r'sparkexitcode|exceed_max_executor_failures|'
            r'driver_timeout|container killed by yarn for exceeding memory limits',
            re.IGNORECASE
        )
        self.error_name = None
        self.ws = None
        self.executionCount = 0
        # WebSocket connection tracking for native reconnection
        self.ws_connection_permanently_lost = False
        self.ws_last_disconnect_time = None
        self.hook: YeeduHook = YeeduHook(
            conf_id=self.notebook_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
        )
        self.log = logger

    def _should_bump_cluster_from_logs(self, run_id: int) -> bool:
        """
        Inspect logs and workflow errors to decide if this failure qualifies for cluster bump.

        Args:
            run_id (int): The run ID to check logs for

        Returns:
            bool: True if error patterns indicate cluster bump would help, False otherwise
        """
        try:
            wf_errors = "\n".join(
                self.hook.get_notebook_workflow_errors(run_id) or [])

            if self._BUMP_PATTERN.search(wf_errors):
                return True

            stderr = self.hook.get_notebook_logs(
                run_id, "stderr", last_n_lines=1000) or ""

            if self._BUMP_PATTERN.search(stderr):
                return True

            stdout = self.hook.get_notebook_logs(
                run_id, "stdout", last_n_lines=1000) or ""

            if self._BUMP_PATTERN.search(stdout):
                return True

            return False

        except Exception as e:
            self.log.warning(
                f"Error checking logs for cluster bump eligibility: {e}")
            return False

    def _can_bump_cluster(self) -> bool:
        """
        Check if cluster bump is possible (more clusters available).

        Returns:
            bool: True if more clusters are available for bumping, False otherwise
        """
        return (self.cluster_ids and self.current_cluster_index < len(self.cluster_ids) - 1)

    def _should_bump_cluster_from_error(self, error_name: str, error_value: str, traceback: list) -> bool:
        """
        Check if the current error qualifies for cluster bump based on error patterns.
        """
        if not error_name or not error_value:
            return False

        # Combine error information for pattern matching
        error_text = f"{error_name}: {error_value}"
        if traceback:
            error_text += " " + " ".join(traceback)

        return bool(self._BUMP_PATTERN.search(error_text))

    def _update_cluster_and_restart(self):
        """
        Update to the next cluster and restart notebook execution.
        Does NOT create new notebook - that's handled by main execution loop.
        """
        if not self.cluster_ids or self.current_cluster_index >= len(self.cluster_ids) - 1:
            return False

        try:
            self.current_cluster_index += 1
            new_cluster_id = self.cluster_ids[self.current_cluster_index]

            # Stop current notebook instance
            self.stop_notebook()

            self.log.info(
                f"Bumping to cluster {new_cluster_id} (index {self.current_cluster_index})")

            # Update notebook cluster configuration
            self.hook.update_notebook_cluster(self.notebook_id, new_cluster_id)

            # Reset state for new run - DON'T create notebook here, main loop will handle it
            self.should_bump_cluster = False
            self.cluster_bumped_successfully = True
            # Don't set notebook_executed = True here, let restart logic handle it

            return True

        except Exception as e:
            self.log.error(f"Failed to bump cluster: {e}")
            return False

    def create_notebook_instance(self):
        try:
            data = {
                "notebook_id": self.notebook_id,
                "is_background": True
            }
            if self.arguments:
                data['arguments'] = self.arguments
            if self.conf:
                data['conf'] = self.conf
            post_url = self.base_url + \
                f'workspace/{self.workspace_id}/notebook/run'
            data = data

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
                self.poll_notebook_run_status()
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

    def poll_notebook_run_status(self):
        TERMINAL_STATES = {"TERMINATED",
                           "STOPPED", "ERROR", "STOPPING", "DONE"}
        NOTEBOOK_GET_STATUS_DELAY_SECONDS = 10
        try:
            while True:
                # Poll by notebook run status only
                notebook_status = self.check_notebook_instance_status()

                if notebook_status == "RUNNING":
                    self.log.info("Notebook instance is RUNNING. Proceeding.")
                    return

                if notebook_status in TERMINAL_STATES:
                    notebook_run_url = (
                        f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}"
                        f"/spark/run/{self.run_id}/run-logs?log_type=stderr"
                    ).replace(f":{self.restapi_port}/api/v1", "")
                    raise AirflowException(
                        f"Notebook is in {notebook_status} state.\n"
                        f"Please check notebook logs for detailed error: {notebook_run_url}"
                    )

                self.log.info(
                    f"Notebook is in '{notebook_status}' state. Retrying after {NOTEBOOK_GET_STATUS_DELAY_SECONDS} seconds...")
                time.sleep(NOTEBOOK_GET_STATUS_DELAY_SECONDS)
        except Exception as e:
            self.log.error(
                f"An error occurred during poll_notebook_run_status: {str(e)}")
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

    def check_kernel_status(self, status: str) -> bool:
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

    def get_notebook_language(self):
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
                notebook_language = notebook_conf_response.json().get(
                    "spark_job_type", {}).get("language")
                return notebook_language
            else:
                error_msg = f"Failed to get notebook configuration. Status code: {notebook_conf_response.status_code} message: {notebook_conf_response.text}"
                self.log.error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            self.log.error(
                f"An error occurred while getting notebook configuration: {e}")
            raise e

    def get_notebook_code_from_snapshot(self):
        try:
            get_notebook_url = (
                self.base_url + f"workspace/{self.workspace_id}/notebook/{self.notebook_id}/run/{self.run_id}/download"
            )
            notebook_download_response = self.hook._api_request(
                "GET",
                get_notebook_url,
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
            elif notebook_stop_response.status_code == 409:
                # Notebook already stopped - this is OK for cluster bump scenarios
                self.log.info(
                    f"Notebook instance id: {self.run_id} is already stopped.")
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
                cell_metadata.pop("runBy", None)
                cell["metadata"] = cell_metadata

            self.log.info(
                "Cleared all previous outputs and execution metadata from notebook cells.")

            # Persist the cleared notebook
            update_cell_url = (
                f"{self.base_url}workspace/{self.workspace_id}/notebook/{self.notebook_id}/update"
            )

            params = {
                "run_id": self.run_id,
                "save_as_snapshot": "true"
            }

            update_cells_response = self.hook._api_request(
                "POST", update_cell_url, self.notebook_json, params
            )

            self.log.info(
                f"Notebook clear-output update response: {update_cells_response.status_code}")

            if update_cells_response.status_code == 200:
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
            run_by_user = self.hook.get_user_info().json()
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
                    cell["metadata"]["runBy"] = run_by_user.get('username', '')
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
                for output in cell.get("outputs", []):
                    output.pop("msg_id", None)
                    output.setdefault("output_type", "text")
            
            params = {
                "run_id": self.run_id,
                "save_as_snapshot": "true"
            }

            update_cell_url = (
                f"{self.base_url}workspace/{self.workspace_id}/notebook/{self.notebook_id}/update"
            )
            update_cells_response = self.hook._api_request(
                "POST", update_cell_url, self.notebook_json, params
            )

            self.log.info(
                f"Notebook update response status: {update_cells_response.status_code}")

            if update_cells_response.status_code == 200:
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
            # If we receive a message, connection is working - reset disconnection tracking
            if self.ws_last_disconnect_time:
                self.log.info(
                    "WebSocket reconnection successful - received message from server")
                self.ws_connection_permanently_lost = False
                self.ws_last_disconnect_time = None

            response = json.loads(message)
            msg_type = response.get("msg_type", "")

            # Safely get msg_id from parent_header
            parent_header = response.get("parent_header", {})
            msg_id = parent_header.get("msg_id", "")

            self.log.debug(f"Response content: {response}")
            # Skip messages without msg_id (they're usually status/heartbeat messages)
            if not msg_id:
                self.log.debug(f"Skipping message without msg_id: {msg_type}")
                return

            self.log.info(
                f"Received message of type: {msg_type} with message id: ({msg_id})")

            self.wait_for_kernel_status(skip_sleep=True)

            if msg_type == "execute_result":
                content = response.get("content", {})
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

                # Check if this error qualifies for cluster bump
                if self._should_bump_cluster_from_error(self.error_name, self.error_value, traceback):
                    if self._can_bump_cluster():
                        self.log.info(
                            f"Error qualifies for cluster bump: {self.error_name} - {self.error_value}")
                        self.should_bump_cluster = True
                        return  # Exit immediately to trigger cluster bump
                    else:
                        # Final cluster failure - no more clusters available
                        self.log.error(
                            f"Bump-eligible error occurred on final cluster: {self.error_name} - {self.error_value}")
                        self.notebook_executed = False
                        return  # Exit immediately to break the infinite wait

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
                    f"Started code cell execution for message id ({msg_id}) ")

            elif msg_type == "stream":
                content = response.get("content", {})
                text_value = content.get("text", "")
                # msg_id = response["parent_header"]["msg_id"]
                self.log.debug(
                    f"Stream for message id ({msg_id})")

                # Check if stream contains bump-eligible patterns for early detection
                if self._BUMP_PATTERN.search(text_value):
                    if self._can_bump_cluster():
                        self.log.info(
                            f"OOM/Resource error detected in stream: {text_value[:200]}...")
                        self.should_bump_cluster = True
                    else:
                        self.log.error(
                            f"Resource error detected on final cluster: {text_value[:200]}...")
                        # Preserve an informative error for final failure handling
                        if not self.error_name:
                            self.error_name = "ResourceLimitError"
                        if not self.error_value:
                            self.error_value = text_value.strip()[:500]
                        self.should_bump_cluster = False
                        self.notebook_executed = False

                self.cell_output_data.append({
                    "msg_id": msg_id,
                    "output_type": "text",
                    "Celloutput": text_value
                })

            elif msg_type == "display_data":
                content = response.get("content", {})
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

                # Handle kernel restart scenario
                if execution_state == "restarting":
                    self.log.error(
                        "Kernel restarting - marking execution as failed")
                    self.notebook_executed = False
                    if not self.error_name:
                        self.error_name = "KernelRestart"
                    if not self.error_value:
                        self.error_value = "Kernel restarted unexpectedly"
                    return

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

                    # Check if this error qualifies for cluster bump
                    if (self._can_bump_cluster() and self._should_bump_cluster_from_error(self.error_name, self.error_value, traceback)):
                        self.log.info(
                            f"Error qualifies for cluster bump: {self.error_name} - {self.error_value}")
                        self.should_bump_cluster = True
                    else:
                        # No cluster bump available, set notebook as failed
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

                    # Only exit if cluster bump is not possible
                    if not self.should_bump_cluster:
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
        # Track disconnection time for timeout detection
        if not self.ws_connection_permanently_lost:
            self.ws_last_disconnect_time = time.time()
            self.log.info(
                f"WebSocket error detected at '{self.ws_last_disconnect_time}', native reconnection will handle recovery")

    def on_close(self, ws, close_status_code, close_msg):
        try:
            self.log.info(
                f"WebSocket closed with status code: {close_status_code} and message: {close_msg}")

            # Track disconnection time if it's an unexpected closure (not normal close)
            if not self.ws_last_disconnect_time:
                self.ws_last_disconnect_time = time.time()
                self.log.info(
                    f"WebSocket connection closed at : '{datetime.now(timezone.utc).isoformat()}'")

            # Only attempt to close if socket exists
            if hasattr(ws, 'sock') and ws.sock:
                try:
                    # The socket object needs to be properly closed
                    if hasattr(ws.sock, 'sock') and ws.sock.sock:
                        try:
                            self.log.debug(
                                "Closing socket connection with SHUT_RDWR flag (stops both sending and receiving data)")
                            ws.sock.sock.shutdown(socket.SHUT_RDWR)
                        except (OSError, socket.error) as e:
                            self.log.debug(
                                f"Socket shutdown raised an expected error: {e}")
                    ws.close()
                    self.log.debug("WebSocket socket successfully closed")
                except Exception as e:
                    # Socket might already be closed, which is fine
                    self.log.debug(
                        f"Socket already closed or error occurred: {e}")
        except Exception as e:
            self.log.warning(f"Error during WebSocket close handler: {e}")

    def on_open(self, ws):
        self.log.info("WebSocket opened")
        # Reset connection tracking on successful connection
        self.ws_connection_permanently_lost = False
        self.ws_last_disconnect_time = None

    def on_reconnect(self, ws):
        """Called when WebSocket starts a reconnection attempt (native library callback)"""
        self.log.info("WebSocket reconnection initiated by native library")

        if not self.ws_last_disconnect_time:
            self.ws_last_disconnect_time = time.time()

        self.log.info(
            "Native WebSocket library handling reconnection with 5-second delays")

    def is_websocket_reconnection_timed_out(self, max_disconnect_time=60):
        """
        Check if WebSocket has been disconnected for too long.
        Since native reconnection handles retry logic, we only check for prolonged disconnection.

        Args:
            max_disconnect_time: Maximum time (in seconds) to allow disconnection before giving up
        """
        if not self.ws_last_disconnect_time:
            return False

        elapsed_time = time.time() - self.ws_last_disconnect_time

        if elapsed_time > max_disconnect_time:
            self.log.error(
                f"WebSocket disconnected for {elapsed_time:.1f}s (longer than {max_disconnect_time}s limit)")
            self.ws_connection_permanently_lost = True
            return True

        return False

    def close_websocket_connection(self):
        if self.ws:
            if self.ws.sock and hasattr(self.ws.sock, 'connected') and self.ws.sock.connected:
                self.log.info("Closing the active WebSocket connection")
                try:
                    if hasattr(self.ws.sock, 'sock') and self.ws.sock.sock:
                        try:
                            self.log.debug(
                                "Closing socket connection with SHUT_RDWR flag (stops both sending and receiving data)")
                            self.ws.sock.sock.shutdown(socket.SHUT_RDWR)
                        except (OSError, socket.error) as e:
                            self.log.debug(
                                f"Socket shutdown raised an expected error: {e}")
                    self.ws.close()
                    self.log.info("WebSocket connection closed")
                except Exception as e:
                    self.log.warning(f"Error during WebSocket closure: {e}")
            else:
                self.log.info("No active WebSocket connections to close")
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
        Modified connect_websocket method with retry logic and timeout handling
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
            on_reconnect=self.on_reconnect,
        )

        def run_forever_in_thread():
            sslopt = {}
            if self.hook.YEEDU_AIRFLOW_VERIFY_SSL == "true":
                sslopt = {
                    "cert_reqs": ssl.CERT_REQUIRED,
                    "ca_certs": self.hook.YEEDU_SSL_CERT_FILE,
                }
            else:
                sslopt = {"cert_reqs": ssl.CERT_NONE}

            self.ws.run_forever(
                sslopt=sslopt,
                # Native reconnection with 5-second delay between attempts (not max attempts)
                reconnect=5
            )

        thread = threading.Thread(target=run_forever_in_thread)
        thread.daemon = True
        thread.start()
        return self.ws

    def send_execute_request(self, ws, code, session_id, msg_id):
        try:
            start_time = datetime.now(timezone.utc).isoformat(
                timespec='milliseconds').replace('+00:00', 'Z')

            # If code is in an array of string format then join it into a single string
            if isinstance(code, list):
                self.log.debug(
                    f"Code is a list, joining into a single string for message id ({msg_id}) ")
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

    def signal_handler(self, sig, frame):
        signal_name = "SIGINT" if sig == signal.SIGINT else "SIGTERM"
        self.log.info(
            f"Received {signal_name}, performing graceful shutdown...")

        rel.abort()
        self.stop_notebook()
        self.cleanup()

    def cleanup(self):
        """Ensure all resources are properly cleaned up"""
        # Close WebSocket connections
        if hasattr(self, 'ws') and self.ws:
            try:
                # Close the WebSocket connection
                self.close_websocket_connection()
                self.log.info("WebSocket connection closed.")

            except Exception as e:
                self.log.warning(f"Failed to close WebSocket: {e}")

        # Close HTTP sessions
        if hasattr(self, 'hook') and hasattr(self.hook, 'session'):
            try:
                # Explicitly close all session connections
                self.hook.session.close()
                self.log.info("HTTP session closed.")
            except Exception as e:
                self.log.warning(f"Failed to close HTTP session: {e}")

    def execute(self, context: dict):
        try:
            ti = context['ti']

            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)

            if self.conf is None:
                self.conf = []

            self.conf.append(f"spark.yeedu.dag_id={ti.dag_id}")
            self.conf.append(f"spark.yeedu.dag_run_id={ti.run_id}")
            self.conf.append(f"spark.yeedu.task_id={ti.task_id}")
            self.conf.append(f"spark.yeedu.map_index={ti.map_index}")

            self.hook.yeedu_login(context)

            # Initialize cluster usage if cluster_ids provided
            if self.cluster_ids:
                self.log.info(
                    f"Cluster bump enabled, planned clusters in order: {self.cluster_ids}")
                self.log.info(
                    "Starting execution on the notebook's existing cluster configuration")

            # Main execution loop for cluster bumping
            while True:
                try:
                    # Only set cluster if we're bumping (current_cluster_index >= 0)
                    if self.cluster_ids and self.current_cluster_index >= 0:
                        current_cluster_id = self.cluster_ids[self.current_cluster_index]
                        self.log.info(
                            f"Cluster bump attempt {self.current_cluster_index + 1}/{len(self.cluster_ids)}: executing notebook on cluster {current_cluster_id}")
                        self.hook.update_notebook_cluster(
                            self.notebook_id, current_cluster_id)
                    elif self.cluster_ids:
                        self.log.info(
                            "Executing notebook on its existing cluster configuration")
                    else:
                        self.log.info(
                            "Executing notebook (no cluster bump configured)")

                    self.create_notebook_instance()
                    self.ws = self.connect_websocket()
                    rel.dispatch()
                    time.sleep(5)

                    notebook_file_id, notebook_language = self.get_notebook_file_id()

                    notebook_download_response = self.get_notebook_code_from_file(
                        notebook_file_id)

                    self.notebook_json = notebook_download_response
                    self.notebook_cells = notebook_download_response.get(
                        "cells", [])

                    self.clear_notebook_cell_outputs()

                    session_id = str(uuid.uuid4())

                    self.log.debug(
                        f"Starting execution of {len(self.notebook_cells)} cells")

                    # Execute all cells
                    for i in range(0, len(self.notebook_cells)):
                        cell = self.notebook_cells[i]
                        code = cell.get("source", "")
                        msg_id = cell.setdefault(
                            "cell_uuid", str(uuid.uuid4()))

                        self.log.debug(
                            f"Sending execution request for cell {i+1}/{len(self.notebook_cells)} (message id: {msg_id})")

                        if notebook_language.upper() == "SQL":
                            code = f"%%sql\n{code}"

                        self.send_execute_request(
                            self.ws, code, session_id, msg_id)
                        cell["msg_id"] = msg_id

                    while len(self.notebook_cells) > 0:
                        # Check if cluster bump was triggered
                        if self.should_bump_cluster:
                            self.log.info(
                                "Cluster bump triggered, preparing the next cluster")
                            if self._update_cluster_and_restart():
                                self.log.info(
                                    "Cluster bump applied, restarting execution on the new cluster")
                                break  # Break out of cell execution loop to restart with new cluster
                            else:
                                # No more clusters available - final failure
                                self.log.error(
                                    "Cluster bump failed because no further clusters are available")
                                self.notebook_executed = False
                                # break
                                # Force exit from the main loop
                                raise AirflowException(
                                    f"Notebook execution failed on an existing cluster and all {len(self.cluster_ids) if self.cluster_ids else 0} bump clusters. Final error: {self.error_name} - {self.error_value}")

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

                        # Check if disconnection has lasted too long (native reconnection handles retry logic)
                        if not ws_connected:
                            self.is_websocket_reconnection_timed_out()

                        # Check if connection is permanently lost or notebook instance stopped
                        if len(self.notebook_cells) != 0 and (notebook_status == "STOPPED" or
                                                            (not ws_connected and self.ws_connection_permanently_lost)):
                            self.log.debug(
                                "Setting notebook executed flag to False due to permanent connection loss.")
                            self.notebook_executed = False
                            raise AirflowException(
                                f"WebSocket connection is permanently lost due to prolonged disconnection. "
                                "Unable to execute remaining cells. Please check logs for more details"
                            )

                        # Log connection status
                        if not ws_connected and not self.ws_connection_permanently_lost:
                            elapsed = time.time() - self.ws_last_disconnect_time if self.ws_last_disconnect_time else 0
                            self.log.info(
                                f"WebSocket disconnected for {elapsed:.1f}s. Native reconnection handling recovery with 5s delays...")

                        if notebook_status in ["STOPPED", "TERMINATED", "ERROR"]:
                            self.log.debug(
                                "Setting notebook executed flag to False.")
                            self.notebook_executed = False
                            break

                    self.log.debug(
                        f"notebook executed: {self.notebook_executed}")

                    # Check for cluster bump restart FIRST (higher priority than success)
                    if self.cluster_bumped_successfully:
                        bumped_cluster_id = None
                        if self.cluster_ids and self.current_cluster_index >= 0:
                            bumped_cluster_id = self.cluster_ids[self.current_cluster_index]
                        self.log.info(
                            f"Cluster bump applied, restarting execution on cluster {bumped_cluster_id}"
                            if bumped_cluster_id is not None
                            else "Cluster bump applied, restarting execution")
                        self.cluster_bumped_successfully = False
                        # Reset cell execution state for fresh start
                        self.notebook_cells = {}
                        self.cell_output_data = []
                        self.execution_times = {}
                        self.notebook_json = {}
                        # Reset execution state for fresh start
                        self.notebook_executed = True
                        continue

                    # If execution was successful, stop the cluster bumping loop
                    if self.notebook_executed:
                        time.sleep(5)
                        self.stop_notebook()
                        return 0

                    # If execution failed and no cluster bump available, proceed with error handling
                    if self.check_notebook_instance_status() not in ["STOPPED", "TERMINATED", "ERROR"]:
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
                        notebook_run_url = f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}/run/{self.run_id}/run-logs?log_type=stderr".replace(
                            f":{self.restapi_port}/api/v1", ""
                        )
                        raise AirflowException(
                            f"Notebook is in {notebook_status} state. \n Please check notebook logs for detailed error:{notebook_run_url}"
                        )

                    # If no cluster_ids provided, break after first execution
                    if not self.cluster_ids:
                        break

                except Exception as e:
                    # Check if we should try cluster bump for this error
                    if (
                        self.cluster_ids
                        and self.current_cluster_index < len(self.cluster_ids) - 1
                        and (
                            self._should_bump_cluster_from_logs(
                                self.run_id) if self.run_id else False
                        )
                    ):
                        self.log.info(
                            f"Exception qualifies for cluster bump: {e}")
                        if self._update_cluster_and_restart():
                            self.log.info(
                                "Cluster bump applied after exception, retrying execution")
                            continue

                    # Re-raise if no cluster bump available or bump failed
                    raise e

            # If we exhausted all clusters without success
            if self.cluster_ids and self.current_cluster_index >= len(self.cluster_ids) - 1:
                self.log.error(
                    "Cluster bump exhausted all configured clusters without success")
                raise AirflowException(
                    f"Notebook execution failed on existings cluster and all {len(self.cluster_ids)} bump clusters")

        except Exception as e:
            self.log.error(f"Notebook execution failed with error:  {e}")
            raise e
        finally:
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
                    self.exit_notebook(f"Exiting notebook from finally block.")
            # Only logout for LDAP or AAD
            try:
                auth_type = self.hook.yeedu_auth_type
                if auth_type in ["LDAP", "AAD"]:
                    self.hook.yeedu_logout()
            except Exception as e:
                self.log.warning(f"Logout skipped or failed: {e}")

            self.cleanup()
            self.log.info("Cleanup completed in finally block.")
