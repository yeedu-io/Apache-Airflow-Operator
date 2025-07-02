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
from yeedu.hooks.yeedu import YeeduHook, headers
from airflow.utils.decorators import apply_defaults


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YeeduNotebookRunOperator:
    content_status = None
    error_value = None

    @apply_defaults
    def __init__(
        self,
        base_url,
        workspace_id,
        notebook_conf_id,
        tenant_id,
        connection_id,
        token_variable_name,
        restapi_port,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.base_url = base_url
        self.workspace_id = workspace_id
        self.notebook_conf_id = notebook_conf_id
        self.tenant_id = tenant_id
        self.connection_id = connection_id
        self.token_variable_name = token_variable_name
        self.restapi_port = restapi_port
        self.notebook_cells = {}
        self.notebook_executed = True
        self.notebook_id = None
        self.cell_output_data = []
        self.cells_info = {}
        self.execution_times = {}
        self.ws = None
        self.executionCount = 0
        self.hook: YeeduHook = YeeduHook(
            conf_id=self.notebook_conf_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
        )
        self.timeout = (30, 60)
        self.session = self.hook.get_session()

    def create_notebook_instance(self):
        try:
            post_url = self.base_url + f"workspace/{self.workspace_id}/notebook"
            data = {"notebook_conf_id": self.notebook_conf_id}
            post_response = self.hook._api_request("POST", post_url, data)
            status_code = post_response.status_code
            logger.info(f"Create Notebook - POST Status Code: {status_code}")
            logger.debug(f"Create Notebook - POST Response: {post_response.json()}")
            restapi_port = self.restapi_port
            if status_code == 200:
                self.notebook_id = post_response.json().get("notebook_id")
                notebook_run_url = f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}/spark/{self.notebook_id}/run-metrics?type=notebook".replace(
                    f":{restapi_port}/api/v1", ""
                )
                logger.info(
                    "Check Yeedu notebook run status and logs here " + notebook_run_url
                )
                self.get_active_notebook_instances()
                self.wait_for_kernel_status(self.notebook_id)
                self.get_websocket_token()
                return
            else:
                raise Exception(post_response.text)
        except Exception as e:
            logger.error(f"An error occurred during create_notebook_instance: {e}")
            raise e

    def check_notebook_instance_status(self):
        try:
            check_notebook_status_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/{self.notebook_id}"
            )
            logger.debug(f"Checking notebook_instance status of : {self.notebook_id}")
            status = None
            notebook_status_response = self.hook._api_request(
                "GET", url=check_notebook_status_url
            )
            logger.debug(
                f"Notebook_instance GET status response: {notebook_status_response.status_code}"
            )
            if notebook_status_response.status_code == 200:
                status = notebook_status_response.json().get("notebook_status")
                logger.info(f"notebook_status: {status}")
                if status == "STOPPED":
                    logger.info("Notebook is stopped.")
                return status
            else:
                raise Exception(
                    f"Failed to get notebook status. Status code: {notebook_status_response.status_code}"
                )
        except Exception as e:
            logger.error(
                f"An error occurred while checking notebook instance status: {e}"
            )
            raise e

    def get_active_notebook_instances(self):
        TERMINAL_STATES = {"TERMINATED", "STOPPED", "ERROR"}
        MAX_ATTEMPTS = 5
        DELAY_SECONDS = 60
        get_params = {
            "notebook_conf_ids": self.notebook_conf_id,
            "notebook_status": "RUNNING",
            "isActive": "true"
        }
        url = f"{self.base_url}workspace/{self.workspace_id}/notebooks"
        attempts_failure = 0
        try:
            while True:
                try:
                    response = self.session.get(url, headers=headers, params=get_params)
                    status_code = response.status_code
                    logger.info(f"Get Active Notebooks - GET Status Code: {status_code}")
                    logger.info(f"Get Active Notebooks - GET Response: {response.json()}")
                    if status_code == 200:
                        return response.json()['data'][0]['notebook_id']
                    if status_code == 404:
                        logger.info(f"Notebook is not yet running. Retrying after {DELAY_SECONDS} seconds...")
                        time.sleep(DELAY_SECONDS)
                        notebook_status = self.check_notebook_instance_status()
                        if notebook_status in TERMINAL_STATES:
                            notebook_run_url = (
                                f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}"
                                f"/spark/{self.notebook_id}/run-logs?log_type=stderr"
                            ).replace(f":{self.restapi_port}/api/v1", "")
                            raise AirflowException(
                                f"Notebook is in {notebook_status} state.\n"
                                f"Please check notebook logs for detailed error: {notebook_run_url}"
                            )
                        attempts_failure = 0
                        continue
                    attempts_failure += 1
                    logger.error(
                        f"Unexpected response status: {status_code} "
                        f"(attempt {attempts_failure}/{MAX_ATTEMPTS})"
                    )
                    logger.info(f"Sleeping for {DELAY_SECONDS} seconds before retrying...")
                    if attempts_failure >= MAX_ATTEMPTS:
                        raise Exception(f"Max retry attempts reached for status code {status_code}")
                    time.sleep(DELAY_SECONDS)
                except AirflowException as e:
                    raise
                except Exception as e:
                    attempts_failure += 1
                    logger.error(
                        f"Request failed due to exception (attempt {attempts_failure}/{MAX_ATTEMPTS}): {str(e)}"
                    )
                    logger.info(f"Sleeping for {DELAY_SECONDS} seconds before retrying...")
                    if attempts_failure >= MAX_ATTEMPTS:
                        raise Exception(f"Continuous API failure reached the threshold after multiple attempts - {str(e)}")
                    time.sleep(DELAY_SECONDS)
        except Exception as e:
            logger.error(f"An error occurred during get_active_notebook_instances: {str(e)}")
            raise e

    def wait_for_kernel_status(self, notebook_id):
        try:
            kernel_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/{notebook_id}/kernel/startOrGetStatus"
            )
            max_retries = 3
            logger.info("Notebook is starting. Please wait....")
            time.sleep(10)
            for retry in range(1, max_retries + 1):
                kernel_response = self.hook._api_request("POST", kernel_url)
                kernel_info = kernel_response.json().get("kernel_info", {})
                kernel_status = kernel_info.get("kernel_status")
                logger.info(
                    f"Kernel status attempt {retry}/{max_retries}: {kernel_status}"
                )
                if self.check_kernel_status(kernel_status):
                    logger.info("Kernel status matched the desired status.")
                    break
                if retry == max_retries:
                    logger.warning(
                        f"Kernel status did not match the desired status after {max_retries} retries."
                    )
                    raise Exception(
                        f"Kernel status did not match the desired status after {max_retries} retries."
                    )
                else:
                    logger.info(
                        f"Retrying in 10 seconds... (Retry {retry}/{max_retries})"
                    )
                    time.sleep(10)
        except Exception as e:
            logger.error(f"An error occurred while checking kernel status: {e}")
            raise e

    def check_kernel_status(self, status):
        return status in ["idle", "starting", "busy"]

    def get_websocket_token(self):
        try:
            token = headers.get("Authorization").split(" ")[1]
            proxy_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/{self.notebook_id}/kernel/ws"
            )
            proxy_response = self.hook._api_request(
                "GET",
                url=proxy_url,
                params={"yeedu_session": token},
            )
            if proxy_response.status_code == 200:
                logger.debug(f"WebSocket Token Response: {proxy_response.json()}")
                websocket_url = (
                    self.base_url
                    + f"workspace/{self.workspace_id}/notebook/{self.notebook_id}/kernel/ws/yeedu_session/{token}"
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
            logger.error(f"An error occurred while getting WebSocket token: {e}")

    def get_notebook_file_id(self):
        try:
            get_notebook_url = (
                self.base_url + f"workspace/{self.workspace_id}/notebook/conf"
            )
            notebook_conf_response = self.hook._api_request(
                "GET",
                get_notebook_url,
                params={"notebook_conf_id": self.notebook_conf_id},
            )
            if notebook_conf_response.status_code == 200:
                notebook_file_id = notebook_conf_response.json().get("notebook_file_id")
                notebook_language = notebook_conf_response.json().get("spark_job_type", {}).get("language")
                return notebook_file_id, notebook_language
            else:
                logger.warning(
                    f"Failed to get notebook configuration. Status code: {notebook_conf_response.status_code} message: {notebook_conf_response.text}"
                )
                raise Exception(
                    f"Failed to get notebook configuration. Status code: {notebook_conf_response.status_code} message: {notebook_conf_response.text}"
                )
        except Exception as e:
            logger.error(f"An error occurred while getting notebook configuration: {e}")
            raise e

    def get_notebook_code_from_file(self, notebook_file_id):
        try:
            get_notebook_url = (
                self.base_url + f"workspace/file/download"
            )
            notebook_download_response = self.hook._api_request(
                "GET",
                get_notebook_url,
                params={"file_id": notebook_file_id, "workspace_id": self.workspace_id},
            )
            if notebook_download_response.status_code == 200:
                notebook_download_response_json = json.loads(notebook_download_response.text)
                return notebook_download_response_json
            else:
                logger.warning(
                    f"Failed to get notebook configuration. Status code: {notebook_download_response.status_code} message: {notebook_download_response.text}"
                )
                raise Exception(
                    f"Failed to get notebook configuration. Status code: {notebook_download_response.status_code} message: {notebook_download_response.text}"
                )
        except Exception as e:
            logger.error(f"An error occurred while getting notebook configuration: {e}")
            raise e

    def stop_notebook(self):
        try:
            self.close_websocket_connection()
            stop_notebook_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/kill/{self.notebook_id}"
            )
            logger.debug(f"Stopping notebook instance id: {self.notebook_id}")
            notebook_stop_response = self.hook._api_request("POST", stop_notebook_url)
            logger.info(
                f"Stop Notebook - POST Response Status: {notebook_stop_response.status_code}"
            )
            logger.debug(
                f"Stop Notebook - POST Response: {notebook_stop_response.json()}"
            )
            if notebook_stop_response.status_code == 201:
                time.sleep(20)
                if self.check_notebook_instance_status() == "STOPPED":
                    logger.info(
                        f"Notebook instance id: {self.notebook_id} stopped successfully."
                    )
                return notebook_stop_response
            else:
                logger.error(
                    f"Failed to stop notebook. Status code: {notebook_stop_response.status_code}, Message: {notebook_stop_response.text}"
                )
                raise Exception(
                    f"Failed to stop notebook. Status code: {notebook_stop_response.status_code}, Message: {notebook_stop_response.text}"
                )
        except Exception as e:
            logger.error(f"An error occurred while stopping notebook: {e}")
            raise e

    def calculate_cell_run_time(self, start_time: str, end_time: str) -> str:
        try:
            start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
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
            logger.warning(f"Failed to calculate duration: {e}")
            return ""

    def update_notebook_cells(self):
        try:
            msg_id_to_update = self.cell_output_data[0]["msg_id"]
            for cell in self.notebook_json["cells"]:
                if cell.get("cell_uuid") == msg_id_to_update:
                    timing_info = self.execution_times.get(msg_id_to_update, {})
                    start_time = timing_info.get("startTime")
                    end_time = timing_info.get("endTime")
                    logger.info(f"CellRunTime: {self.calculate_cell_run_time(start_time, end_time)}")
                    if start_time:
                        cell["metadata"]["startTime"] = start_time
                    if end_time:
                        cell["metadata"]["endTime"] = end_time
                    cell["metadata"]["lastRunTime"] = self.calculate_cell_run_time(start_time, end_time)
                    cell["outputs"] = copy.deepcopy(self.cell_output_data)
                    self.cell_output_data.clear()
            for cell in self.notebook_json["cells"]:
                for output in cell.get("outputs", []):
                    output.pop("msg_id", None)
                    output.setdefault("output_type", "text")
            update_cell_url = (
                f"{self.base_url}workspace/{self.workspace_id}/notebook/{self.notebook_conf_id}/update"
            )
            update_cells_response = self.hook._api_request(
                "POST", update_cell_url, self.notebook_json
            )
            logger.info(f"Notebook update response status: {update_cells_response.status_code}")
            logger.debug(f"Notebook update response body: {update_cells_response.json()}")
            if update_cells_response.status_code == 201:
                logger.info("Notebook cells updated successfully.")
                return update_cells_response
            else:
                raise Exception(
                    f"Failed to update notebook cells. Status code: {update_cells_response.status_code}, Message: {update_cells_response.text}"
                )
        except Exception as e:
            logger.error(f"An error occurred while updating notebook cells: {e}")
            raise

    def exit_notebook(self, exit_reason):
        try:
            if self.notebook_executed:
                return 0
            logger.info(f"Clearing the notebook cells")
            self.notebook_cells.clear()
            self.stop_notebook()
        except Exception as e:
            logger.error(f"Error while exiting notebook: {e}")
            raise e
        
    def set_execution_count(self, msg_id):
        try:
            for cell in self.notebook_json["cells"]:
                if cell.get("cell_uuid") == msg_id:
                    metadata = cell.setdefault("metadata", {})
                    metadata["executionCount"] = self.executionCount
                    break
        except Exception as e:
            logger.error(f"Failed to set executionCount for cell {msg_id}: {e}")

    def on_message(self, ws, message):
        try:
            response = json.loads(message)
            msg_type = response.get("msg_type", "")
            if msg_type == "execute_result":
                content = response.get("content", {})
                msg_id = response["parent_header"]["msg_id"]
                logger.debug(f"content: {content}")
                plain_data = content.get("data", {}).get("text/plain", "")
                html_data = content.get("data", {}).get("text/html", "")
                image_data = content.get("data", {}).get("image/png")
                if html_data:
                    self.cell_output_data.append(
                        {"msg_id": msg_id, "output_type": "html", "Celloutput": html_data}
                    )
                if not html_data and plain_data:
                    logger.debug(f"Execution Result-text/plain :\n{plain_data}")
                    self.cell_output_data.append(
                        {"msg_id": msg_id, "output_type": "text", "Celloutput": plain_data}
                    )
                if image_data:
                    image_resp_url = f"data:image/png;base64,{image_data}"
                    self.cell_output_data.append(
                        {
                            "msg_id": msg_id,
                            "output_type": "image",
                            "Celloutput": image_resp_url,
                        }
                    )
            elif msg_type == "error":
                content = response.get("content", {})
                error_name = content.get("ename", "")
                self.error_value = content.get("evalue", "")
                traceback = content.get("traceback", [])
                logger.error(f"Error: {error_name} - {self.error_value}")
                logger.error("Traceback:")
                for tb in traceback:
                    logger.error(tb)
            elif msg_type == "execute_input":
                logger.debug(response)
                content = response.get("content", {})
                code_input = content.get("code", "")
                logger.debug(f"Execute Input:\n{code_input}")
            elif msg_type == "stream":
                content = response.get("content", {})
                text_value = content.get("text", "")
                msg_id = response["parent_header"]["msg_id"]
                logger.debug(f"this msg_id is in stream {msg_id}")
                self.cell_output_data.append(
                    {"msg_id": msg_id, "output_type": "text", "Celloutput": text_value}
                )
            elif msg_type == "display_data":
                content = response.get("content", {})
                logger.debug(f"Display Data: {content}")
                msg_id = response["parent_header"]["msg_id"]
                img_resp = response.get("content", {}).get("data", {}).get("image/png")
                text_resp = (
                    response.get("content", {}).get("data", {}).get("text/plain")
                )
                if img_resp:
                    image_url = f"data:image/png;base64,{img_resp}"
                    self.cell_output_data.append(
                        {"msg_id": msg_id, "output_type": "image", "Celloutput": image_url}
                    )
                if text_resp:
                    self.cell_output_data.append(
                        {"msg_id": msg_id, "output_type": "text", "Celloutput": text_resp}
                    )
            elif msg_type == "status":
                execution_state = response.get("content", {}).get("execution_state", "")
                msg_id = response["parent_header"]["msg_id"]
                if execution_state == "idle" and msg_id:
                        end_time = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                        self.execution_times.setdefault(msg_id, {})["endTime"] = end_time
                if self.cell_output_data:
                    self.update_notebook_cells()
            elif msg_type == "execute_reply":
                content = response.get("content", {})
                logger.debug(f"Content {content}")
                self.content_status = content.get("status", "")
                logger.debug(self.content_status)
                msg_id = response["parent_header"]["msg_id"]
                logger.info(f"Message Id: {msg_id}")
                if self.content_status == "ok":
                    try:
                        self.executionCount += 1
                        self.set_execution_count(msg_id)
                        logger.debug("Removing notebook cells ...")
                        self.notebook_cells = [
                            cell
                            for cell in self.notebook_cells
                            if cell.get("msg_id") != msg_id
                        ]
                        logger.debug(
                            f"Notebook cells array after removing {msg_id}: {self.notebook_cells}"
                        )
                    except ValueError:
                        pass
                elif self.content_status == "error":
                    self.error_value = content.get("evalue", "")
                    traceback = content.get("traceback", [])
                    logger.info(traceback)
                    self.notebook_executed = False
                    self.exit_notebook(
                        f"cell with message_id: {msg_id} failed with error: {self.error_value}"
                    )
                else:
                    raise Exception(
                        f"Invalid self.content_status: {self.content_status}"
                    )
        except Exception as e:
            logger.error(f"Unsupported message type: {e}")
            if self.check_notebook_instance_status() not in [
                "STOPPED",
                "TERMINATED",
                "ERROR",
            ]:
                self.exit_notebook(f"Unsupported message_type: {e}")
            raise e

    def on_error(self, ws, error):
        try:
            logger.info(f"WebSocket encountered an error: {error}")
        except Exception as e:
            logger.error(e)
            raise e

    def on_close(self, ws, close_status_code, close_msg):
        try:
            logger.info(f"WebSocket closed {close_status_code} {close_msg}")
        except Exception as e:
            logger.error(e)
            raise e

    def on_open(self, ws):
        logger.info("WebSocket opened")

    def close_websocket_connection(self):
        if self.ws:
            if self.ws.sock and self.ws.sock.connected:
                logger.info("Closing the active WebSocket connection")
                self.ws.close()
                logger.info("WebSocket connection closed")
            else:
                logger.info("No active WebSocket connections")
        else:
            logger.info("WebSocket instance is not initialized")

    def send_execute_request(self, ws, code, session_id, msg_id):
        try:
            start_time = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            self.execution_times[msg_id] = { "startTime": start_time }
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
                },
                "buffers": [],
                "parent_header": {},
                "channel": "shell",
            }
            ws.send(json.dumps(execute_request))
        except Exception as e:
            logger.error(f"Error while sending execute request: {e}")
            raise e

    def connect_websocket(self):
        ws_url = self.get_websocket_token()
        if not ws_url:
            logger.debug("Unable to retrieve WebSocket URL. Exiting...")
            return None
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
                self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, reconnect=5)
        thread = threading.Thread(target=run_forever_in_thread)
        thread.start()
        return self.ws

    def signal_handler(sig, frame):
        logger.debug("Signal received, aborting...")
        rel.abort()

    def execute(self, context: dict):
        try:
            self.create_notebook_instance()
            signal.signal(signal.SIGINT, self.signal_handler)
            self.ws = self.connect_websocket()
            rel.dispatch()
            time.sleep(5)
            notebook_file_id, notebook_language = self.get_notebook_file_id()
            notebook_download_response = self.get_notebook_code_from_file(notebook_file_id)
            self.notebook_json = notebook_download_response
            self.notebook_cells = (
                notebook_download_response.get("cells", [])
            )
            self.cells_info = copy.deepcopy(self.notebook_cells)
            for cell in self.cells_info:
                cell.update({"outputs": []})
            session_id = str(uuid.uuid4())
            for cell in self.notebook_cells:
                code = cell.get("source")
                msg_id = cell.get("cell_uuid")
                if notebook_language.upper() == "SQL":
                    code = f"%%sql\n{code}"
                self.send_execute_request(self.ws, code, session_id, msg_id)
                cell["msg_id"] = msg_id
            while len(self.notebook_cells) > 0:
                time.sleep(10)
                logger.info(
                    "Waiting {} cells to finish".format(len(self.notebook_cells))
                )
                notebook_status = self.check_notebook_instance_status()
                if len(self.notebook_cells) != 0 and (
                    notebook_status == "STOPPED"
                    or not (self.ws.sock and self.ws.sock.connected)
                ):
                    self.notebook_executed = False
                    raise AirflowException(
                        "Connection is lost without executing all the cells. please check logs for more details"
                    )
                if notebook_status in ["TERMINATED", "STOPPED", "ERROR"]:
                    self.notebook_executed = False
                    break
            logger.info(f"notebook_executed: {self.notebook_executed}")
            if self.notebook_executed:
                time.sleep(5)
                self.stop_notebook()
                return 0
            else:
                if self.check_notebook_instance_status() not in [
                    "STOPPED",
                    "TERMINATED",
                    "ERROR",
                ]:
                    logger.debug("Exiting notebook from main function")
                    self.exit_notebook(f"Exiting notebook from main function")
            self.close_websocket_connection()
            if self.content_status == "error":
                raise AirflowException(f"{self.error_value}")
            if notebook_status in ["TERMINATED", "ERROR"]:
                notebook_run_url = f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}/spark/{self.notebook_id}/run-logs?log_type=stderr".replace(
                    f":{self.restapi_port}/api/v1", ""
                )
                raise AirflowException(
                    f"Notebook is in {notebook_status} state. \n Please check notebook logs for detailed error:{notebook_run_url}"
                )
        except Exception as e:
            logger.error(f"Notebook execution failed with error:  {e}")
            raise e
        finally:
            self.close_websocket_connection()
            logger.info("Closing the websocket connection in finally block")
            if self.notebook_id is not None:
                self.notebook_executed = False
                if self.check_notebook_instance_status() not in [
                    "STOPPED",
                    "TERMINATED",
                    "ERROR",
                    "STOPPING"
                ]:
                    logger.info("Exiting notebook in finally block")
                    self.exit_notebook(f"Exiting notebook")
            # Only logout for LDAP or AAD
            try:
                auth_type = self.hook.get_auth_type()
                if auth_type in ["LDAP", "AAD"]:
                    self.hook.yeedu_logout(context)
            except Exception as e:
                logger.warning(f"Logout skipped or failed: {e}")