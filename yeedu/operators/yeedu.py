#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""This module contains Yeedu Operator."""
from typing import Optional, Tuple, Union
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from yeedu.hooks.yeedu import YeeduHook
from yeedu.hooks.yeedu import headers
from airflow.exceptions import AirflowException
from airflow.models import Variable  # Import Variable from airflow.models
import requests
import time
import json
import websocket
import _thread
import uuid
import logging
import copy
import ssl
import threading
import rel
import signal
from urllib.parse import urlparse


# Configure the logging system
logging.basicConfig(level=logging.INFO)  # Set the logging level to INFO

# Create a logger object
logger = logging.getLogger(__name__)

class YeeduOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self, job_url, connection_id, token_variable_name=None, *args, **kwargs
    ):
        """
        Initializes the class with the given parameters.

        Parameters:
        job_url (str): The URL of the Yeedu Notebook or job.
        connection_id (str): The Airflow connection ID. This connection should contain:
            - username (str): The username for the connection.
            - password (str): The password for the connection.
            - hostname (str): The hostname for the connection.
            - extra (dict): Additional parameters in JSON format, including:
                - YEEDU_AIRFLOW_VERIFY_SSL (str): true or false to verify SSL.
                - YEEDU_SSL_CERT_FILE (str): Path to the SSL certificate file.

        *args: Additional positional arguments.
        **kwargs: Additional keyword arguments.
        """
        super().__init__(
            *args,
            **kwargs,
        )
        self.url = self.check_url(job_url)
        self.connection_id = connection_id
        self.token_variable_name = token_variable_name
        (
            self.base_url,
            self.tenant_id,
            self.workspace_id,
            self.job_type,
            self.conf_id,
            self.restapi_port,
        ) = self.extract_ids(self.url)

        self.hook: YeeduHook = YeeduHook(
            conf_id=self.conf_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
        )

    def check_url(self, job_url):
        """
        Checks if the job URL is provided.
        Parameters:
        - job_url (str): The URL for the job.
        Returns:
        - str: The job URL if it is provided.
        Raises:
        - ValueError: If the job URL is not provided (i.e., None).
        """
        if job_url is not None:
            return job_url
        else:
            raise AirflowException(f"url is not set'{job_url}'")

    def extract_ids(self, url):
        parsed_url = urlparse(url)
        restapi_port = urlparse(url).port
        path_segments = parsed_url.path.split("/")
        tenant_id = path_segments[2] if len(path_segments) > 2 else None
        workspace_id = path_segments[4] if len(path_segments) > 4 else None

        if "notebook" in path_segments:
            conf_id = (
                path_segments[path_segments.index("notebook") + 1]
                if len(path_segments) > path_segments.index("notebook") + 1
                else None
            )
            job_type = "notebook"
        elif "conf" in path_segments:
            conf_id = (
                path_segments[path_segments.index("conf") + 2]
                if len(path_segments) > path_segments.index("conf") + 2
                else None
            )
            job_type = "conf"
        elif "healthCheck" in path_segments:
            job_type = "healthCheck"
            conf_id = -1
            workspace_id = -1
        else:
            raise AirflowException(
                "Please provide valid URL to schedule/run Jobs and Notebooks"
            )

        # Construct base URL with :{restapi_port}/api/v1/ appended
        base_url = f"{parsed_url.scheme}://{parsed_url.hostname}:{restapi_port}/api/v1/"

        return (
            base_url,
            tenant_id,
            int(workspace_id),
            job_type,
            int(conf_id),
            int(restapi_port),
        )

    def execute(self, context):
        """
        Execute the YeeduOperator.

        - Submits a job to Yeedu based on the provided configuration ID.
        - Executes the appropriate operator based on the job_type parameter.

        :param context: The execution context.
        :type context: dict
        """
        if self.job_type == "conf":
            self.hook.yeedu_login(context)
            self._execute_job_operator(context)
        elif self.job_type == "notebook":
            self.hook.yeedu_login(context)
            self._execute_notebook_operator(context)
        elif self.job_type == "healthCheck":
            self._execute_heathcheck(context)
        else:
            raise ValueError(f"Invalid operator type: {self.job_type}")

    def _execute_heathcheck(self, context):
        # Create and execute YeeduJobRunOperator
        health_check_operator = YeeduHealthCheckOperator(
            base_url=self.base_url, connection_id=self.connection_id
        )
        health_check_operator.execute()

    def _execute_job_operator(self, context):
        # Create and execute YeeduJobRunOperator
        job_operator = YeeduJobRunOperator(
            job_conf_id=self.conf_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
            restapi_port=self.restapi_port,
        )
        job_operator.execute(context)

    def _execute_notebook_operator(self, context):
        # Create and execute YeeduNotebookRunOperator
        notebook_operator = YeeduNotebookRunOperator(
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            notebook_conf_id=self.conf_id,
            tenant_id=self.tenant_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
            restapi_port=self.restapi_port,
        )
        notebook_operator.execute(context)

        """
YeeduHealthRunOperator
"""


class YeeduHealthCheckOperator:
    """
    YeeduHealthCheckOperator submits a job to Yeedu and waits for its completion.

    :param hostname: Yeedu API hostname (mandatory).
    :type hostname: str

    :param args: Additional positional arguments.
    :param kwargs: Additional keyword arguments.
    """

    template_fields: Tuple[str] = ("job_id",)

    @apply_defaults
    def __init__(
        self,
        base_url: str,
        connection_id: str,
        *args,
        **kwargs,
    ) -> None:
        """
        Initialize the YeeduJobRunOperator.

        :param job_conf_id: The ID of the job configuration in Yeedu (mandatory).
        :param tenant_id: Yeedu API tenant_id. If not provided, retrieved from url provided.
        :param hostname: Yeedu API hostname (mandatory).
        :param workspace_id: The ID of the Yeedu workspace to execute the job within (mandatory).
        """
        super().__init__(*args, **kwargs)
        self.base_url: str = base_url
        self.connection_id = connection_id
        self.hook: YeeduHook = YeeduHook(
            conf_id=None,
            tenant_id=None,
            base_url=self.base_url,
            workspace_id=None,
            connection_id=self.connection_id,
            token_variable_name=None,
        )
        self.job_id: Optional[Union[int, None]] = None

    def execute(self) -> None:
        """
        Execute the YeeduHealthCheckOperator.

        - Hits HealthCheck API

        """
        try:
            health_check_status: str = self.hook.yeedu_health_check()
            logger.info("Health Check Status: %s", health_check_status.status_code)
        except Exception as e:
            raise AirflowException(e)


"""
YeeduJobRunOperator
"""


class YeeduJobRunOperator:
    """
    YeeduJobRunOperator submits a job to Yeedu and waits for its completion.

    :param job_conf_id: The job configuration ID (mandatory).
    :type job_conf_id: str
    :param hostname: Yeedu API hostname (mandatory).
    :type hostname: str
    :param workspace_id: The ID of the Yeedu workspace to execute the job within (mandatory).
    :type workspace_id: int
    :param tenant_id: Yeedu API tenant_id. If not provided, it will be retrieved from Airflow Variables.
    :type token: str

    :param args: Additional positional arguments.
    :param kwargs: Additional keyword arguments.
    """

    template_fields: Tuple[str] = ("job_id",)

    @apply_defaults
    def __init__(
        self,
        job_conf_id: str,
        base_url: str,
        workspace_id: int,
        tenant_id: str,
        connection_id: str,
        token_variable_name: str,
        restapi_port: int,
        *args,
        **kwargs,
    ) -> None:
        """
        Initialize the YeeduJobRunOperator.

        :param job_conf_id: The ID of the job configuration in Yeedu (mandatory).
        :param tenant_id: Yeedu API tenant_id. If not provided, retrieved from Airflow Variables.
        :param hostname: Yeedu API hostname (mandatory).
        :param workspace_id: The ID of the Yeedu workspace to execute the job within (mandatory).
        """
        super().__init__(*args, **kwargs)
        self.job_conf_id: str = job_conf_id
        self.tenant_id: str = tenant_id
        self.base_url: str = base_url
        self.workspace_id: int = workspace_id
        self.connection_id = connection_id
        self.token_variable_name = token_variable_name
        self.restapi_port = restapi_port
        self.hook: YeeduHook = YeeduHook(
            conf_id=self.job_conf_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
        )
        self.job_id: Optional[Union[int, None]] = None

    def execute(self, context: dict) -> None:
        """
        Execute the YeeduJobRunOperator.

        - Submits a job to Yeedu based on the provided configuration ID.
        - Waits for the job to complete and retrieves job logs.

        :param context: The execution context.
        :type context: dict
        """
        try:
            # self.hook.yeedu_login()
            logger.info("Job Config Id: %s", self.job_conf_id)
            job_id = self.hook.submit_job(self.job_conf_id)
            restapi_port = self.restapi_port

            logger.info("Job Submited (Job Id: %s)", job_id)
            job_run_url = f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}/spark/{job_id}/run-metrics?type=spark_job".replace(
                f":{restapi_port}/api/v1", ""
            )
            logger.info("Check Yeedu Job run status and logs here " + job_run_url)
            job_status: str = self.hook.wait_for_completion(job_id)

            logger.info("Final Job Status: %s", job_status)

            job_log_stdout: str = self.hook.get_job_logs(job_id, "stdout")
            job_log_stderr: str = self.hook.get_job_logs(job_id, "stderr")
            job_log: str = " stdout: " + job_log_stdout + " stderr: " + job_log_stderr
            logger.info("Logs for Job ID %s (%s)", job_id, job_log)

            if job_status in ["ERROR", "TERMINATED", "KILLED", "STOPPED"]:
                logger.error(job_log)
                raise AirflowException(job_log)

        except Exception as e:
            raise AirflowException(e)

        finally:
            logger.info("Stopping job in finally")
            job_status = self.hook.get_job_status(job_id).json().get("job_status")
            if job_status not in ["ERROR", "TERMINATED", "KILLED", "STOPPED", "DONE"]:
                self.hook.kill_job(job_id)


"""
YeeduNotebookRunOperator
"""


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
        # self.headers = {'Accept': 'application/json'}
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
        self.ws = None
        self.hook: YeeduHook = YeeduHook(
            conf_id=self.notebook_conf_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
        )

        # default (30, 60) -- connect time: 30, read time: 60 seconds
        # https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
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
            logger.info(f"Checking notebook_instance status of : {self.notebook_id}")

            status = None

            notebook_status_response = self.hook._api_request(
                "GET", url=check_notebook_status_url
            )

            logger.info(
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
        """
        Retrieves active notebook instances and handles their status checking with retries.
        
        Returns:
            str: The notebook_id of the active instance if found
            
        Raises:
            AirflowException: When notebook enters a terminal state (TERMINATED, STOPPED, ERROR)
            Exception: When max retry attempts are reached or other unexpected errors occur
        """
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
                    
                    # Handle unexpected status codes
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
                    # Re-raise AirflowException with the same error message
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

            # logger.info(f"Kernel URL: {kernel_url}")

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

    # Example usage:
    def check_kernel_status(self, status):
        return status in ["idle", "starting", "busy"]

    def get_websocket_token(self):
        try:
            token = headers.get("Authorization").split(" ")[1]

            # Construct WebSocket token URL
            proxy_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/{self.notebook_id}/kernel/ws"
            )

            # hit proxy api to create web socket connection
            proxy_response = self.hook._api_request(
                "GET",
                url=proxy_url,
                params={"yeedu_session": token},
            )

            if proxy_response.status_code == 200:

                logger.debug(f"WebSocket Token Response: {proxy_response.json()}")

                # creating web socket url
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

    def get_code_from_notebook_configuration(self):
        try:
            # Construct notebook configuration URL
            get_notebook_url = (
                self.base_url + f"workspace/{self.workspace_id}/notebook/conf"
            )

            notebook_conf_response = self.hook._api_request(
                "GET",
                get_notebook_url,
                params={"notebook_conf_id": self.notebook_conf_id},
            )

            if notebook_conf_response.status_code == 200:
                return notebook_conf_response
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


    def stop_notebook(self):
        try:
            self.close_websocket_connection()
            stop_notebook_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/kill/{self.notebook_id}"
            )

            logger.debug(f"Stopping notebook instance id: {self.notebook_id}")

            # Use post_request function
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

    def update_notebook_cells(self):
        try:
            cells_info = {"cells": self.cells_info}
            msg_id_to_update = self.cell_output_data[0]["msg_id"]

            # Iterate through cells and update output if msg_id matches
            for cell in cells_info["cells"]:
                if "cell_uuid" in cell and cell["cell_uuid"] == msg_id_to_update:
                    print(f"MSG ID {msg_id_to_update}")
                    print(f"THE OUTPUT VALUE {self.cell_output_data[0]['Celloutput']}")

                    # Extend the current cell's output with all collected outputs
                    cell["output"].extend(self.cell_output_data)

                    self.cell_output_data.clear()

            for cell in cells_info["cells"]:
                if "output" in cell:
                    for output_entry in cell["output"]:
                        if "msg_id" in output_entry:
                            del output_entry["msg_id"]

            print(f"CELLS INFO {cells_info}")
            update_cell_url = (
                self.base_url
                + f"workspace/{self.workspace_id}/notebook/{self.notebook_conf_id}/update"
            )
            data = cells_info
            update_cells_response = self.hook._api_request(
                "POST", update_cell_url, data
            )

            print(f"this is cell response {update_cells_response}")

            logger.info(update_cells_response.status_code)

            logger.debug(
                f"Update Notebook cells - POST Response: {update_cells_response.json()}"
            )

            if update_cells_response.status_code == 201:
                logger.info("Notebook cells updated successfully.")
                self.cell_output_data.clear()
                return update_cells_response
            else:
                logger.error(
                    f"Failed to update notebook cells. Status code: {update_cells_response.status_code}, Message: {update_cells_response.text}"
                )
                raise Exception(
                    f"Failed to update notebook cells. Status code: {update_cells_response.status_code}, Message: {update_cells_response.text}"
                )
        except Exception as e:
            logger.error(f"An error occurred while updating notebook cells: {e}")
            raise e

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

    def on_message(self, ws, message):
        try:
            response = json.loads(message)

            msg_type = response.get("msg_type", "")

            if msg_type == "execute_result":
                content = response.get("content", {})
                msg_id = response["parent_header"]["msg_id"]
                logger.info(f"content: {content}")
                plain_data = content.get("data", {}).get("text/plain", "")
                html_data = content.get("data", {}).get("text/html", "")
                image_data = content.get("data", {}).get("image/png")
                if html_data:
                    self.cell_output_data.append(
                        {"msg_id": msg_id, "type": "html", "Celloutput": html_data}
                    )
                if not html_data and plain_data:
                    logger.info(f"Execution Result-text/plain :\n{plain_data}")
                    self.cell_output_data.append(
                        {"msg_id": msg_id, "type": "text", "Celloutput": plain_data}
                    )
                if image_data:
                    image_resp_url = f"data:image/png;base64,{image_data}"
                    self.cell_output_data.append(
                        {
                            "msg_id": msg_id,
                            "type": "image",
                            "Celloutput": image_resp_url,
                        }
                    )
                self.update_notebook_cells()

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
                print(response)
                content = response.get("content", {})
                code_input = content.get("code", "")
                logger.info(f"Execute Input:\n{code_input}")
            elif msg_type == "stream":
                content = response.get("content", {})
                text_value = content.get("text", "")
                msg_id = response["parent_header"]["msg_id"]
                print(f"this msg_id is in stream {msg_id}")
                self.cell_output_data.append(
                    {"msg_id": msg_id, "type": "text", "Celloutput": text_value}
                )
                print(f"stream output is {self.cell_output_data}")
                self.update_notebook_cells()
            elif msg_type == "display_data":
                content = response.get("content", {})
                logger.info(f"Display Data: {content}")
                msg_id = response["parent_header"]["msg_id"]
                img_resp = response.get("content", {}).get("data", {}).get("image/png")
                text_resp = (
                    response.get("content", {}).get("data", {}).get("text/plain")
                )
                if img_resp:
                    image_url = f"data:image/png;base64,{img_resp}"
                    self.cell_output_data.append(
                        {"msg_id": msg_id, "type": "image", "Celloutput": image_url}
                    )
                if text_resp:
                    self.cell_output_data.append(
                        {"msg_id": msg_id, "type": "text", "Celloutput": text_resp}
                    )
                self.update_notebook_cells()
            elif msg_type == "status":
                execution_state = response.get("content", {}).get("execution_state", "")
                logger.info(f"Execution State: {execution_state}")
            elif msg_type == "execute_reply":
                content = response.get("content", {})
                logger.info(f"Content {content}")
                self.content_status = content.get("status", "")
                logger.info(self.content_status)
                msg_id = response["parent_header"]["msg_id"]
                logger.info(f"Message Id: {msg_id}")
                if self.content_status == "ok":
                    try:
                        logger.info("Removing notebook cells ...")

                        # Update the notebook_cells array after content.status is ok by removing the msg_id under parent_header
                        self.notebook_cells = [
                            cell
                            for cell in self.notebook_cells
                            if cell.get("msg_id") != msg_id
                        ]
                        logger.info(
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
            from datetime import datetime

            current_date = datetime.now()

            execute_request = {
                "header": {
                    "msg_type": "execute_request",
                    "msg_id": msg_id,
                    "username": "username",
                    "session": session_id,
                    "date": current_date.strftime("%Y-%m-%d %H:%M:%S"),
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
            print("Unable to retrieve WebSocket URL. Exiting...")
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

    # Define the signal handler function
    def signal_handler(sig, frame):
        print("Signal received, aborting...")
        rel.abort()  # Stop the event loop and gracefully shut down the WebSocket connection

    def execute(self, context: dict):
        # Send execute requests for notebook cells
        try:
            # self.yeedu_login()
            self.create_notebook_instance()

            signal.signal(signal.SIGINT, self.signal_handler)

            self.ws = self.connect_websocket()

            rel.dispatch()

            time.sleep(5)

            notebook_get_response = self.get_code_from_notebook_configuration()

            self.notebook_cells = (
                notebook_get_response.json().get("notebook_cells", {}).get("cells", [])
            )

            self.cells_info = copy.deepcopy(self.notebook_cells)

            for cell in self.cells_info:
                cell.update({"output": []})

            logger.info(f"Notebook Cells: {self.notebook_cells}")

            session_id = str(uuid.uuid4())

            for cell in self.notebook_cells:
                code = cell.get("code")
                msg_id = cell.get("cell_uuid")

                self.send_execute_request(self.ws, code, session_id, msg_id)

                cell["msg_id"] = msg_id

            # awaiting all the notebook cells executing
            while len(self.notebook_cells) > 0:
                time.sleep(10)
                logger.info(
                    "Waiting {} cells to finish".format(len(self.notebook_cells))
                )

                notebook_status = self.check_notebook_instance_status()

                # handling the case if notebook is abruptly stopped without executing all the cells in a notebook,

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
                    logger.info("Exiting notebook from main function")
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
