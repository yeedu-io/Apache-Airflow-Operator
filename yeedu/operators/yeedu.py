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

# Configure the logging system
logging.basicConfig(level=logging.INFO)  # Set the logging level to INFO

# Create a logger object
logger = logging.getLogger(__name__)


class YeeduOperator(BaseOperator):
    """
    YeeduOperator submits a job to Yeedu based on the provided configuration ID.

    :param conf_id: The configuration ID (mandatory).
    :type conf_id: str
    :param tenant_id: Yeedu API tenant ID (optional).
    :type tenant_id: str
    :param base_url: Yeedu API base URL (optional).
    :type base_url: str
    :param workspace_id: The ID of the Yeedu workspace (optional).
    :type workspace_id: int
    :param args: Additional positional arguments.
    :param kwargs: Additional keyword arguments.
    """
    @apply_defaults
    def __init__(self, conf_id, tenant_id, base_url, workspace_id, *args, **kwargs):
        """
        Initialize the YeeduOperator.

        :param conf_id: The ID of the configuration (job or notebook) in Yeedu (mandatory).
        :param tenant_id: Yeedu API tenant ID (optional).
        :param base_url: Yeedu API base URL (optional).
        :param workspace_id: The ID of the Yeedu workspace (optional).
        """
        super().__init__(*args, **kwargs,)
        self.conf_id = conf_id
        self.tenant_id = tenant_id
        self.base_url = base_url
        self.workspace_id = workspace_id
        self.hook: YeeduHook = YeeduHook(conf_id=self.conf_id, tenant_id=self.tenant_id, base_url=self.base_url, workspace_id=self.workspace_id)

    
    def execute(self, context):
        """
        Execute the YeeduOperator.

        - Submits a job to Yeedu based on the provided configuration ID.
        - Executes the appropriate operator based on the job_type parameter.

        :param context: The execution context.
        :type context: dict
        """
        self.hook.yeedu_login()
        job_type = self.hook.get_job_type()
        if job_type == 'job':
            self._execute_job_operator(context)
        elif job_type == 'notebook':
            self._execute_notebook_operator(context)
        else:
            raise ValueError(f"Invalid operator type: {job_type}")

    def _execute_job_operator(self, context):
        # Create and execute YeeduJobRunOperator
        job_operator = YeeduJobRunOperator(
            job_conf_id=self.conf_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id
        )
        job_operator.execute(context)

    def _execute_notebook_operator(self, context):
        # Create and execute YeeduNotebookRunOperator
        notebook_operator = YeeduNotebookRunOperator(
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            notebook_conf_id=self.conf_id,
            tenant_id=self.tenant_id
        )
        notebook_operator.execute(context)

class YeeduJobRunOperator():
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
        self.hook: YeeduHook = YeeduHook(conf_id = self.job_conf_id, tenant_id=self.tenant_id, base_url=self.base_url, workspace_id=self.workspace_id)
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
            #self.hook.yeedu_login()
            logger.info("Job Config Id: %s",self.job_conf_id)
            job_id = self.hook.submit_job(self.job_conf_id)

            logger.info("Job Submited (Job Id: %s)", job_id)
            job_status: str = self.hook.wait_for_completion(job_id)

            logger.info("Final Job Status: %s", job_status)

            if job_status in ['DONE']:
                log_type: str = 'stdout'
            elif job_status in ['ERROR', 'TERMINATED', 'KILLED']:
                log_type: str = 'stdout'
            else:
                logger.error("Job completion status is unknown.")
                return

            job_log: str = self.hook.get_job_logs(job_id, log_type)
            logger.info("Logs for Job ID %s (Log Type: %s): %s", job_id, log_type, job_log)

            if job_status in ['ERROR', 'TERMINATED', 'KILLED']:
                raise AirflowException(job_log)
                       
        except Exception as e:
            raise AirflowException(e)
        


"""
YeeduNotebookRunOperator
"""



class YeeduNotebookRunOperator():

    content_status = None 
    error_value = None

    @apply_defaults
    def __init__(self, base_url, workspace_id, notebook_conf_id, tenant_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_url = base_url
        #self.headers = {'Accept': 'application/json'}
        self.workspace_id = workspace_id
        self.notebook_conf_id = notebook_conf_id
        self.tenant_id = tenant_id
        self.notebook_cells = {}
        self.notebook_executed = True
        self.notebook_id = None
        self.cell_output_data = []
        self.cells_info = {}
        self.hook: YeeduHook = YeeduHook(conf_id = self.notebook_conf_id, tenant_id=self.tenant_id, base_url=self.base_url, workspace_id=self.workspace_id)

        # default (30, 60) -- connect time: 30, read time: 60 seconds
        # https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
        self.timeout = (30, 60)

         
    def create_notebook_instance(self):
        try:
            post_url = self.base_url + f'workspace/{self.workspace_id}/notebook'
            data={'notebook_conf_id': self.notebook_conf_id}
            post_response = self.hook._api_request('POST',post_url,data)

            status_code = post_response.status_code

            logger.info(f'Create Notebook - POST Status Code: {status_code}')
            logger.debug(
                f'Create Notebook - POST Response: {post_response.json()}')

            if status_code == 200:
                self.notebook_id = post_response.json().get('notebook_id')
                self.get_active_notebook_instances()
                self.wait_for_kernel_status(self.notebook_id)
                self.get_websocket_token()
                return
            else:
                raise Exception(post_response.text)
        except Exception as e:
            logger.error(
                f"An error occurred during create_notebook_instance: {e}")
            raise e

    def get_active_notebook_instances(self):
        try:
            retry_interval = 60
            max_retries = 10
            for retry in range(1, max_retries + 1):

                get_params = {
                    'notebook_conf_id': self.notebook_conf_id,
                    'notebook_status': 'RUNNING',
                    'isActive': 'true'
                }

                get_url = self.base_url + f'workspace/{self.workspace_id}/notebooks'

                get_active_notebook_instances_response = self.hook._api_request('GET',
                    url=get_url,
                    params=get_params,
                )

                if get_active_notebook_instances_response is not None:

                    status_code_response = get_active_notebook_instances_response.status_code

                    logger.info(
                        f'Get Active Notebooks - GET Status Code: {status_code_response}')
                    logger.debug(
                        f'Get Active Notebooks - GET Response: {get_active_notebook_instances_response.json()}')

                    if status_code_response == 200:
                        return (get_active_notebook_instances_response.json()).get('data')[0].get('notebook_id')
                    else:
                        logger.warning(
                            f'Retry attempt {retry}/{max_retries}. Retrying in {retry_interval} seconds...')
                        time.sleep(retry_interval)
                else:
                    # Handle the case where the response is None
                    logger.warning(
                        f'Retry attempt {retry}/{max_retries}. Retrying in {retry_interval} seconds...')
                    time.sleep(retry_interval)

            logger.error(
                f"Failed after {max_retries} retry attempts. Stopping program.")
            raise Exception(
                f"Failed after {max_retries} retry attempts. Stopping program.")
        except Exception as e:
            logger.error(
                f"An error occurred during get_active_notebook_instances: {e}")
            raise e

    def wait_for_kernel_status(self, notebook_id):
        try:
            kernel_url = self.base_url + f'workspace/{self.workspace_id}/notebook/{notebook_id}/kernel/startOrGetStatus'

            logger.info(f"Kernel URL: {kernel_url}")

            max_retries = 3
            logger.info("Notebook is starting. Please wait....")
            time.sleep(10)

            for retry in range(1, max_retries + 1):
                kernel_response = self.hook._api_request('POST',kernel_url)

                kernel_info = kernel_response.json().get('kernel_info', {})
                kernel_status = kernel_info.get('kernel_status')

                logger.info(
                    f"Kernel status attempt {retry}/{max_retries}: {kernel_status}")

                if self.check_kernel_status(kernel_status):
                    logger.info("Kernel status matched the desired status.")
                    break

                if retry == max_retries:
                    logger.warning(
                        f"Kernel status did not match the desired status after {max_retries} retries.")
                    raise Exception(
                        f"Kernel status did not match the desired status after {max_retries} retries.")
                else:
                    logger.info(
                        f"Retrying in 10 seconds... (Retry {retry}/{max_retries})")
                    time.sleep(10)

        except Exception as e:
            logger.error(
                f"An error occurred while checking kernel status: {e}")
            raise e

    # Example usage:
    def check_kernel_status(self, status):
        return status in ['idle', 'starting', 'busy']

    def get_websocket_token(self):
        try:
            token = headers.get('Authorization').split(" ")[1]

            # Construct WebSocket token URL
            proxy_url = self.base_url + f'workspace/{self.workspace_id}/notebook/{self.notebook_id}/kernel/ws'

            #logger.info(f"WebSocket Token URL: {proxy_url}")


            # hit proxy api to create web socket connection
            proxy_response = self.hook._api_request('GET',
                url=proxy_url,
                params={
                    'yeedu_session': token
                },
            )

            if proxy_response.status_code == 200:

                logger.debug(
                    f"WebSocket Token Response: {proxy_response.json()}")

                # creating web socket url
                websocket_url = self.base_url + f"workspace/{self.workspace_id}/notebook/{self.notebook_id}/kernel/ws/yeedu_session/{token}"
                websocket_url = websocket_url.replace('http://', 'ws://').replace('https://', 'ws://')
                #logger.info(f"WebSocket URL: {websocket_url}")
                return websocket_url
            else:
                raise Exception(
                    f"Failed to get WebSocket token. Status code: {proxy_response.status_code} messsgae: {proxy_response.text}")

        except Exception as e:
            logger.error(
                f"An error occurred while getting WebSocket token: {e}")

    def get_code_from_notebook_configuration(self):
        try:
            # Construct notebook configuration URL
            get_notebook_url = self.base_url + f'workspace/{self.workspace_id}/notebook/conf'

            notebook_conf_response = self.hook._api_request('GET',
                get_notebook_url,
                params={'notebook_conf_id': self.notebook_conf_id},
            )

            if notebook_conf_response.status_code == 200:
                return notebook_conf_response
            else:
                logger.warning(
                    f"Failed to get notebook configuration. Status code: {notebook_conf_response.status_code} message: {notebook_conf_response.text}")
                raise Exception(
                    f"Failed to get notebook configuration. Status code: {notebook_conf_response.status_code} message: {notebook_conf_response.text}")

        except Exception as e:
            logger.error(
                f"An error occurred while getting notebook configuration: {e}")
            raise e

    def check_notebook_instance_status(self):
        try:
            check_notebook_status_url = self.base_url + f'workspace/{self.workspace_id}/notebook/{self.notebook_id}'

            logger.info(
                f"Checking notebook_instance status of : {self.notebook_id}")

            max_retries = 3
            status = None

            # Check if the notebook status is 'stopped'
            for retry in range(0, max_retries + 1):
                notebook_status_response = self.hook._api_request('GET',
                    url=check_notebook_status_url,
                )

                logger.info(
                    f'Notebook_instance GET status response: {notebook_status_response.status_code}')
                logger.debug(
                    f'Notebook_instance GET status response: {notebook_status_response.json()}')

                if notebook_status_response.status_code == 200:

                    status = notebook_status_response.json().get('notebook_status')

                    if status == 'STOPPED':
                        logger.info("Notebook is stopped.")
                        break

                elif retry == max_retries:
                    logger.warning(
                        f"Notebook_instance status did not match the desired status after {max_retries} retries.")
                    raise Exception(
                        f"Failed to get notebook status. Status code: {notebook_status_response.status_code}")
                else:
                    logger.info(
                        f"Retrying in 10 seconds... (Retry {retry}/{max_retries})")
                    time.sleep(10)

            return status
        except Exception as e:
            logger.error(
                f"An error occurred while checking notebook instance status: {e}")

    def stop_notebook(self):
        try:
            stop_notebook_url = self.base_url + f'workspace/{self.workspace_id}/notebook/kill/{self.notebook_id}'

            logger.debug(f"Stopping notebook instance id: {self.notebook_id}")

            # Use post_request function
            notebook_stop_response = self.hook._api_request('POST',stop_notebook_url)

            logger.info(
                f'Stop Notebook - POST Response Status: {notebook_stop_response.status_code}')
            logger.debug(
                f'Stop Notebook - POST Response: {notebook_stop_response.json()}')

            if notebook_stop_response.status_code == 201:
                self.check_notebook_instance_status()
                logger.info(
                    f"Notebook instance id: {self.notebook_id} stopped successfully.")
                return notebook_stop_response
            else:
                logger.error(
                    f"Failed to stop notebook. Status code: {notebook_stop_response.status_code}, Message: {notebook_stop_response.text}")
                raise Exception(
                    f"Failed to stop notebook. Status code: {notebook_stop_response.status_code}, Message: {notebook_stop_response.text}")
        except Exception as e:
            logger.error(f"An error occurred while stopping notebook: {e}")
            raise e

    def update_notebook_cells(self):
        try:
            cells_info = {"cells": self.cells_info}
            msg_id_to_update = self.cell_output_data[0]['msg_id']
            # Iterate through cells and update output if msg_id matches
            for cell in cells_info['cells']:
                if 'cell_uuid' in cell and cell['cell_uuid'] == msg_id_to_update:
                    print(f"MSG ID {msg_id_to_update}")
                    print(f"THE OUTPUT VALUE {self.cell_output_data[0]['output']}")
                    cell['output'] = [{"text": self.cell_output_data[0]['output']}] 
                    self.cell_output_data.clear()

            for cell in cells_info['cells']:
                if 'msg_id' in cell:
                    del cell['msg_id']

            print(f"CELLS INFO {cells_info}")
            update_cell_url = self.base_url + f'workspace/{self.workspace_id}/notebook/{self.notebook_conf_id}/update'
            data = cells_info
            update_cells_response = self.hook._api_request('POST',update_cell_url,data)

            print(f'this is cell response {update_cells_response}')

            logger.info(update_cells_response.status_code)

            logger.debug(
                f'Update Notebook cells - POST Response: {update_cells_response.json()}')

            if update_cells_response.status_code == 201:
                logger.info("Notebook cells updated successfully.")
                self.cell_output_data.clear()
                return update_cells_response
            else:
                logger.error(
                    f"Failed to update notebook cells. Status code: {update_cells_response.status_code}, Message: {update_cells_response.text}")
                raise Exception(
                    f"Failed to update notebook cells. Status code: {update_cells_response.status_code}, Message: {update_cells_response.text}")
        except Exception as e:
            logger.error(f"An error occurred while updating notebook cells: {e}")
            raise e

    def exit_notebook(self, exit_reason):
        try:
            if self.notebook_executed:
                return 0

            logger.info(f'Clearing the notebook cells')

            self.notebook_cells.clear()

            self.stop_notebook()
            #raise Exception(exit_reason)
        except Exception as e:
            logger.error(f'Error while exiting notebook: {e}')
            raise e

    def on_message(self, ws, message):
        try:
            response = json.loads(message)

            msg_type = response.get('msg_type', '')

            if msg_type == 'execute_result':
                content = response.get('content', {})
                output_data = content.get('data', {}).get('text/plain', '')
                logger.info(f"Execution Result:\n{output_data}")
            elif msg_type == 'error':
                content = response.get('content', {})
                error_name = content.get('ename', '')
                self.error_value = content.get('evalue', '')
                traceback = content.get('traceback', [])
                logger.error(f"Error: {error_name} - {self.error_value}")
                logger.error("Traceback:")
                #logger.info(self.error_value)
                for tb in traceback:
                    logger.error(tb)
            elif msg_type == 'execute_input':
                print(response)
                content = response.get('content', {})
                code_input = content.get('code', '')
                logger.info(f"Execute Input:\n{code_input}")
            elif msg_type == 'stream':
                content = response.get('content', {})
                text_value = content.get('text', '')
                msg_id = response['parent_header']['msg_id']
                print(f'this msg_id is in stream {msg_id}')
                self.cell_output_data.append({'msg_id': msg_id, 'type': 'text', 'output': [text_value]})
                print(f'stream output is {self.cell_output_data}')
                self.update_notebook_cells()
            elif msg_type == 'display_data':
                content = response.get('content', {})
                logger.info(f"Display Data: {content}")
                msg_id = response['parent_header']['msg_id']
                img_resp = response.get('content', {}).get('data', {}).get('image/png')
                text_resp = response.get('content', {}).get('data', {}).get('text/plain')
                if img_resp:
                     image_url = f'data:image/png;base64,{img_resp}'
                     self.cell_output_data.append({'msg_id': msg_id,'type': 'image', 'output': image_url})
                if text_resp:
                    self.cell_output_data.append({'msg_id': msg_id,'type': 'text', 'output': text_resp})
                self.update_notebook_cells()
            elif msg_type == 'status':
                execution_state = response.get(
                    'content', {}).get('execution_state', '')
                logger.info(f"Execution State: {execution_state}")
            elif msg_type == 'execute_reply':
                content = response.get('content', {})
                logger.info(f"Content {content}")
                self.content_status = content.get('status', '')
                logger.info(self.content_status)
                msg_id = response['parent_header']['msg_id']
                logger.info(f"Message Id: {msg_id}")
                if self.content_status == 'ok':
                    try:
                        logger.info('Removing notebook cells ...')

                        # Update the notebook_cells array after content.status is ok by removing the msg_id under parent_header
                        self.notebook_cells = [
                            cell for cell in self.notebook_cells if cell.get('msg_id') != msg_id]
                        logger.info(
                            f"Notebook cells array after removing {msg_id}: {self.notebook_cells}")
                    except ValueError:
                        pass
                elif self.content_status == 'error':
                    self.error_value = content.get('evalue', '')
                    traceback = content.get('traceback', [])
                    logger.info(traceback)

                    self.notebook_executed = False

                    self.exit_notebook(
                        f'cell with message_id: {msg_id} failed with error: {self.error_value}')
                else:
                    raise Exception(
                        f"Invalid self.content_status: {self.content_status}")

        except Exception as e:
            logger.error(f"Unsupported message type: {e}")

            if self.check_notebook_instance_status() != 'STOPPED':
                self.exit_notebook(f'Unsupported message_type: {e}')

            raise e

    def on_error(self, ws, error):
        try:
            logger.error(f"WebSocket encountered an error: {error}")

            if self.check_notebook_instance_status() != 'STOPPED':
                self.exit_notebook(f'Websocket encountered error: {error}')

        except Exception as e:
            logger.error(e)
            raise e

    def on_close(self, ws, e=None):
        try:
            logger.info(f"WebSocket closed")

            if self.check_notebook_instance_status()!= 'STOPPED':
                self.exit_notebook(f"Websocket closed : {e}")

        except Exception as e:
            logger.error(e)
            raise e

    def on_open(self, ws):
        logger.info("WebSocket opened")

    def send_execute_request(self, ws, code, session_id, msg_id):
        try:
            from datetime import datetime
            current_date = datetime.now()

            execute_request = {
                'header': {
                    'msg_type': 'execute_request',
                    'msg_id': msg_id,
                    'username': 'username',
                    'session': session_id,
                    'date': current_date.strftime("%Y-%m-%d %H:%M:%S"),
                    'version': '5.3'
                },
                'metadata': {},
                'content': {
                    'code': code,
                    'silent': False,
                    'store_history': True,
                    'user_expressions': {},
                    'allow_stdin': False
                },
                'buffers': [],
                'parent_header': {},
                'channel': 'shell'
            }

            ws.send(json.dumps(execute_request))
        except Exception as e:
            logger.error(f"Error while sending execute request: {e}")
            raise e

    def execute(self, context: dict):
        # Send execute requests for notebook cells
        try:
            #self.yeedu_login()
            self.create_notebook_instance()
            ws = websocket.WebSocketApp(
                self.get_websocket_token(),
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            _thread.start_new_thread(ws.run_forever, ())

            time.sleep(5)

            notebook_get_response = self.get_code_from_notebook_configuration()

            self.notebook_cells = notebook_get_response.json().get(
                'notebook_cells', {}).get('cells', [])

            self.cells_info = copy.deepcopy(self.notebook_cells)

            logger.info(f"Notebook Cells: {self.notebook_cells}")

            session_id = str(uuid.uuid4())

            for cell in self.notebook_cells:
                code = cell.get('code')
                msg_id = cell.get('cell_uuid')

                self.send_execute_request(ws, code, session_id, msg_id)

                cell['msg_id'] = msg_id

            # awaiting all the notebook cells executing
            while len(self.notebook_cells) > 0:
                logger.info('Waiting {} cells to finish'.format(
                    len(self.notebook_cells)))
                time.sleep(1)

            if self.notebook_executed:
                time.sleep(5)
                self.stop_notebook()
                return 0
            else:
                logger.info("Exiting notebook from main function")

                if self.check_notebook_instance_status() != 'STOPPED':
                    self.exit_notebook(
                        f'Exiting notebook from main function: {e}')

            ws.close()

            if self.content_status == 'error': 
                raise AirflowException(f"{self.error_value}")

        except Exception as e:
            logger.error(
                f"Notebook execution failed with error:  {e}")
            raise e