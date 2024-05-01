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

"""
Yeedu hook.
This hook enable the submitting and running of jobs to the Yeedu platform. Internally the
operators talk to the ``/spark/job`
"""

import requests
import time
import logging
from typing import Tuple
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from typing import Optional, Dict
import json
import os
from requests.exceptions import RequestException



session = requests.Session()

YEEDU_SCHEDULER_USER = os.getenv("YEEDU_SCHEDULER_USER")
YEEDU_SCHEDULER_PASSWORD = os.getenv("YEEDU_SCHEDULER_PASSWORD")

headers: dict = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }

class YeeduHook(BaseHook):
    """
    YeeduHook provides an interface to interact with the Yeedu API.

    :param token: Yeedu API token.
    :type token: str
    :param hostname: Yeedu API hostname.
    :type hostname: str
    :param workspace_id: The ID of the Yeedu workspace.
    :type workspace_id: int
    :param args: Additional positional arguments.
    :param kwargs: Additional keyword arguments.
    """

    def __init__(self, conf_id: int, tenant_id: str, base_url: str, workspace_id: int, *args, **kwargs) -> None:
        """
        Initializes YeeduHook with the necessary configurations to communicate with the Yeedu API.

        :param tenant_id: Yeedu API tenant_id.
        :param base_url: Yeedu API base_url.
        :param workspace_id: The ID of the Yeedu workspace.
        """

        super().__init__(*args, **kwargs)
        self.tenant_id: str = tenant_id
        self.conf_id = conf_id
        self.workspace_id = workspace_id
        self.base_url: str = base_url

    def check_ssl(self):
        try:
            # if not provided set to true by default

            YEEDU_SSL_VERIFICATION = os.getenv(
                'YEEDU_SSL_VERIFICATION', 'true').lower()
            if YEEDU_SSL_VERIFICATION == 'true':

                # check for the ssl cert dir
                if not os.getenv('YEEDU_SSL_CERT_FILE'):
                    self.log.error(
                        f"Please set the environment variable: YEEDU_SSL_CERT_FILE if YEEDU_SSL_VERIFICATION is set to: {YEEDU_SSL_VERIFICATION} (default: true)")
                    raise AirflowException(f"Please set the environment variable: YEEDU_SSL_CERT_FILE if YEEDU_SSL_VERIFICATION is set to: {YEEDU_SSL_VERIFICATION} (default: true)")
                else:
                    # check if the file exists or not
                    if os.path.isfile(os.getenv('YEEDU_SSL_CERT_FILE')):
                        return os.getenv('YEEDU_SSL_CERT_FILE')
                    else:
                        self.log.error(
                            f"Provided YEEDU_SSL_CERT_FILE: {os.getenv('YEEDU_SSL_CERT_FILE')} doesnot exists")
                        raise AirflowException(f"Provided YEEDU_SSL_CERT_FILE: {os.getenv('YEEDU_SSL_CERT_FILE')} doesnot exists")
            elif YEEDU_SSL_VERIFICATION == 'false':
                return False

            else:
                self.log.error(
                    f"Provided YEEDU_SSL_VERIFICATION: {os.getenv('YEEDU_SSL_VERIFICATION')} is neither true/false")
                raise AirflowException(f"Provided YEEDU_SSL_VERIFICATION: {os.getenv('YEEDU_SSL_VERIFICATION')} is neither true/false")

        except Exception as e:
            self.log.error(f"Check SSL failed due to: {e}")
            raise AirflowException(e)
        

    def _api_request(self, method: str, url: str, data=None, params: Optional[Dict] = None) -> requests.Response:
        """
        Makes an HTTP request to the Yeedu API with retries.

        :param method: The HTTP method (GET, POST, etc.).
        :param url: The URL of the API endpoint.
        :param data: The JSON data for the request.
        :param params: Optional dictionary of query parameters.
        :return: The API response.
        """

        if method =='POST':
            response = session.post(url, headers=headers, json=data, params=params, verify=self.check_ssl())
        else:
            response = session.get(url, headers=headers, json=data, params=params, verify=self.check_ssl())
        return response 
            

    def yeedu_login(self):
        try:
            login_url = self.base_url+'login'
            data = {
                    "username": f"{YEEDU_SCHEDULER_USER}",
                    "password": f"{YEEDU_SCHEDULER_PASSWORD}"
                }
            # self.log.info(f"{data},{YEEDU_SCHEDULER_USER},{YEEDU_SCHEDULER_PASSWORD}")
            login_response = self._api_request('POST',login_url,data)

            if login_response.status_code == 200:
                self.log.info(
                    f'Login successful. Token: {login_response.json().get("token")}')
                headers['Authorization'] = f"Bearer {login_response.json().get('token')}"
                self.associate_tenant()
                return login_response.json().get('token')
            else:
                raise AirflowException(login_response.text)
        except Exception as e:
            self.log.info(f"An error occurred during yeedu_login: {e}")
            raise AirflowException(e)

    def associate_tenant(self):
        try:
            # Construct the tenant URL
            tenant_url = self.base_url+f'user/select/{self.tenant_id}'

            # Make the POST request to associate the tenant
            tenant_associate_response = self._api_request('POST',tenant_url)

            if tenant_associate_response.status_code == 201:
                self.log.info(
                    f'Tenant associated successfully. Status Code: {tenant_associate_response.status_code}')
                self.log.info(
                    f'Tenant Association Response: {tenant_associate_response.json()}')
                return 0
            else:
                raise AirflowException(tenant_associate_response.text)
        except Exception as e:
            self.log.info(f"An error occurred during associate_tenant: {e}")
            raise AirflowException(e)  
        
    def get_job_type(self):

        # URL for notebook configuration API
        notebook_url = self.base_url + f'workspace/{self.workspace_id}/notebook/conf?notebook_conf_id={self.conf_id}'
        response_notebook = self._api_request('GET',notebook_url)
        # self.log.info("notebookresponse",response_notebook.json())
        # URL for job configuration API
        job_url = self.base_url + f'workspace/{self.workspace_id}/spark/job/conf?job_conf_id={self.conf_id}'
        response_job = self._api_request('GET',job_url)
        # self.log.info(response_job.json())
        # Check status codes and return job type
        if response_notebook.status_code == 200:
            return 'notebook'
        elif response_job.status_code == 200:
            return 'job'
        else:
            return None
        
    def submit_job(self, job_conf_id: str) -> int:
        """
        Submits a job to Yeedu.

        :param job_conf_id: The job configuration ID.
        :return: The ID of the submitted job.
        """

        try:
            job_url: str = self.base_url + f'workspace/{self.workspace_id}/spark/job'
            data: dict = {'job_conf_id': job_conf_id}
            response = self._api_request('POST', job_url, data)
            api_status_code = response.status_code
            response_json = response.json()
            if api_status_code == 200:
                job_id = response.json().get('job_id')
                if job_id:
                    return job_id
                else:
                    raise AirflowException(response_json)
            else:
                raise AirflowException(response_json)
                
        except Exception as e:
            raise AirflowException(e)
            


    def get_job_status(self, job_id: int) -> requests.Response:
        """
        Retrieves the status of a Yeedu job.

        :param job_id: The ID of the job.
        :return: The API response containing job status.
        """

        job_status_url: str = self.base_url + f'workspace/{self.workspace_id}/spark/job/{job_id}'
        return self._api_request('GET', job_status_url)

            

    def get_job_logs(self, job_id: int, log_type: str) -> str:
        """
        Retrieves logs for a Yeedu job.

        :param job_id: The ID of the job.
        :param log_type: The type of logs to retrieve ('stdout' or 'stderr').
        :return: The logs for the specified job and log type.
        """

        try:
            logs_url: str = self.base_url + f'workspace/{self.workspace_id}/spark/job/{job_id}/log/{log_type}'
            time.sleep(30)
            return self._api_request('GET', logs_url).text
        
        except Exception as e:
            raise AirflowException(e)
            

    def wait_for_completion(self, job_id: int) -> str:
        """
        Waits for the completion of a Yeedu job and retrieves its final status.

        :param job_id: The ID of the job.
        :return: The final status of the job.
        :raises AirflowException: If continuous API failures reach the threshold.
        """
        
        try:
            max_attempts: int = 5
            attempts_failure: int = 0

            while True:
                time.sleep(5)
                # Check job status
                try:
                    response: requests.Response = self.get_job_status(job_id)
                    api_status_code: int = response.status_code
                    self.log.info("Current API Status Code: %s",api_status_code)
                    if api_status_code == 200:
                        # If API status is a success, reset the failure attempts counter
                        attempts_failure = 0
                        job_status: str = response.json().get('job_status')
                        self.log.info("Current Job Status: %s ", job_status)
                        if job_status in ['DONE', 'ERROR', 'TERMINATED', 'KILLED','STOPPED']:
                            break
                except RequestException as e:
                    attempts_failure += 1
                    delay = 20
                    self.log.info(f"GET Job Status API request failed (attempt {attempts_failure}/{max_attempts}) due to {e}")
                    self.log.info(f"Sleeping for {delay*attempts_failure} seconds before retrying...")
                    time.sleep(delay*attempts_failure)

                # If continuous failures reach the threshold, throw an error
                if attempts_failure == max_attempts:
                    raise AirflowException("Continuous API failure reached the threshold")

            return job_status
        
        except Exception as e:
            raise AirflowException(e)
        