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
from airflow.models import Variable

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

    def __init__(self, conf_id: int, tenant_id: str, base_url: str, workspace_id: int, connection_id: str, 
                 token_variable_name: str,*args, **kwargs) -> None:
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
        self.connection_id = connection_id
        self.connection = self.get_connection(self.connection_id)
        self.base_url: str = base_url
        self.token_variable_name = token_variable_name
        self.YEEDU_SSL_CERT_FILE = self.connection.extra_dejson.get('YEEDU_SSL_CERT_FILE')
        self.YEEDU_AIRFLOW_VERIFY_SSL = self.connection.extra_dejson.get('YEEDU_AIRFLOW_VERIFY_SSL', 'true')
        self.session = requests.Session()
        self.session.verify = self.check_ssl()


    def get_session(self):
        return self.session

    def check_ssl(self):
        try:
            # if not provided set to true by default
            if self.YEEDU_AIRFLOW_VERIFY_SSL == 'true':

                # check for the ssl cert dir
                if not self.YEEDU_SSL_CERT_FILE:
                    self.log.error(
                        f"Please provide YEEDU_SSL_CERT_FILE if YEEDU_AIRFLOW_VERIFY_SSL is set to: {self.YEEDU_AIRFLOW_VERIFY_SSL} (default: true)")
                    raise AirflowException(f"Please provide YEEDU_SSL_CERT_FILE if YEEDU_AIRFLOW_VERIFY_SSL is set to: {self.YEEDU_AIRFLOW_VERIFY_SSL} (default: true)")
                else:
                    # check if the file exists or not
                    if os.path.isfile(self.YEEDU_SSL_CERT_FILE):
                        return self.YEEDU_SSL_CERT_FILE
                    else:
                        self.log.error(
                            f"Provided self.YEEDU_SSL_CERT_FILE: {self.YEEDU_SSL_CERT_FILE} doesnot exists")
                        raise AirflowException(f"Provided self.YEEDU_SSL_CERT_FILE: {self.YEEDU_SSL_CERT_FILE} doesnot exists")
            elif self.YEEDU_AIRFLOW_VERIFY_SSL == 'false':
                self.log.info("YEEDU_AIRFLOW_VERIFY_SSL False")
                return False

            else:
                self.log.error(
                    f"Provided YEEDU_AIRFLOW_VERIFY_SSL: {self.YEEDU_AIRFLOW_VERIFY_SSL} is neither true/false")
                raise AirflowException(f"Provided YEEDU_AIRFLOW_VERIFY_SSL: {self.YEEDU_AIRFLOW_VERIFY_SSL} is neither true/false")

        except Exception as e:
            self.log.error(f"Check SSL failed due to: {e}")
            raise AirflowException(e)

    
    def get_auth_details(self):
 
        auth_type = self.get_auth_type()

        if auth_type in ['AAD','LDAP']:
            username = self.connection.login
            password = self.connection.password
            if not username or not password:
                raise AirflowException(f"Username or password is not set in the connection '{self.connection_id}'")
            return username, password, None
        elif auth_type=='AZURE_SSO': 
            if self.check_token():
                token = self.get_token()
                return None, None, token
            else:
                raise AirflowException("The authentication type is set to Azure_SSO. Please provide a token to schedule jobs or notebooks.") 
        else:
             raise AirflowException(f"The current AirflowOperator only supports LDAP, AAD, and Azure_SSO authentication types, but received {auth_type}.")      

    
    def _api_request(self, method: str, url: str, data=None, params: Optional[Dict] = None,max_attempts: int = 5,delay: int =20) -> requests.Response:
        """
        Makes an HTTP request to the Yeedu API with retries.

        :param method: The HTTP method (GET, POST, etc.).
        :param url: The URL of the API endpoint.
        :param data: The JSON data for the request.
        :param params: Optional dictionary of query parameters.
        :return: The API response.
        :raises AirflowException: If continuous request failures reach the threshold.
        """
        attempts_failure: int = 0

        while attempts_failure < max_attempts:
            try:
                # Make the HTTP request
                if method == 'POST':
                    response = self.session.post(url, headers=headers, json=data, params=params)
                else:
                    response = self.session.get(url, headers=headers, json=data, params=params)

                if response.status_code in [200,201,409]:
                    return response 

                else:
                    attempts_failure += 1
                    self.log.info(f"API request failed with status {response.status_code} (attempt {attempts_failure}/{max_attempts})")
                    self.log.info(f"Sleeping for {delay} seconds before retrying...")
                    time.sleep(delay)

            except Exception as e:
                attempts_failure += 1
                self.log.error(f"Request failed due to exception: {e} (attempt {attempts_failure}/{max_attempts})")
                self.log.info(f"Sleeping for {delay} seconds before retrying...")
                time.sleep(delay)
        
        
        raise AirflowException(f"Continuous API failure reached the threshold after multiple attempts - {response.text}")

    def check_token(self):
        """
        Checks whether the token variable name is set.
        :return: True if the token variable name is set, False otherwise.
        """

        if self.token_variable_name is not None:
            return True
        else:
            return False


    def get_token(self):
        """self.tenant_id
        Retrieves the token from Airflow Variables.

        :return: The token value if available, or None if not found.
        :raises ValueError: If there is an issue retrieving the token.
        """
        try:
            token = Variable.get(self.token_variable_name,default_var=None)
            return token
        except Exception as e:
            self.log.info(f"Please provide valid block name: {e}")
            raise ValueError(e)

            
    def get_auth_type(self):
        """
        Retrieves the authentication type from the Yeedu API.
        :return: The authentication type (e.g., LDAP, AAD, AZURE_SSO).
        :raises AirflowException: If there is an error in getting the auth type.
        """

        try:
            auth_url = self.base_url+'login/auth_type'
            auth_response = self._api_request('GET',auth_url)
            self.log.info(f"auth_type: {auth_response.json().get('auth_type')}")
            return auth_response.json().get('auth_type')

        except Exception as e:
            self.log.info(f"An error occurred in getting auth_type: {e}")
            raise AirflowException(e)


    def yeedu_login(self,context):
        """
        Logs in to the Yeedu API and retrieves an authentication token.

        :return: The authentication token.
        :raises AirflowException: If there is an issue during the login process or if an unsupported auth type is returned.
        """
        try:
            auth_type = self.get_auth_type()
            username, password , token = self.get_auth_details()
            if auth_type in ['LDAP', 'AAD']:   
                login_url = self.base_url+'login'

                data = {
                        "username": f"{username}",
                        "password": f"{password}",
                        "auth_type": f"{auth_type}",
                        "timeout": "infinity"
                    }
                login_response = self._api_request('POST',login_url,data)
                if login_response.status_code == 200:

                    headers['Authorization'] = f"Bearer {login_response.json().get('token')}"
                    self.associate_tenant()
                    return login_response.json().get('token')
                
            elif auth_type == 'AZURE_SSO':
                if token is not None:  
                    headers['Authorization'] = f"Bearer {token}"
                    self.associate_tenant()
                    return token              
            else:
                raise AirflowException(f"The current AirflowOperator only supports LDAP, AAD, and Azure_SSO authentication types, but received {auth_type}.")         
        except Exception as e:
            self.log.info(f"An error occurred during yeedu_login: {e}")
            raise AirflowException(e)

    def yeedu_logout(self,context):
        try:
            # Construct the logout URL
            logout_url = self.base_url+'logout'

            # Make the POST request to associate the tenant
            logout_response = self._api_request('POST',logout_url)

            if logout_response.status_code == 200:
                self.log.info(
                    f'Status Code: {logout_response.status_code}')
                self.log.info(
                    f'{logout_response.text}')
                return 0
            else:
                raise AirflowException(logout_response.text)
        except Exception as e:
            self.log.info(f"An error occurred during yeedu_logout: {e}")
            raise AirflowException(e)
        
    def associate_tenant(self):
        """
        Associates a tenant to the user in the Yeedu API.

        :return: 0 if the tenant is successfully associated.
        :raises AirflowException: If there is an issue associating the tenant or if the response is not successful.
        """
        try:
            # Construct the tenant URL
            tenant_url = self.base_url + f'user/select/{self.tenant_id}'

            # Make the POST request to associate the tenant
            tenant_associate_response = self._api_request('POST', tenant_url)
            self.log.info(f"Tenant Association Response: {tenant_associate_response.text}")

            if tenant_associate_response.status_code == 201:
                self.log.info(
                    f'Tenant associated successfully. Status Code: {tenant_associate_response.status_code}')
                self.log.info(
                    f'Tenant Association Response: {tenant_associate_response.json()}')
                return 0
            else:
                raise AirflowException(tenant_associate_response.text)
        except Exception as e:
            # Only ignore if the error text matches the "already associated" message
            error_text = str(e)
            if (
                f"Association to the tenant Id: {self.tenant_id}" in error_text and
                "is not allowed for the current session." in error_text
            ):
                self.log.warning(
                    f"Tenant association skipped: {error_text} as it is already associated"
                )
                return 0
            self.log.info(f"An error occurred during associate_tenant: {e}")
            raise AirflowException(e)  
        
    def yeedu_health_check(self) -> int:
        """
        Hitting Health Check API
        """
        health_check_url: str = self.base_url + f'healthCheck'
        return self._api_request('GET', health_check_url)
        

        
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
        try:
            job_status_url: str = self.base_url + f'workspace/{self.workspace_id}/spark/job/{job_id}'
            return self._api_request('GET', job_status_url)
        except Exception as e:
            self.log.info(f"An error occurred during fetching job_status: {e}")
            raise AirflowException(e) 
                        
            

    def get_job_logs(self, job_id: int, log_type: str) -> str:
        """
        Retrieves logs for a Yeedu job.

        :param job_id: The ID of the job.
        :param log_type: The type of logs to retrieve ('stdout' or 'stderr').
        :return: The logs for the specified job and log type.
        """

        try:
            logs_url: str = self.base_url + f'workspace/{self.workspace_id}/spark/job/{job_id}/log/{log_type}'
            time.sleep(40)
            return self._api_request('GET', logs_url).text
        
        except Exception as e:
            raise AirflowException(e)
            
        
    def kill_job(self, job_id: int):
        try:
            job_kill_url = self.base_url + f'workspace/{self.workspace_id}/spark/job/kill/{job_id}'
            self.log.info(f"Stopping job of Job Id {job_id}")
            response = self._api_request('POST',job_kill_url)
            if response.status_code == 201:
                self.log.info("Stopped the Job")
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
            while True:
                time.sleep(5)

                try:
                #check job_status
                    response: requests.Response = self.get_job_status(job_id)
                    api_status_code: int = response.status_code
                    self.log.info("Current API Status Code: %s",api_status_code)

                    if api_status_code == 200:

                        job_status: str = response.json().get('job_status')
                        self.log.info("Current Job Status: %s ", job_status)
                        if job_status in ['DONE', 'ERROR', 'TERMINATED', 'KILLED','STOPPED']:
                            return job_status
                    else:
                        raise AirflowException(f"Failed to get job status, API returned status code: {api_status_code}") 
              
                except Exception as e:
                    raise AirflowException(f"API failure while waiting for job completion: {e}")       
      
        except Exception as e:
            raise AirflowException(f"An error occurred while waiting for job completion: {e}")