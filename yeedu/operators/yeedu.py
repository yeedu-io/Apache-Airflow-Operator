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

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from yeedu.operators.job_operator import YeeduJobRunOperator
from yeedu.operators.notebook_operator import YeeduNotebookRunOperator
from yeedu.operators.healthcheck_operator import YeeduHealthCheckOperator
from yeedu.hooks.yeedu import YeeduHook
import logging
from urllib.parse import urlparse

# Configure the logging system
logging.basicConfig(level=logging.INFO)  # Set the logging level to INFO

# Create a logger object
logger = logging.getLogger(__name__)

class YeeduOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        job_url,
        connection_id,
        token_variable_name=None,
        *args,
        **kwargs
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

        super().__init__(*args, **kwargs)
        self.job_url = job_url
        self.connection_id = connection_id
        self.token_variable_name = token_variable_name
        (
            self.base_url,
            self.tenant_id,
            self.workspace_id,
            self.job_type,
            self.conf_id,
            self.restapi_port,
        ) = self.extract_ids(self.job_url)
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
            job_operator = YeeduJobRunOperator(
                job_conf_id=self.conf_id,
                base_url=self.base_url,
                workspace_id=self.workspace_id,
                tenant_id=self.tenant_id,
                connection_id=self.connection_id,
                token_variable_name=self.token_variable_name,
                restapi_port=self.restapi_port,
            )
            return job_operator.execute(context)
        elif self.job_type == "notebook":
            self.hook.yeedu_login(context)
            notebook_operator = YeeduNotebookRunOperator(
                base_url=self.base_url,
                workspace_id=self.workspace_id,
                notebook_conf_id=self.conf_id,
                tenant_id=self.tenant_id,
                connection_id=self.connection_id,
                token_variable_name=self.token_variable_name,
                restapi_port=self.restapi_port,
            )
            return notebook_operator.execute(context)
        elif self.job_type == "healthcheck":
            health_check_operator = YeeduHealthCheckOperator(
                base_url=self.base_url,
                connection_id=self.connection_id,
            )
            return health_check_operator.execute(context)
        else:
            raise AirflowException(f"Unknown job_type: {self.job_type}")
