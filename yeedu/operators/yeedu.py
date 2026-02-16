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
from yeedu.operators.job_operator import YeeduJobRunOperator
from yeedu.operators.notebook_operator import YeeduNotebookRunOperator
from yeedu.operators.healthcheck_operator import YeeduHealthCheckOperator
from typing import List
from urllib.parse import urlparse


class YeeduOperator(BaseOperator):
    template_fields = ("loop_input",)

    def __init__(
        self,
        job_url: str,
        connection_id: str,
        token_variable_name: str = None,
        arguments: str = None,
        loop_input: str = None,
        conf: List[str] = None,
        cluster_ids: List[int] = None,
        *args,
        **kwargs
    ):
        """
        Initializes the operator with Yeedu connection details.

        Parameters:
        job_url (str): The URL of the Yeedu Notebook or job.
        connection_id (str): The Airflow connection ID. This connection should contain:
            - username (str): The username for the connection.
            - password (str): The password for the connection.
            - hostname (str): The hostname for the connection.
            - extra (dict): Additional parameters in JSON format, including:
                - YEEDU_AIRFLOW_VERIFY_SSL (str): true or false to verify SSL.
                - YEEDU_SSL_CERT_FILE (str): Path to the SSL certificate file.
        arguments (str, optional): Arguments to pass to the job or notebook run.
        conf (List[str], optional): Configuration list for the job or notebook run.
            Must be provided as a list using square brackets [].
            Each configuration item must be in 'key=value' format.
            If duplicate keys are provided, only the last occurrence will be used.

            Usage Example:
                YeeduOperator(
                    job_url="your_url",
                    connection_id="your_conn_id",
                    conf=[
                        "spark.driver.memory=4g",
                        "spark.executor.memory=8g"
                    ]
                )

            Invalid formats:
                conf=("key=value",)         # Wrong: Using tuple () instead of list []
                conf=["key value"]          # Wrong: Missing '=' delimiter
                conf=["key="]               # Wrong: Empty value

            Duplicate handling:
                conf=[                      # Only "spark.driver.memory=8g" will be used
                    "spark.driver.memory=4g",
                    "spark.driver.memory=8g"
                ]
        token_variable_name (str, optional): Airflow Variable name that keeps the Yeedu Session token, if AZURE_SSO is configured.
        loop_input (str, optional): Value to push to XCom for downstream tasks.
        cluster_ids (List[int], optional): Cluster IDs to try after the run fails on existing cluster configured. The job always runs once on its current cluster before bumping. Pass cluster IDs in bump order, duplicates are removed automatically.
        *args: Additional positional arguments for BaseOperator.
        **kwargs: Additional keyword arguments for BaseOperator.
        """
        super().__init__(*args, **kwargs)
        self.job_url = job_url
        self.connection_id = connection_id
        self.token_variable_name = token_variable_name
        self.arguments = arguments
        self.loop_input = loop_input
        self.cluster_ids = self._prepare_cluster_ids(cluster_ids)
        (
            self.base_url,
            self.tenant_id,
            self.workspace_id,
            self.job_type,
            self.conf_id,
            self.restapi_port,
        ) = self.extract_ids(self.job_url)
        # Validate and process conf if provided
        if conf is not None and (self.job_type == "conf" or self.job_type == "notebook"):
            if not isinstance(conf, List):
                raise AirflowException("conf parameter must be a list")
            self.conf = self._validate_conf(conf)  # Store processed conf
        else:
            self.conf = None

    def _prepare_cluster_ids(self, cluster_ids: List[int]) -> List[int]:
        """Return cluster IDs with duplicates removed while preserving the original order."""
        if cluster_ids is None:
            return []

        if not isinstance(cluster_ids, list):
            raise AirflowException(
                "cluster_ids parameter must be provided as a list.")

        seen = set()
        ordered_unique_ids = []

        for index, cluster_id in enumerate(cluster_ids):
            try:
                cluster_value = int(cluster_id)
            except (TypeError, ValueError):
                raise AirflowException(
                    f"cluster_ids item at position {index} must be an integer. Received: {cluster_id!r}"
                )

            if cluster_value not in seen:
                ordered_unique_ids.append(cluster_value)
                seen.add(cluster_value)

        return ordered_unique_ids

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
        restapi_port = parsed_url.port
        path_segments = parsed_url.path.strip("/").split("/")

        tenant_id = path_segments[1] if len(path_segments) > 1 else None
        workspace_id = path_segments[3] if len(path_segments) > 3 else None

        if "job" in path_segments:
            conf_id = (
                path_segments[path_segments.index("job") + 1]
                if len(path_segments) > path_segments.index("job") + 1
                else None
            )
            job_type = "job"
        elif "notebook" in path_segments:
            conf_id = (
                path_segments[path_segments.index("notebook") + 1]
                if len(path_segments) > path_segments.index("notebook") + 1
                else None
            )
            job_type = "notebook"
        elif "healthCheck" in path_segments:
            job_type = "healthcheck"
            conf_id = -1
            workspace_id = -1
        else:
            raise AirflowException(
                "Please provide valid URL to schedule/run Jobs and Notebooks"
            )

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
        ti = context['ti']
        run_id = ti.run_id
        map_index = ti.map_index
        task_id = ti.task_id

        composite_key = f"{run_id}__{task_id}__{map_index}"

        # Convert to str if needed
        value = self.loop_input
        if not isinstance(value, (str, int, float, dict, list)):
            value = str(value)

        ti.xcom_push(key=composite_key, value=value)

        if self.job_type == "job":
            job_operator = YeeduJobRunOperator(
                job_id=self.conf_id,
                base_url=self.base_url,
                workspace_id=self.workspace_id,
                tenant_id=self.tenant_id,
                connection_id=self.connection_id,
                token_variable_name=self.token_variable_name,
                restapi_port=self.restapi_port,
                arguments=self.arguments,
                conf=self.conf,
                cluster_ids=self.cluster_ids,
                logger=self.log.getChild("job_operator")
            )
            try:
                result = job_operator.execute(context)
                # Push tracking info to XCom for email notifications
                self._push_execution_info(ti, job_operator)
                return result
            except Exception as e:
                # Push tracking info even on failure
                self._push_execution_info(ti, job_operator)
                raise
        elif self.job_type == "notebook":
            notebook_operator = YeeduNotebookRunOperator(
                base_url=self.base_url,
                workspace_id=self.workspace_id,
                notebook_id=self.conf_id,
                tenant_id=self.tenant_id,
                connection_id=self.connection_id,
                token_variable_name=self.token_variable_name,
                restapi_port=self.restapi_port,
                arguments=self.arguments,
                conf=self.conf,
                cluster_ids=self.cluster_ids,
                logger=self.log.getChild("notebook_operator")
            )
            try:
                result = notebook_operator.execute(context)
                # Push tracking info to XCom for email notifications
                self._push_execution_info(ti, notebook_operator)
                return result
            except Exception as e:
                # Push tracking info even on failure
                self._push_execution_info(ti, notebook_operator)
                raise
        elif self.job_type == "healthcheck":
            health_check_operator = YeeduHealthCheckOperator(
                base_url=self.base_url,
                connection_id=self.connection_id,
                logger=self.log.getChild("health_check_operator")
            )
            return health_check_operator.execute(context)
        else:
            raise AirflowException(f"Unknown job_type: {self.job_type}")

    def _push_execution_info(self, ti, operator) -> None:
        """
        Push execution tracking info to XCom for email notifications.

        Args:
            ti: TaskInstance from context
            operator: The sub-operator (job or notebook) that was executed
        """
        try:
            # Push cluster bump attempts if any
            attempted_clusters = getattr(operator, 'attempted_clusters', [])
            if attempted_clusters:
                ti.xcom_push(key='yeedu_cluster_attempts',
                             value=attempted_clusters)

            # Push error summary if available
            error_summary = getattr(operator, 'last_error_summary', None)
            if error_summary:
                ti.xcom_push(key='yeedu_error_summary', value=error_summary)

            # Push Yeedu run URL
            yeedu_url = getattr(operator, 'yeedu_run_url', None)
            if yeedu_url:
                ti.xcom_push(key='yeedu_run_url', value=yeedu_url)
        except Exception as e:
            self.log.warning(f"Failed to push execution info to XCom: {e}")

    def _validate_conf(self, conf: List[str]) -> List[str]:
        """
        Validate configuration format and process duplicates.

        :param conf: Set of configuration strings
        :return: Processed list with only the last occurrence of duplicate keys.
        :raises AirflowException: If any conf item is not in correct format or if validation fails.
        """

        processed_conf = {}
        for item in conf:
            if '=' not in item:
                raise AirflowException(
                    f"Invalid conf format for '{item}'. Must be in 'key=value' format")

            key, value = item.split(sep='=', maxsplit=1)
            if not key or not value:
                raise AirflowException(
                    f"Invalid conf item '{item}'. Both key and value must be non-empty")

            if key in processed_conf:
                self.log.warning(
                    f"Duplicate configuration key found: '{key}'. "
                    f"Value '{processed_conf[key]}' will be overwritten with '{value}'"
                )
            processed_conf[key] = value

        # Convert processed dict back to list of "key=value" strings
        return [f"{k}={v}" for k, v in processed_conf.items()]
