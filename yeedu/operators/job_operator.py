from typing import Optional, Union, Tuple, List
from airflow.exceptions import AirflowException
from yeedu.hooks.yeedu import YeeduHook


class YeeduJobRunOperator:
    template_fields: Tuple[str] = ("run_id",)

    def __init__(
        self,
        job_id: str,
        base_url: str,
        workspace_id: int,
        tenant_id: str,
        connection_id: str,
        token_variable_name: str,
        restapi_port: int,
        arguments: str = None,
        conf: List[str] = None,
        logger=None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.job_id: str = job_id
        self.tenant_id: str = tenant_id
        self.base_url: str = base_url
        self.workspace_id: int = workspace_id
        self.connection_id = connection_id
        self.token_variable_name = token_variable_name
        self.restapi_port = restapi_port
        self.arguments = arguments
        self.conf = conf
        self.hook: YeeduHook = YeeduHook(
            conf_id=self.job_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
        )
        self.run_id: Optional[Union[int, None]] = None
        self.log = logger

    def execute(self, context: dict) -> None:
        try:
            ti = context['ti']
            run_id = ti.run_id
            map_index = ti.map_index
            task_id = ti.task_id

            params = context.get("params", {}).get("input") or {}
            composite_key = f"{run_id}__{task_id}__{map_index}"
            ti.xcom_push(key=composite_key, value=params)

            self.hook.yeedu_login(context)
            self.log.info("Job Id: %s", self.job_id)
            run_id = self.hook.submit_job(
                self.job_id,
                arguments=self.arguments,
                conf=self.conf
            )
            restapi_port = self.restapi_port

            self.log.info("Job Submited (Job Id: %s)", run_id)
            job_run_url = f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}/run/{run_id}/run-metrics?type=spark_job".replace(
                f":{restapi_port}/api/v1", ""
            )
            self.log.info(
                "Check Yeedu Job run status and logs here " + job_run_url)
            job_status: str = self.hook.wait_for_completion(run_id)

            self.log.info("Final Job Status: %s", job_status)

            job_log_stdout: str = self.hook.get_job_logs(run_id, "stdout")
            job_log_stderr: str = self.hook.get_job_logs(run_id, "stderr")
            job_log: str = " stdout: " + job_log_stdout + " stderr: " + job_log_stderr
            self.log.info("Logs for run ID %s (%s)", run_id, job_log)

            if job_status in ["ERROR", "TERMINATED", "KILLED", "STOPPED"]:
                self.log.error(job_log)
                raise AirflowException(job_log)

        except Exception as e:
            raise AirflowException(e)

        finally:
            self.log.info("Stopping job in finally")
            job_status = self.hook.get_job_status(
                run_id).json().get("run_status")

            if job_status not in ["ERROR", "TERMINATED", "KILLED", "STOPPED", "DONE"]:
                self.hook.kill_job(run_id)

            # Only logout for LDAP or AAD
            try:
                auth_type = self.hook.yeedu_auth_type
                if auth_type in ["LDAP", "AAD"]:
                    self.hook.yeedu_logout()
            except Exception as e:
                self.log.warning(f"Logout skipped or failed: {e}")

            # Close HTTP session if it exists
            if hasattr(self, 'hook') and hasattr(self.hook, 'session'):
                try:
                    self.hook.session.close()
                    self.log.info("HTTP session closed in finally block.")
                except Exception as session_close_error:
                    self.log.warning(
                        f"Failed to close HTTP session: {session_close_error}")
