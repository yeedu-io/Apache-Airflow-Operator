from typing import Optional, Union, Tuple
from airflow.exceptions import AirflowException
import logging
from yeedu.hooks.yeedu import YeeduHook
from airflow.utils.decorators import apply_defaults


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YeeduJobRunOperator:
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
        try:
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
            # Only logout for LDAP or AAD
            try:
                auth_type = self.hook.get_auth_type()
                if auth_type in ["LDAP", "AAD"]:
                    self.hook.yeedu_logout(context)
            except Exception as e:
                logger.warning(f"Logout skipped or failed: {e}")