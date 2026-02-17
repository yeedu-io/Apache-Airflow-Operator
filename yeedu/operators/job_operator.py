from typing import Optional, Union, Tuple, List
from airflow.exceptions import AirflowException
from yeedu.hooks.yeedu import YeeduHook
import time
import re


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
        cluster_ids: List[int] = None,
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
        self.cluster_ids = cluster_ids or []
        self._BUMP_PATTERN = re.compile(
            r'exit code:?\s*(?:137|143|139|134|52)|'
            r'oomkilled|outofmemoryerror|java heap space|'
            r'gc overhead limit exceeded|killed process|'
            r'sigkill|sigterm|sigsegv|segmentation fault|sigabrt|'
            r'sparkexitcode|exceed_max_executor_failures|'
            r'driver_timeout|container killed by yarn for exceeding memory limits',
            re.IGNORECASE
        )
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

        # Tracking for email notifications
        self.attempted_clusters: List[int] = []
        self.last_error_summary: Optional[str] = None
        self.yeedu_run_url: Optional[str] = None

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
                self.hook.get_job_workflow_errors(run_id) or [])

            if self._BUMP_PATTERN.search(wf_errors):
                return True

            stderr = self.hook.get_job_logs(
                run_id, "stderr", last_n_lines=1000) or ""

            if self._BUMP_PATTERN.search(stderr):
                return True

            stdout = self.hook.get_job_logs(
                run_id, "stdout", last_n_lines=1000) or ""

            if self._BUMP_PATTERN.search(stdout):
                return True

            return False
        except Exception as e:
            self.log.warning(
                f"Failed log analysis for cluster bump decision: {e}")
            return False

    def _build_error_summary(self, run_id: int, stderr: str = None) -> str:
        """
        Build a concise error summary from job logs for email notifications.

        Args:
            run_id: The run ID to fetch errors for
            stderr: Pre-fetched stderr logs (optional)

        Returns:
            A truncated error summary string
        """
        try:
            # Get workflow errors first (most relevant)
            wf_errors = self.hook.get_job_workflow_errors(run_id) or []
            if wf_errors:
                # Last 10 workflow errors
                error_text = "\n".join(wf_errors[-10:])
                return error_text[:1500] if len(error_text) > 1500 else error_text

            # Fall back to stderr
            if stderr:
                lines = stderr.strip().split('\n')
                last_lines = lines[-20:] if len(lines) > 20 else lines
                error_text = "\n".join(last_lines)
                return error_text[:1500] if len(error_text) > 1500 else error_text

            return "No error details available"
        except Exception as e:
            return f"Failed to retrieve error details: {e}"

    def run_job(self, cluster_id=None) -> tuple:
        """
        Runs a job on a specified cluster and handles the complete job lifecycle.

        Args:
            cluster_id (int, optional): The cluster ID to run the job on. 
                                       If None, uses the existing cluster.

        Returns:
            tuple: (success, run_id, job_status, exception)
                  - success (bool): True if job completed successfully, False otherwise
                  - run_id (int): The run ID of the submitted job
                  - job_status (str): Final status of the job
                  - exception (Exception): Exception object if job failed, None otherwise
        """
        run_id = None
        job_status = None
        exception = None

        try:
            # Update cluster binding if specified
            if cluster_id is not None:
                self.log.info(
                    f"Binding job {self.job_id} to cluster {cluster_id}")
                self.hook.update_job_cluster(
                    job_id=self.job_id, cluster_id=int(cluster_id))

            # Submit job
            self.log.info(f"Submitting job {self.job_id}")
            run_id = self.hook.submit_job(
                self.job_id,
                arguments=self.arguments,
                conf=self.conf
            )

            # Store the run_id for templating and later use
            self.run_id = run_id

            # Generate job URL for monitoring
            job_run_url = f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}/run/{run_id}/run-metrics?type=spark_job".replace(
                f":{self.restapi_port}/api/v1", ":5173/"
            )
            self.yeedu_run_url = job_run_url
            self.log.info(
                f"Job submitted with Run ID: {run_id}. Monitor at: {job_run_url}")

            # Wait for job completion
            self.log.info(
                f"Waiting for job {run_id} to complete")
            job_status = self.hook.wait_for_completion(run_id)
            self.log.info(f"Job {run_id} completed with status: {job_status}")

            # Fetch job logs
            self.log.info(
                f"Retrieving logs after 40 seconds sleep for run id: {run_id}")
            time.sleep(40)  # Ensure logs are available
            job_log_stdout = self.hook.get_job_logs(run_id, "stdout") or ""
            job_log_stderr = self.hook.get_job_logs(run_id, "stderr") or ""
            job_log = f" stdout: {job_log_stdout} stderr: {job_log_stderr}"
            self.log.info(
                f"Retrieved logs for run ID: {run_id}, logs: {job_log}")

            # Check if job was successful
            if job_status in ["ERROR", "TERMINATED", "STOPPED"]:
                self.log.error(
                    f"Job {run_id} failed with status: {job_status}")
                # Capture error summary for notifications
                self.last_error_summary = self._build_error_summary(
                    run_id, job_log_stderr)
                exception = AirflowException(
                    f"Job failed with status '{job_status}', logs: {job_log}")
                return False, run_id, job_status, exception

            # Success case
            self.log.info(f"Job {run_id} completed successfully")
            return True, run_id, job_status, None

        except Exception as e:
            self.log.error(f"Error executing job: {str(e)}")
            exception = e
            return False, run_id, job_status, exception

        finally:
            self.log.info("Stopping job in finally")

            # If run_id exists, ensure job is killed
            if run_id is not None:
                try:
                    status_response = self.hook.get_job_status(run_id)
                    status = status_response.json().get("run_status")

                    if status in ["RUNNING", "SUBMITTED"]:
                        self.log.info(
                            f"Stopping run id: {run_id}")
                        self.hook.kill_job(run_id)
                except Exception as stop_error:
                    self.log.warning(
                        f"Error during stopping the job run: {str(stop_error)}")

    def execute(self, context: dict) -> None:
        """
        Execute the Yeedu job with cluster bump logic.

        First executes the job on the current cluster. If the job fails and meets cluster bump criteria,
        it will retry on subsequent clusters in the cluster_ids list.
        """
        clusters = self.cluster_ids[:] if self.cluster_ids else []

        try:
            # Login to Yeedu
            self.hook.yeedu_login(context)

            if self.cluster_ids:
                self.log.info(
                    f"Cluster bump enabled, planned clusters in order: {self.cluster_ids}"
                )

            # First attempt: run using the job's current cluster binding
            self.log.info(
                f"Running job {self.job_id} on the existing cluster configuration"
            )
            success, run_id, job_status, exception = self.run_job()

            # If job succeeded, we're done
            if success:
                self.log.info(
                    f"Job {self.job_id} completed successfully without cluster bump"
                )
                return

            # If job failed, check if we should attempt cluster bump
            should_bump = False
            if job_status in ["ERROR", "TERMINATED", "STOPPED"]:
                # Only check for cluster bump if we have a terminal failure status
                should_bump = self._should_bump_cluster_from_logs(run_id)

            if not should_bump:
                self.log.info(
                    "Cluster bump skipped because failure logs do not match bump criteria"
                )
                raise exception or AirflowException(
                    f"Job failed with status: {job_status}")

            # Start cluster bump process using remaining clusters, if any
            available_clusters = clusters

            if not available_clusters:
                self.log.error(
                    "Cluster bump requested but no additional clusters are configured"
                )
                raise exception or AirflowException(
                    f"Job failed with status: {job_status}"
                )

            # Try on subsequent clusters
            total_attempts = len(available_clusters)
            for attempt, cluster_id in enumerate(available_clusters, start=1):
                self.attempted_clusters.append(cluster_id)
                self.log.info(
                    f"Cluster bump attempt {attempt}/{total_attempts}: switching to cluster {cluster_id}"
                )

                # Run job on this cluster
                success, run_id, job_status, exception = self.run_job(
                    cluster_id)

                # If job succeeded, we're done
                if success:
                    self.log.info(
                        f"Job {self.job_id} completed successfully after cluster bump to cluster {cluster_id}"
                    )
                    # Clear error summary on success
                    self.last_error_summary = None
                    return

                # Check if we should continue bumping
                should_continue_bump = False
                if job_status in ["ERROR", "TERMINATED", "STOPPED"]:
                    # Only check for cluster bump if we have a terminal failure status
                    should_continue_bump = self._should_bump_cluster_from_logs(
                        run_id
                    )

                # If this is the last cluster or we shouldn't bump anymore, raise the exception
                if attempt == total_attempts or not should_continue_bump:
                    if attempt == total_attempts:
                        self.log.error(
                            "Cluster bump exhausted all configured clusters without success"
                        )
                    if not should_continue_bump:
                        self.log.error(
                            "Cluster bump stopped because the latest failure is not eligible for another bump"
                        )
                    raise exception or AirflowException(
                        f"Job failed with status: {job_status}"
                    )

                self.log.info(
                    f"Cluster bump will continue, preparing next cluster after failure on cluster {cluster_id}"
                )

        except Exception as e:
            self.log.error(f"Job execution failed: {str(e)}")
            raise

        finally:
            # Cleanup: logout and close connections
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
