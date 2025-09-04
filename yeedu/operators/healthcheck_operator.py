from airflow.exceptions import AirflowException
from yeedu.hooks.yeedu import YeeduHook


class YeeduHealthCheckOperator:
    """
    YeeduHealthCheckOperator performs a health check on a Yeedu API endpoint.

    This operator makes a request to the Yeedu API health check endpoint and logs the status.

    :param base_url: Base URL for the Yeedu API (mandatory).
    :type base_url: str
    :param connection_id: Airflow connection ID to retrieve connection information (mandatory).
    :type connection_id: str
    :param logger: Logger object for logging messages (optional).
    :type logger: logging.Logger
    """

    def __init__(
        self,
        base_url: str,
        connection_id: str,
        logger=None,
        *args,
        **kwargs,
    ) -> None:
        """
        Initialize the YeeduHealthCheckOperator.

        :param base_url: Base URL for the Yeedu API.
        :param connection_id: Airflow connection ID to use.
        :param logger: Logger object for logging messages (optional).
        """
        super().__init__(*args, **kwargs)
        self.base_url = base_url
        self.connection_id = connection_id
        self.hook: YeeduHook = YeeduHook(
            conf_id=None,
            tenant_id=None,
            base_url=self.base_url,
            workspace_id=None,
            connection_id=self.connection_id,
            token_variable_name=None
        )
        self.log = logger

    def execute(self, context: dict) -> None:
        """
        Execute the health check operation against the Yeedu API endpoint.

        This method performs a health check by calling the YeeduHook's health check method
        and logs the response status code. Any exceptions are caught and raised as
        AirflowExceptions. The HTTP session is properly closed in the finally block.

        :param context: Airflow context dictionary containing execution information.
        :raises AirflowException: If the health check fails for any reason.
        """
        try:
            health_check_status: str = self.hook.yeedu_health_check()
            self.log.info(
                f"Health Check Status: {health_check_status.status_code}")
        except Exception as e:
            self.log.error(f"Health check operation failed: {str(e)}")
            raise AirflowException(f"Yeedu health check failed: {str(e)}")

        finally:
            # Close HTTP session if it exists
            if hasattr(self, 'hook') and hasattr(self.hook, 'session'):
                try:
                    self.hook.session.close()
                    self.log.info("HTTP session closed in finally block.")
                except Exception as session_close_error:
                    self.log.warning(
                        f"Failed to close HTTP session: {session_close_error}")
            self.log.info("Finished Health Check Operator execution.")
