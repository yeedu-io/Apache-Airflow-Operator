from typing import Optional, Tuple, Union
from airflow.exceptions import AirflowException
import logging
from yeedu.hooks.yeedu import YeeduHook
from airflow.utils.decorators import apply_defaults

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        Initialize the YeeduHealthCheckOperator.

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