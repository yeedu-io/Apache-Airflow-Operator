import msal
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.configuration import conf as airflow_conf
import re


class EmailNotificationHook(BaseHook):
    """
    A hook that sends emails via Microsoft Graph API.

    Credentials are loaded from Airflow Variables.
    Recipients can be passed as a string or list of strings.
    """

    def __init__(self):
        self.tenant_id = Variable.get("AIRFLOW_VAR_TENANT_ID")
        self.client_id = Variable.get("AIRFLOW_VAR_CLIENT_ID")
        self.client_secret = Variable.get("AIRFLOW_VAR_CLIENT_SECRET")
        self.sender = Variable.get("AIRFLOW_VAR_SENDER_EMAIL")
        self.airflow_base_url = self._get_airflow_base_url()

        # Check which variables are missing
        missing_vars = []
        if not self.tenant_id:
            missing_vars.append("TENANT_ID")
        if not self.client_id:
            missing_vars.append("CLIENT_ID")
        if not self.client_secret:
            missing_vars.append("CLIENT_SECRET")
        if not self.sender:
            missing_vars.append("SENDER_EMAIL")

        if missing_vars:
            raise ValueError(
                f"Required environment variables missing: {', '.join(missing_vars)}")

        self.api_url = "https://graph.microsoft.com/v1.0"
        self.token = self.get_oauth_token()

    def get_oauth_token(self) -> str:
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )
        token_response = app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"])
        if "access_token" not in token_response:
            raise Exception(
                f"Failed to acquire access token: {token_response.get('error_description')}"
            )
        return token_response["access_token"]

    def _get_airflow_base_url(self) -> str:
        """
        Get Airflow webserver/API base URL, compatible with Airflow 2.x and 3.x.
        Airflow 3.x moved [webserver] base_url to [api] base_url.
        Falls back to Variable if config is not set.
        """
        # Try Airflow 3.x location first
        try:
            base_url = airflow_conf.get("api", "base_url")
            if base_url:
                return base_url.rstrip('/')
        except Exception:
            pass

        # Fall back to Airflow 2.x location
        try:
            base_url = airflow_conf.get("webserver", "base_url")
            if base_url:
                return base_url.rstrip('/')
        except Exception:
            pass

        # Final fallback: check Airflow Variable
        try:
            base_url = Variable.get("AIRFLOW_VAR_BASE_URL", default_var=None)
            if base_url:
                return base_url.rstrip('/')
        except Exception:
            pass

        # Default fallback
        return "http://localhost:8080"

    def _send_email(self, recipients, subject: str, body: str) -> None:
        """Internal helper to send the email via Graph API."""
        if isinstance(recipients, str):
            recipients = [recipients]
        message = {
            "message": {
                "subject": subject,
                "body": {"contentType": "HTML", "content": body},
                "toRecipients": [
                    {"emailAddress": {"address": r}} for r in recipients
                ],
            }
        }
        send_mail_url = f"{self.api_url}/users/{self.sender}/sendMail"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        response = requests.post(send_mail_url, json=message, headers=headers)
        if response.status_code != 202:
            raise Exception(
                f"Failed to send email: {response.status_code} - {response.text}"
            )
        self.log.info(f"Sent email to {recipients} with subject '{subject}'")

    def _generate_html(
        self, identifier: str, run_id: str, status: str, is_dag=False, context=None,
        extra_info=None, error_summary: str = None, cluster_bump_info: str = None
    ) -> str:
        """
        Build a styled HTML email body with visual hierarchy and actionable links.

        Args:
            identifier: DAG ID or Task ID
            run_id: The run ID
            status: success/failed/running
            is_dag: True if this is a DAG-level notification
            context: Airflow context dictionary
            extra_info: Additional info (typically Yeedu notebook/job URL)
            error_summary: Error details for failed runs
            cluster_bump_info: Information about cluster bump attempts
        """
        status_msg = "SUCCESS" if status.lower() == "success" else "FAILED"
        heading = "Airflow DAG Notification" if is_dag else "Airflow Task Notification"
        name_label = "DAG Name" if is_dag else "Task ID"

        # Extract Yeedu URL from extra_info
        yeedu_url = re.search(r'(https?://\S+)', extra_info).group(
            1) if extra_info and re.search(r'(https?://\S+)', extra_info) else None

        # Pick status color and banner style using brand palette
        if status_msg.lower() == "success":
            status_color = "#28a745"  # green for success
            banner_bg = "#e8f5e9"
            banner_border = "#c8e6c9"
        elif status_msg.lower() == "failed":
            status_color = "#D85040"  # brand red
            banner_bg = "#fdecea"
            banner_border = "#f5c6cb"
        elif status_msg.lower() == "running":
            status_color = "#3380F6"  # brand blue
            banner_bg = "#e3f2fd"
            banner_border = "#bbdefb"
        else:
            status_color = "#5F6368"  # brand grey
            banner_bg = "#f5f5f5"
            banner_border = "#e0e0e0"

        # Build Quick Links section
        quick_links = []
        if context:
            dag_run = context.get("dag_run")
            ti = context.get("task_instance")

            if dag_run and self.airflow_base_url:
                dag_id = dag_run.dag_id
                dag_run_id = dag_run.run_id

                # DAG run detail page
                dag_run_url = f"{self.airflow_base_url}/dags/{dag_id}/runs/{dag_run_id}"
                quick_links.append(
                    f"<a href='{dag_run_url}' style='color:#3380F6;text-decoration:none;font-weight:500;'>View DAG Run</a>")

            if ti and not is_dag and self.airflow_base_url:
                # Task detail page with logs
                task_log_url = f"{self.airflow_base_url}/dags/{ti.dag_id}/runs/{ti.run_id}/tasks/{ti.task_id}"
                quick_links.append(
                    f"<a href='{task_log_url}' style='color:#3380F6;text-decoration:none;font-weight:500;'>View Task Logs</a>")

        if yeedu_url:
            quick_links.append(
                f"<a href='{yeedu_url}' target='_blank' style='color:#3380F6;text-decoration:none;font-weight:500;'>View in Yeedu</a>")

        quick_links_html = ""
        if quick_links:
            links_formatted = " &nbsp;|&nbsp; ".join(quick_links)
            quick_links_html = f"""
            <div style="background:#FFEFE6;padding:12px;border-radius:4px;margin:15px 0;text-align:center;">
                {links_formatted}
            </div>
            """

        # Status banner
        status_banner = f"""
        <div style="background:{banner_bg};border:2px solid {banner_border};border-radius:5px;padding:15px;margin:15px 0;text-align:center;">
            <span style="font-size:20px;font-weight:bold;color:{status_color};">{status_msg}</span>
            <div style="font-size:14px;color:#666;margin-top:5px;">{identifier}</div>
        </div>
        """

        # Build info table rows
        rows = []
        rows.append(
            f"<tr><td style='padding:8px 10px;font-weight:500;width:30%;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>{name_label}</td><td style='padding:8px 10px;color:#25221E;border-bottom:1px solid #e0e0e0;'>{identifier}</td></tr>"
        )
        rows.append(
            f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>Run ID</td><td style='padding:8px 10px;color:#5F6368;border-bottom:1px solid #e0e0e0;font-family:monospace;font-size:12px;'>{run_id}</td></tr>"
        )

        if is_dag and context:
            # --- DAG RUN DETAILS ---
            dr = context.get("dag_run")
            if dr:
                logical_date = str(
                    dr.logical_date) if dr.logical_date else None
                run_type = dr.run_type
                start = str(dr.start_date) if dr.start_date else None
                end = str(dr.end_date) if dr.end_date else None
                duration = (
                    str(dr.end_date - dr.start_date)
                    if dr.start_date and dr.end_date
                    else None
                )
                dag_version = getattr(dr, "dag_version", None)

                if logical_date:
                    rows.append(
                        f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>Logical Date</td><td style='padding:8px 10px;color:#25221E;border-bottom:1px solid #e0e0e0;'>{logical_date}</td></tr>"
                    )
                if run_type:
                    rows.append(
                        f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>Run Type</td><td style='padding:8px 10px;color:#25221E;border-bottom:1px solid #e0e0e0;'>{run_type}</td></tr>"
                    )
                if start:
                    rows.append(
                        f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>Start</td><td style='padding:8px 10px;color:#25221E;border-bottom:1px solid #e0e0e0;'>{start}</td></tr>"
                    )
                if end:
                    rows.append(
                        f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>End</td><td style='padding:8px 10px;color:#25221E;border-bottom:1px solid #e0e0e0;'>{end}</td></tr>"
                    )
                if duration:
                    rows.append(
                        f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>Duration</td><td style='padding:8px 10px;color:#F2600C;border-bottom:1px solid #e0e0e0;font-weight:500;'>{duration}</td></tr>"
                    )
                if dag_version:
                    rows.append(
                        f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>DAG Version(s)</td><td style='padding:8px 10px;color:#25221E;border-bottom:1px solid #e0e0e0;'>{dag_version}</td></tr>"
                    )

        if not is_dag and context:
            # --- TASK DETAILS ---
            ti = context.get("task_instance")
            dag = context.get("dag")

            execution_date = str(
                context.get("logical_date") or context.get("execution_date")
            )
            owner = None
            if dag:
                owner = getattr(
                    dag, "owner", None) or dag.default_args.get("owner")

            duration = (
                str(ti.end_date - ti.start_date)
                if ti and ti.start_date and ti.end_date
                else None
            )

            if execution_date:
                rows.append(
                    f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>Execution Date</td><td style='padding:8px 10px;color:#25221E;border-bottom:1px solid #e0e0e0;'>{execution_date}</td></tr>"
                )
            if owner:
                rows.append(
                    f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>Owner</td><td style='padding:8px 10px;color:#25221E;border-bottom:1px solid #e0e0e0;'>{owner}</td></tr>"
                )
            if duration:
                rows.append(
                    f"<tr><td style='padding:8px 10px;font-weight:500;background:#fafafa;color:#25221E;border-bottom:1px solid #e0e0e0;'>Duration</td><td style='padding:8px 10px;color:#F2600C;border-bottom:1px solid #e0e0e0;font-weight:500;'>{duration}</td></tr>"
                )

        # Add cluster bump info if provided
        if cluster_bump_info:
            rows.append(
                f"<tr><td style='padding:8px 10px;font-weight:500;background:#FFEFE6;color:#25221E;border-bottom:1px solid #e0e0e0;'>Cluster Bump</td><td style='padding:8px 10px;background:#FFEFE6;color:#F2600C;border-bottom:1px solid #e0e0e0;'>{cluster_bump_info}</td></tr>"
            )

        # Build error section for failures
        error_section = ""
        if status_msg == "FAILED" and error_summary:
            # Truncate error if too long
            error_display = error_summary
            if len(error_summary) > 2000:
                error_display = error_summary[:2000] + \
                    "\n\n... (truncated, see logs for full details)"

            error_section = f"""
            <div style="margin:15px 0;">
                <h3 style="color:#D85040;margin:10px 0;font-size:14px;">Error Details</h3>
                <div style="background:#fafafa;border-left:3px solid #D85040;padding:12px;font-family:monospace;font-size:12px;overflow-x:auto;white-space:pre-wrap;word-wrap:break-word;">{error_display}</div>
            </div>
            """

        return f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; background-color: #f5f5f5; padding: 20px; margin: 0;">
            <div style="max-width: 600px; margin: auto; background: #ffffff; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <!-- Header -->
                <div style="background: #F2600C; padding: 20px; text-align: center;">
                    <h2 style="color: #ffffff; margin: 0; font-size: 18px; font-weight: 600;">{heading}</h2>
                </div>
                
                <!-- Content -->
                <div style="padding: 20px;">
                    {status_banner}
                    {quick_links_html}
                    
                    <h3 style="color: #25221E; margin: 15px 0 10px 0; font-size: 14px; font-weight: 600;">Run Information</h3>
                    <table style="width: 100%; border-collapse: collapse; font-size: 13px; border: 1px solid #e0e0e0;">
                        {''.join(rows)}
                    </table>
                    
                    {error_section}
                </div>
                
                <!-- Footer -->
                <div style="background: #FFEFE6; padding: 12px 20px; text-align: center;">
                    <p style="margin: 0; font-size: 11px; color: #5F6368;">Automated notification from Yeedu Apache Airflow Operator</p>
                </div>
            </div>
        </body>
        </html>
        """

    # Public methods for callbacks
    def notify_task(
        self, recipients, task_id: str, run_id: str, status: str, context=None,
        extra_info=None, error_summary: str = None, cluster_bump_info: str = None
    ) -> None:
        """
        Send task-level notification email.

        Args:
            recipients: Email recipient(s) - string or list of strings
            task_id: The task ID
            run_id: The run ID
            status: success/failed/running
            context: Airflow context dictionary
            extra_info: Additional info (typically Yeedu URL)
            error_summary: Error details for failed tasks
            cluster_bump_info: Information about cluster bump attempts
        """
        if not recipients:
            return None

        # Build subject line
        dag_id = context.get("dag_run").dag_id if context and context.get(
            "dag_run") else "Unknown"
        subject = f"[{status.upper()}] Task: {task_id} | DAG: {dag_id}"

        body = self._generate_html(
            task_id, run_id, status, is_dag=False, context=context, extra_info=extra_info,
            error_summary=error_summary, cluster_bump_info=cluster_bump_info
        )
        self._send_email(recipients, subject, body)

    def notify_dag(
        self, recipients, dag_id: str, run_id: str, status: str, context=None,
        extra_info=None, error_summary: str = None, cluster_bump_info: str = None
    ) -> None:
        """
        Send DAG-level notification email.

        Args:
            recipients: Email recipient(s) - string or list of strings
            dag_id: The DAG ID
            run_id: The run ID
            status: success/failed/running
            context: Airflow context dictionary
            extra_info: Additional info
            error_summary: Error details for failed DAGs
            cluster_bump_info: Information about cluster bump attempts
        """
        if not recipients:
            return None

        # Build subject line
        logical_date = ""
        if context and context.get("dag_run"):
            dr = context.get("dag_run")
            if dr.logical_date:
                logical_date = f" | {str(dr.logical_date)[:10]}"

        subject = f"[{status.upper()}] DAG: {dag_id}{logical_date}"

        body = self._generate_html(
            dag_id, run_id, status, is_dag=True, context=context, extra_info=extra_info,
            error_summary=error_summary, cluster_bump_info=cluster_bump_info
        )
        self._send_email(recipients, subject, body)
