import msal
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
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
        self, identifier: str, run_id: str, status: str, is_dag=False, context=None, extra_info=None
    ) -> str:
        """
        Build a styled HTML email body.
        When is_dag=True the identifier is a DAG id, otherwise a task id.
        """
        status_msg = "SUCCESS" if status.lower() == "success" else "FAILED"
        heading = "Airflow DAG Notification" if is_dag else "Airflow Task Notification"
        name_label = "DAG Name" if is_dag else "Task ID"
        url = re.search(r'(https?://\S+)', extra_info).group(1) if extra_info and re.search(r'(https?://\S+)', extra_info) else None


        # Pick text color in Python
        if status_msg.lower() == "success":
            status_color = "#28a745"  # green
        elif status_msg.lower() == "failed":
            status_color = "#dc3545"  # red
        elif status_msg.lower() == "running":
            status_color = "#007bff"  # blue
        else:
            status_color = "#6c757d"  # grey

        rows = []
        rows.append(
            f"<tr><td style='padding:10px;font-weight:bold;width:30%'>{name_label}</td><td>{identifier}</td></tr>"
        )
        rows.append(
            f"<tr><td style='padding:10px;font-weight:bold;'>Run ID</td><td>{run_id}</td></tr>"
        )
        rows.append(
            f"<tr><td style='padding:10px;font-weight:bold;'>Status</td>"
            f"<td style='font-weight:bold;color:{status_color};'>{status_msg}</td></tr>"
        )

        if is_dag and context:
            # --- DAG RUN DETAILS ---
            dr = context.get("dag_run")
            if dr:
                logical_date = str(dr.logical_date) if dr.logical_date else None
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
                        f"<tr><td style='padding:10px;font-weight:bold;'>Logical Date</td><td>{logical_date}</td></tr>"
                    )
                if run_type:
                    rows.append(
                        f"<tr><td style='padding:10px;font-weight:bold;'>Run Type</td><td>{run_type}</td></tr>"
                    )
                if start:
                    rows.append(
                        f"<tr><td style='padding:10px;font-weight:bold;'>Start</td><td>{start}</td></tr>"
                    )
                if end:
                    rows.append(
                        f"<tr><td style='padding:10px;font-weight:bold;'>End</td><td>{end}</td></tr>"
                    )
                if duration:
                    rows.append(
                        f"<tr><td style='padding:10px;font-weight:bold;'>Duration</td><td>{duration}</td></tr>"
                    )
                if dag_version:
                    rows.append(
                        f"<tr><td style='padding:10px;font-weight:bold;'>DAG Version(s)</td><td>{dag_version}</td></tr>"
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
                owner = getattr(dag, "owner", None) or dag.default_args.get("owner")

            # log_url = ti.log_url if ti else None
            duration = (
                str(ti.end_date - ti.start_date)
                if ti and ti.start_date and ti.end_date
                else None
            )

            if execution_date:
                rows.append(
                    f"<tr><td style='padding:10px;font-weight:bold;'>Execution Date</td><td>{execution_date}</td></tr>"
                )
            if owner:
                rows.append(
                    f"<tr><td style='padding:10px;font-weight:bold;'>Owner</td><td>{owner}</td></tr>"
                )
            if duration:
                rows.append(
                    f"<tr><td style='padding:10px;font-weight:bold;'>Duration</td><td>{duration}</td></tr>"
                )
            # if log_url:
            #     rows.append(
            #         f"<tr><td style='padding:10px;font-weight:bold;'>Log URL</td><td><a href='{log_url}'>View Logs</a></td></tr>"
            #     )

        # Notebook URL (doc_md passed from DAG or task)
        if extra_info:
            rows.append(
                f"<tr><td style='padding:10px;font-weight:bold;'>Notebook URL</td><td><a href='{url}' target='_blank'>{url}</a></td></tr>"
            )

        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #f7f9fc; padding: 20px;">
            <div style="max-width: 700px; margin: auto; background: #ffffff; border-radius: 8px; 
                        box-shadow: 0 2px 6px rgba(0,0,0,0.1); padding: 20px;">
            <h2 style="text-align: center; color: #333333; margin-bottom: 20px;">{heading}</h2>
            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                {''.join(rows)}
            </table>
            </div>
        </body>
        </html>
        """

    # Public methods for callbacks
    def notify_task(
        self, recipients, task_id: str, run_id: str, status: str, context=None, extra_info=None
    ) -> None:
        if not recipients: 
            return None
        subject = f"Airflow Task {task_id} {status.capitalize()}"
        body = self._generate_html(
            task_id, run_id, status, is_dag=False, context=context, extra_info=extra_info
        )
        self._send_email(recipients, subject, body)

    def notify_dag(
        self, recipients, dag_id: str, run_id: str, status: str, context=None, extra_info=None
    ) -> None:
        if not recipients: 
            return None
        subject = f"Airflow DAG {dag_id} {status.capitalize()}"
        body = self._generate_html(
            dag_id, run_id, status, is_dag=True, context=context, extra_info=extra_info
        )
        self._send_email(recipients, subject, body)
