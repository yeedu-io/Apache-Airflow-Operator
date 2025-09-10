# hooks/graph_api_hook.py
import msal
import os
import requests
from airflow.hooks.base import BaseHook

class EmailNotificationHook(BaseHook):
    """
    A hook that sends emails via Microsoft Graph API.  Credentials are loaded from
    environment variables.  Recipients can be passed as a string or list of strings.
    """

    def __init__(self):
        self.tenant_id = os.getenv("TENANT_ID")
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.sender = os.getenv("SENDER_EMAIL")
        if not all([self.tenant_id, self.client_id, self.client_secret, self.sender]):
            raise ValueError("One or more required environment variables are missing!")
        self.api_url = "https://graph.microsoft.com/v1.0"
        self.token = self.get_oauth_token()

    def get_oauth_token(self) -> str:
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )
        token_response = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
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

    def _generate_html(self, identifier: str, run_id: str, status: str, is_dag=False) -> str:
        """
        Build a styled HTML email body.  When is_dag=True the identifier is a DAG id,
        otherwise it is a task id.
        """
        status_msg = "SUCCESS" if status.lower() == "success" else "FAILED"
        heading = "Airflow DAG Notification" if is_dag else "Airflow Task Notification"
        name_label = "DAG Name" if is_dag else "Task ID"

        # ✅ Pick text color in Python
        if status_msg.lower() == "success":
            status_color = "#28a745"  # green
        elif status_msg.lower() == "failed":
            status_color = "#dc3545"  # red
        elif status_msg.lower() == "running":
            status_color = "#007bff"  # blue
        else:
            status_color = "#6c757d"  # grey

        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #f7f9fc; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: #ffffff; border-radius: 8px; 
                        box-shadow: 0 2px 6px rgba(0,0,0,0.1); padding: 20px;">
            <h2 style="text-align: center; color: #333333; margin-bottom: 20px;">{heading}</h2>
            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                <tr>
                <td style="padding: 10px; font-weight: bold; border-bottom: 1px solid #eaeaea; width: 30%;">{name_label}</td>
                <td style="padding: 10px; border-bottom: 1px solid #eaeaea;">{identifier}</td>
                </tr>
                <tr>
                <td style="padding: 10px; font-weight: bold; border-bottom: 1px solid #eaeaea;">Run ID</td>
                <td style="padding: 10px; border-bottom: 1px solid #eaeaea;">{run_id}</td>
                </tr>
                <tr>
                <td style="padding: 10px; font-weight: bold;">Status</td>
                <td style="padding: 10px; font-weight: bold; color: {status_color};">
                    {status_msg}
                </td>
                </tr>
            </table>
            </div>
        </body>
        </html>
        """

    # Public methods for callbacks
    def notify_task(self, recipients, task_id: str, run_id: str, status: str) -> None:
        subject = f"Airflow Task {task_id} {status.capitalize()}"
        body = self._generate_html(task_id, run_id, status, is_dag=False)
        self._send_email(recipients, subject, body)

    def notify_dag(self, recipients, dag_id: str, run_id: str, status: str) -> None:
        subject = f"Airflow DAG {dag_id} {status.capitalize()}"
        body = self._generate_html(dag_id, run_id, status, is_dag=True)
        self._send_email(recipients, subject, body)
