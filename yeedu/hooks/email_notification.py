import msal
import requests
import traceback as _traceback
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.configuration import conf as airflow_conf
from airflow.utils import timezone
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
        # Check Airflow Variable
        try:
            base_url = Variable.get("AIRFLOW_VAR_BASE_URL", default_var=None)
            if base_url:
                return base_url.rstrip('/')
        except Exception:
            pass

        # Final fallback: Try Airflow 3.x location first
        try:
            base_url = airflow_conf.get("api", "base_url")
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
        Build a styled HTML email body.

        Args:
            identifier: DAG ID or Task ID
            run_id: The Airflow DAG run ID
            status: success/failed/running
            is_dag: True if this is a DAG-level notification
            context: Airflow context dictionary
            extra_info: Additional info (typically Yeedu run URL)
            error_summary: Error details from operator XCom (post-execution failures)
            cluster_bump_info: Information about cluster bump attempts
        """
        status_msg = "SUCCESS" if status.lower() == "success" else "FAILED"
        heading = "Airflow DAG Notification" if is_dag else "Airflow Task Notification"

        # Extract Yeedu URL from extra_info
        yeedu_url_match = re.search(
            r'(https?://\S+)', extra_info) if extra_info else None
        yeedu_url = yeedu_url_match.group(1) if yeedu_url_match else None

        # Determine Yeedu resource label and run ID by inspecting the URL's type param
        yeedu_label = "Yeedu Run"
        yeedu_run_id_str = None
        if yeedu_url:
            if "type=spark_job" in yeedu_url:
                yeedu_label = "Yeedu Job Run"
            elif "type=notebook" in yeedu_url:
                yeedu_label = "Yeedu Notebook Run"
            run_id_match = re.search(r'/run/(\d+)/', yeedu_url)
            if run_id_match:
                yeedu_run_id_str = run_id_match.group(1)

        # Pick status color and banner style using brand palette
        if status_msg.lower() == "success":
            status_color = "#28a745"
            banner_bg = "#e8f5e9"
            banner_border = "#c8e6c9"
        elif status_msg.lower() == "failed":
            status_color = "#D85040"
            banner_bg = "#fdecea"
            banner_border = "#f5c6cb"
        elif status_msg.lower() == "running":
            status_color = "#3380F6"
            banner_bg = "#e3f2fd"
            banner_border = "#bbdefb"
        else:
            status_color = "#5F6368"
            banner_bg = "#f5f5f5"
            banner_border = "#e0e0e0"

        # Pre-compute Airflow UI URLs for clickable table values.
        # For DAG notifications, build URLs directly from the identifier (dag_id) and run_id
        # parameters since DAG-level callbacks may not populate context["dag_run"].
        dag_run_url = None
        dag_overview_url = None
        task_log_url = None
        dag_name = None
        if self.airflow_base_url:
            if is_dag:
                # identifier = dag_id, run_id = dag run_id — always available as params
                dag_overview_url = f"{self.airflow_base_url}/dags/{identifier}"
                dag_run_url = f"{self.airflow_base_url}/dags/{identifier}/runs/{run_id}"
            elif context:
                dr = context.get("dag_run")
                ti_ctx = context.get("task_instance")
                if dr:
                    dag_name = dr.dag_id
                    dag_run_url = f"{self.airflow_base_url}/dags/{dr.dag_id}/runs/{dr.run_id}"
                    dag_overview_url = f"{self.airflow_base_url}/dags/{dr.dag_id}"
                if ti_ctx:
                    task_log_url = (
                        f"{self.airflow_base_url}/dags/{ti_ctx.dag_id}"
                        f"/runs/{ti_ctx.run_id}/tasks/{ti_ctx.task_id}"
                    )
        # For task notifications without airflow_base_url, still extract dag_name from context
        if not is_dag and context and not dag_name:
            dr = context.get("dag_run")
            if dr:
                dag_name = dr.dag_id

        # Status banner — status only. All identifiers live in the table.
        status_banner = f"""
        <div style="background:{banner_bg};border:2px solid {banner_border};border-radius:5px;padding:12px 15px;margin:4px 0 9px 0;text-align:center;">
            <span style="font-size:18px;font-weight:bold;color:{status_color};">{status_msg}</span>
        </div>
        """

        # Helper: build a clickable link
        def _link(text, url, new_tab=False):
            target = " target='_blank'" if new_tab else ""
            return f"<a href='{url}'{target} style='color:#3380F6;text-decoration:underline;'>{text}</a>"

        # Helper: build a standard table row
        def _row(label, value, monospace=False, highlight=False):
            label_bg = "#FFEFE6" if highlight else "#fafafa"
            val_color = "#F2600C" if highlight else "#25221E"
            mono = "font-family:monospace;font-size:12px;" if monospace else ""
            return (
                f"<tr>"
                f"<td style='padding:8px 10px;font-weight:500;width:32%;background:{label_bg};"
                f"color:#25221E;border-bottom:1px solid #e0e0e0;'>{label}</td>"
                f"<td style='padding:8px 10px;color:{val_color};border-bottom:1px solid #e0e0e0;{mono}'>{value}</td>"
                f"</tr>"
            )

        # Helper: format timedelta as HH:MM:SS (start → end UTC)
        def _format_duration(start_dt, end_dt):
            delta = end_dt - start_dt
            total_secs = delta.total_seconds()
            # Human-friendly label matching Airflow UI style
            if total_secs < 60:
                delta_str = f"{total_secs:.2f}s"
            elif total_secs < 3600:
                m, s = divmod(int(total_secs), 60)
                delta_str = f"{m}m {s}s"
            else:
                h, rem = divmod(int(total_secs), 3600)
                m, s = divmod(rem, 60)
                delta_str = f"{h}h {m}m {s}s"
            start_str = start_dt.strftime("%H:%M:%S")
            end_str = end_dt.strftime("%H:%M:%S")
            return (
                f"<span style='color:#25221E;font-weight:500;'>{delta_str}</span>"
                f"&nbsp;&nbsp;<span style='color:#5F6368;font-size:12px;'>"
                f"({start_str} to {end_str} UTC)</span>"
            )

        # Build table rows
        rows = []

        if is_dag and context:
            # DAG Name — clickable to DAG overview
            dag_id_display = _link(
                identifier, dag_overview_url) if dag_overview_url else identifier
            rows.append(_row("DAG Name", dag_id_display))

            # DAG Run ID — clickable to run detail
            dag_run_id_display = _link(
                run_id, dag_run_url) if dag_run_url else run_id
            rows.append(_row("DAG Run ID", dag_run_id_display, monospace=True))

            dr = context.get("dag_run")
            if dr:
                if dr.logical_date:
                    rows.append(_row("Logical Date", str(dr.logical_date)))
                if dr.run_type:
                    rows.append(_row("Run Type", dr.run_type))
                # Duration: always calculated; fall back to now if end_date not yet set
                if dr.start_date:
                    end_dt = dr.end_date or timezone.utcnow()
                    rows.append(
                        _row("Duration", _format_duration(dr.start_date, end_dt)))
                dag_version = getattr(dr, "dag_version", None)
                if dag_version:
                    rows.append(_row("DAG Version(s)", str(dag_version)))

        if not is_dag and context:
            ti_ctx = context.get("task_instance")
            dag = context.get("dag")

            # DAG Name — clickable to DAG overview, first row for full context
            dag_name_display = _link(
                dag_name, dag_overview_url) if dag_name and dag_overview_url else (dag_name or "Unknown")
            rows.append(_row("DAG Name", dag_name_display))

            # DAG Run ID — clickable to run detail
            dag_run_id_display = _link(
                run_id, dag_run_url) if dag_run_url else run_id
            rows.append(_row("DAG Run ID", dag_run_id_display, monospace=True))

            # Task — clickable to task logs
            task_display = _link(
                identifier, task_log_url) if task_log_url else identifier
            rows.append(_row("Task", task_display))

            # Task Duration: always calculated; fall back to now if end_date not yet set
            # This covers the Airflow task lifecycle (auth + submission + polling + log fetch)
            if ti_ctx and ti_ctx.start_date:
                end_dt = ti_ctx.end_date or timezone.utcnow()
                rows.append(
                    _row("Task Duration", _format_duration(ti_ctx.start_date, end_dt)))

            # Owner
            if dag:
                owner = getattr(
                    dag, "owner", None) or dag.default_args.get("owner")
                if owner:
                    rows.append(_row("Owner", owner))

        # Yeedu run row — dynamic label, only if URL is available
        if yeedu_url:
            run_label = f"Run #{yeedu_run_id_str}" if yeedu_run_id_str else "Open Run"
            rows.append(_row(yeedu_label, _link(
                run_label, yeedu_url, new_tab=True)))

        # Cluster bump info
        if cluster_bump_info:
            rows.append(
                _row("Cluster Bump", cluster_bump_info, highlight=True))

        # --- Error section ---
        # Normalise error_summary: treat the sentinel string returned by _build_error_summary()
        # when no real information is available as empty, so the context['exception'] fallback
        # can fire for pre-job failures (auth, API connection, validation, etc.).
        _EMPTY_SENTINELS = {"No error details available",
                            "Failed to retrieve error details"}
        effective_error = error_summary if error_summary and error_summary.strip(
        ) not in _EMPTY_SENTINELS else None
        exc_obj = None
        if not effective_error and status_msg == "FAILED" and context:
            exc_obj = context.get("exception")
            if exc_obj:
                effective_error = "".join(
                    _traceback.format_exception(
                        type(exc_obj), exc_obj, exc_obj.__traceback__)
                )

        error_section = ""
        if status_msg == "FAILED" and effective_error:
            if exc_obj:
                # Smart extraction for Python exception objects from context['exception'].
                # Strategy:
                #   1. Walk __cause__/__context__ chain, preferring non-AirflowException types
                #      as the true root cause (the real trigger before Airflow re-wrapped it).
                #   2. If the entire chain is AirflowExceptions, use the deepest one's message.
                #   3. Only split into "Root Cause / Raised As" when they are meaningfully
                #      different (different types or clearly different messages).
                #   4. Last 5 frames rendered with explicit <br> tags — reliable across all
                #      email clients regardless of CSS white-space support.

                def _exc_type_str(e):
                    m = type(e).__module__
                    n = type(e).__name__
                    return f"{m}.{n}" if m and m != "builtins" else n

                # Walk chain: stop at the FIRST non-AirflowException as root cause.
                # That is the real trigger before Airflow re-wrapped it. Going deeper
                # (e.g. TimeoutError) loses the host/URL context that is actionable.
                # If the entire chain is AirflowExceptions, use the deepest one.
                root_exc = exc_obj
                deepest_exc = exc_obj
                found_non_airflow = False
                seen_ids = {id(exc_obj)}
                cur = exc_obj
                while True:
                    nxt = getattr(cur, '__cause__', None) or getattr(
                        cur, '__context__', None)
                    if nxt is None or id(nxt) in seen_ids:
                        break
                    seen_ids.add(id(nxt))
                    deepest_exc = nxt
                    if not found_non_airflow and type(nxt).__name__ != "AirflowException":
                        root_exc = nxt
                        found_non_airflow = True
                    cur = nxt
                # If we never found a non-AirflowException, use the deepest one
                if root_exc is exc_obj and deepest_exc is not exc_obj:
                    root_exc = deepest_exc

                final_type = _exc_type_str(exc_obj)
                final_msg = str(exc_obj)
                if len(final_msg) > 800:
                    final_msg = final_msg[:800] + "..."

                root_type = _exc_type_str(root_exc)
                root_msg = str(root_exc)
                if len(root_msg) > 800:
                    root_msg = root_msg[:800] + "..."

                # Only show a split view when root and final are different exception types.
                # Re-wrapping the same type (e.g. AirflowException inside AirflowException)
                # is not meaningful to surface — just show the deepest (most specific) message.
                show_split = (
                    root_exc is not exc_obj
                    and root_type != final_type
                )

                if show_split:
                    error_block = f"""
                        <div style='margin-bottom:4px;color:#888;font-size:11px;'>Root Cause</div>
                        <div style='margin-bottom:6px;font-weight:700;font-size:13px;'>{root_type}</div>
                        <div style='margin-bottom:14px;color:#25221E;font-size:13px;'>{root_msg}</div>
                        <div style='border-top:1px solid #e0e0e0;padding-top:8px;margin-bottom:4px;color:#aaa;font-size:11px;'>Raised As</div>
                        <div style='margin-bottom:4px;font-weight:600;color:#5F6368;'>{final_type}</div>
                        <div style='margin-bottom:6px;color:#5F6368;'>{final_msg}</div>
                    """
                else:
                    # Single exception or identical chain — type is always AirflowException
                    # here so it adds no value; show the message directly.
                    error_block = f"""
                        <div style='margin-bottom:6px;color:#25221E;font-size:13px;'>{root_msg}</div>
                    """

                error_section = f"""
                <div style="margin:15px 0 6px 0;">
                    <div style="background:#D85040;color:#fff;padding:8px 12px;font-size:13px;font-weight:600;border-radius:3px 3px 0 0;">Error</div>
                    <div style="background:#fafafa;border:1px solid #e0e0e0;border-top:none;padding:12px;font-family:monospace;font-size:12px;border-radius:0 0 3px 3px;">
                        {error_block}
                        <div style="margin-top:12px;border-top:1px solid #e0e0e0;padding-top:8px;
                                    font-family:sans-serif;font-size:11px;color:#888;font-style:italic;">
                            This is a truncated summary. For the full traceback and logs, check the
                            Airflow task logs for this run.
                        </div>
                    </div>
                </div>
                """
            else:
                # Operator-provided summary: plain text from job/notebook log analysis.
                # No reformatting — preserve content exactly. Horizontal scroll so long
                # lines don't wrap and the block stays readable. padding-bottom gives the
                # scrollbar clearance so it doesn't overlap the last line of text.
                error_display = effective_error
                if len(error_display) > 2000:
                    error_display = error_display[:2000] + \
                        "\n\n... (truncated, see Airflow logs for full details)"

                error_section = f"""
                <div style="margin:15px 0;">
                    <div style="background:#D85040;color:#fff;padding:8px 12px;font-size:13px;font-weight:600;border-radius:3px 3px 0 0;">Error</div>
                    <div style="background:#fafafa;border:1px solid #e0e0e0;border-top:none;padding:12px 12px 20px 12px;font-family:monospace;font-size:12px;overflow-x:auto;white-space:pre;border-radius:0 0 3px 3px;">{error_display}</div>
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
                <div style="padding: 10px 18px 10px 18px;">
                    {status_banner}

                    <h3 style="color: #25221E; margin: 15px 0 10px 0; font-size: 14px; font-weight: 600;">Run Information</h3>
                    <table style="width: 100%; border-collapse: collapse; font-size: 13px; border: 1px solid #e0e0e0;">
                        {''.join(rows)}
                    </table>

                    {error_section}
                </div>

                <!-- Footer -->
                <div style="background: #FFEFE6; padding: 8px 20px; text-align: center; margin-bottom: 18px;">
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
