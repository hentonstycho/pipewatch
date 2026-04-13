"""Notification dispatchers for Slack and email alerts."""

import smtplib
import urllib.request
import json
import logging
from email.mime.text import MIMEText
from typing import List

from pipewatch.checker import CheckResult
from pipewatch.config import NotificationConfig

logger = logging.getLogger(__name__)


def _format_message(results: List[CheckResult]) -> str:
    """Format a list of CheckResults into a human-readable alert message."""
    lines = ["*Pipewatch Alert* — pipeline health issues detected:", ""]
    for r in results:
        status = "OK" if r.healthy else "FAIL"
        lines.append(f"  [{status}] {r.pipeline_name}")
        for violation in r.violations:
            lines.append(f"    • {violation}")
    return "\n".join(lines)


def send_slack(webhook_url: str, results: List[CheckResult]) -> bool:
    """Send a Slack notification via incoming webhook. Returns True on success."""
    message = _format_message(results)
    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                logger.info("Slack notification sent successfully.")
                return True
            logger.warning("Slack returned non-200 status: %s", resp.status)
            return False
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send Slack notification: %s", exc)
        return False


def send_email(
    cfg: NotificationConfig,
    results: List[CheckResult],
) -> bool:
    """Send an email alert. Returns True on success."""
    if not cfg.email_recipients:
        logger.warning("No email recipients configured; skipping email.")
        return False

    message = _format_message(results)
    msg = MIMEText(message)
    msg["Subject"] = "Pipewatch Alert: pipeline health issues detected"
    msg["From"] = cfg.email_from or "pipewatch@localhost"
    msg["To"] = ", ".join(cfg.email_recipients)

    smtp_host = cfg.smtp_host or "localhost"
    smtp_port = cfg.smtp_port or 25

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.sendmail(msg["From"], cfg.email_recipients, msg.as_string())
        logger.info("Email notification sent to %s.", cfg.email_recipients)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send email notification: %s", exc)
        return False


def dispatch_notifications(
    cfg: NotificationConfig,
    results: List[CheckResult],
) -> None:
    """Dispatch all configured notifications for failing pipelines."""
    failing = [r for r in results if not r.healthy]
    if not failing:
        logger.debug("All pipelines healthy; no notifications to send.")
        return

    if cfg.slack_webhook:
        send_slack(cfg.slack_webhook, failing)

    if cfg.email_recipients:
        send_email(cfg, failing)
