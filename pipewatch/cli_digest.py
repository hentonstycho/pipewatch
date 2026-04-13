"""CLI command: pipewatch digest — print a human-readable pipeline digest."""

from __future__ import annotations

import sys
import click

from pipewatch.config import load_config
from pipewatch.digest import build_digest
from pipewatch.notifier import dispatch_notifications
from pipewatch.checker import CheckResult


@click.command("digest")
@click.option(
    "--config", "config_path", default="pipewatch.yaml", show_default=True,
    help="Path to pipewatch config file.",
)
@click.option(
    "--history-dir", default=".pipewatch_history", show_default=True,
    help="Directory where history files are stored.",
)
@click.option(
    "--notify", is_flag=True, default=False,
    help="Send digest via configured Slack/email notifications.",
)
@click.option(
    "--fail-degraded", is_flag=True, default=False,
    help="Exit with code 1 if overall health is degraded.",
)
def digest_cmd(
    config_path: str,
    history_dir: str,
    notify: bool,
    fail_degraded: bool,
) -> None:
    """Print a periodic digest of all pipeline metrics."""
    try:
        config = load_config(config_path)
    except FileNotFoundError:
        click.echo(f"Config file not found: {config_path}", err=True)
        sys.exit(2)

    digest = build_digest(config, history_dir)
    click.echo(digest.to_text())

    if notify:
        # Build a synthetic CheckResult to reuse dispatch_notifications
        message = digest.to_text()
        _send_digest_notification(config, message)

    if fail_degraded and not digest.overall_healthy:
        sys.exit(1)


def _send_digest_notification(config, message: str) -> None:
    """Send the digest text via configured notifiers."""
    from pipewatch.notifier import send_slack, send_email

    nc = config.notifications
    if nc is None:
        return
    if nc.slack_webhook:
        send_slack(nc.slack_webhook, message)
    if nc.email_to:
        send_email(
            smtp_host=nc.smtp_host or "localhost",
            smtp_port=nc.smtp_port or 25,
            from_addr=nc.email_from or "pipewatch@localhost",
            to_addrs=nc.email_to,
            subject="Pipewatch Digest",
            body=message,
        )
