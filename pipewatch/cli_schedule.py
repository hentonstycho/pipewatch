"""CLI sub-command: ``pipewatch schedule`` — run checks on a recurring interval."""

from __future__ import annotations

import logging

import click

from pipewatch.checker import check_all_pipelines
from pipewatch.config import load_config
from pipewatch.history import record_result
from pipewatch.notifier import dispatch_notifications
from pipewatch.scheduler import run_scheduler

log = logging.getLogger(__name__)


@click.command("schedule")
@click.option(
    "--interval",
    default="5m",
    show_default=True,
    help="How often to run checks, e.g. 30s, 5m, 1h.",
)
@click.option(
    "--config",
    "config_path",
    default="pipewatch.yaml",
    show_default=True,
    help="Path to pipewatch YAML config.",
)
@click.option(
    "--notify/--no-notify",
    default=True,
    show_default=True,
    help="Send notifications for failing checks.",
)
@click.option(
    "--stop-on-error",
    is_flag=True,
    default=False,
    help="Stop the scheduler if a check raises an unhandled exception.",
)
def schedule_cmd(
    interval: str,
    config_path: str,
    notify: bool,
    stop_on_error: bool,
) -> None:
    """Run pipeline checks repeatedly on a fixed interval."""
    cfg = load_config(config_path)
    click.echo(f"Scheduler starting — interval={interval}, config={config_path}")

    def _tick() -> None:
        results = check_all_pipelines(cfg)
        for result in results:
            record_result(result)
            if not result.healthy:
                log.warning("Pipeline unhealthy: %s", result.pipeline_name)
        if notify:
            dispatch_notifications(results, cfg.notifications)
        healthy_count = sum(1 for r in results if r.healthy)
        click.echo(
            f"[{interval}] checked {len(results)} pipeline(s) — "
            f"{healthy_count} healthy, {len(results) - healthy_count} failing"
        )

    run_scheduler(interval, _tick, stop_on_error=stop_on_error)
