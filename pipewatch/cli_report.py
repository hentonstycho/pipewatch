"""CLI sub-command: `pipewatch report` — print a health summary."""

from __future__ import annotations

import sys
from typing import Optional

import click

from pipewatch.config import load_config
from pipewatch.reporter import build_report, format_report


@click.command(name="report")
@click.option(
    "--config",
    "config_path",
    default="pipewatch.yaml",
    show_default=True,
    help="Path to the pipewatch config file.",
)
@click.option(
    "--history-dir",
    default=".pipewatch",
    show_default=True,
    help="Directory where history JSON lines are stored.",
)
@click.option(
    "--pipeline",
    "pipeline_filter",
    default=None,
    help="Limit report to a single pipeline name.",
)
@click.option(
    "--fail-on-degraded",
    is_flag=True,
    default=False,
    help="Exit with code 1 when overall health is degraded.",
)
def report_cmd(
    config_path: str,
    history_dir: str,
    pipeline_filter: Optional[str],
    fail_on_degraded: bool,
) -> None:
    """Display a health summary for all monitored pipelines."""
    try:
        cfg = load_config(config_path)
    except FileNotFoundError:
        click.echo(f"Config file not found: {config_path}", err=True)
        sys.exit(2)

    names = [p.name for p in cfg.pipelines]
    if pipeline_filter:
        if pipeline_filter not in names:
            click.echo(f"Unknown pipeline: {pipeline_filter}", err=True)
            sys.exit(2)
        names = [pipeline_filter]

    report = build_report(names, history_dir=history_dir)
    click.echo(format_report(report))

    if fail_on_degraded and report.overall_health == "degraded":
        sys.exit(1)
