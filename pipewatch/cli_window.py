"""cli_window.py – CLI commands for sliding-window failure analysis."""
from __future__ import annotations

import sys

import click

from pipewatch.config import load_config
from pipewatch.windower import analyse_all_windows, analyse_window


@click.group(name="window")
def window_cmd() -> None:
    """Sliding-window failure analysis."""


@window_cmd.command(name="show")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--hours", default=24, show_default=True, help="Window size in hours.")
@click.option("--pipeline", "pipeline_name", default=None, help="Limit to one pipeline.")
@click.option(
    "--fail-degraded",
    is_flag=True,
    default=False,
    help="Exit 1 if any pipeline has failures in the window.",
)
def show_cmd(
    config_path: str,
    hours: int,
    pipeline_name: str | None,
    fail_degraded: bool,
) -> None:
    """Print sliding-window failure stats for pipelines."""
    cfg = load_config(config_path)
    known = {p.name for p in cfg.pipelines}

    if pipeline_name is not None:
        if pipeline_name not in known:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            sys.exit(2)
        results = [analyse_window(pipeline_name, window_hours=hours)]
    else:
        results = analyse_all_windows(cfg, window_hours=hours)

    any_degraded = False
    for r in results:
        click.echo(str(r))
        if not r.healthy:
            any_degraded = True

    if fail_degraded and any_degraded:
        sys.exit(1)
