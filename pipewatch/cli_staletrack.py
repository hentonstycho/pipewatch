"""cli_staletrack.py – CLI surface for the stale-tracker feature."""
from __future__ import annotations

import sys
from pathlib import Path

import click

from pipewatch.config import load_config
from pipewatch.staletracker import track_all, track_pipeline


@click.group("staletrack")
def staletrack_cmd() -> None:
    """Detect pipelines whose results have gone stale."""


@staletrack_cmd.command("check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option(
    "--history-dir", default=".pipewatch/history", show_default=True,
    help="Directory containing per-pipeline .jsonl history files.",
)
@click.option(
    "--max-age", "max_age_minutes", default=60.0, show_default=True,
    help="Default max age in minutes before a pipeline is considered stale.",
)
@click.option(
    "--pipeline", "pipeline_name", default=None,
    help="Check a single pipeline by name.",
)
@click.option(
    "--fail-stale", is_flag=True, default=False,
    help="Exit with code 1 if any pipeline is stale.",
)
def check_cmd(
    config_path: str,
    history_dir: str,
    max_age_minutes: float,
    pipeline_name: str | None,
    fail_stale: bool,
) -> None:
    """Print staleness status for pipelines."""
    cfg = load_config(config_path)
    hdir = Path(history_dir)

    if pipeline_name is not None:
        names = [p.name for p in cfg.pipelines]
        if pipeline_name not in names:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            sys.exit(2)
        entries = [track_pipeline(pipeline_name, max_age_minutes, hdir)]
    else:
        entries = track_all(cfg, hdir, default_max_age_minutes=max_age_minutes)

    any_stale = False
    for entry in entries:
        icon = "\u26a0\ufe0f" if entry.is_stale else "\u2705"
        click.echo(f"{icon}  {entry}")
        if entry.is_stale:
            any_stale = True

    if fail_stale and any_stale:
        sys.exit(1)
