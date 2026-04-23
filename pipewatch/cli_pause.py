"""cli_pause.py – CLI commands for pausing / resuming pipelines."""
from __future__ import annotations

import sys
from pathlib import Path

import click

from pipewatch.pauser import is_paused, list_pauses, pause_pipeline, unpause_pipeline

_DATA_DIR = Path(".pipewatch")


@click.group("pause")
def pause_cmd() -> None:
    """Pause or resume pipeline checks."""


@pause_cmd.command("add")
@click.argument("pipeline")
@click.option(
    "--minutes",
    "-m",
    default=None,
    type=int,
    help="Duration in minutes (omit for indefinite).",
)
def add_cmd(pipeline: str, minutes: int | None) -> None:
    """Pause PIPELINE (optionally for MINUTES minutes)."""
    expiry = pause_pipeline(pipeline, minutes=minutes, data_dir=_DATA_DIR)
    if expiry:
        click.echo(f"Paused '{pipeline}' until {expiry}")
    else:
        click.echo(f"Paused '{pipeline}' indefinitely")


@pause_cmd.command("remove")
@click.argument("pipeline")
def remove_cmd(pipeline: str) -> None:
    """Resume (un-pause) PIPELINE."""
    removed = unpause_pipeline(pipeline, data_dir=_DATA_DIR)
    if removed:
        click.echo(f"Resumed '{pipeline}'")
    else:
        click.echo(f"'{pipeline}' was not paused", err=True)
        sys.exit(1)


@pause_cmd.command("status")
@click.argument("pipeline", required=False)
def status_cmd(pipeline: str | None) -> None:
    """Show pause status for PIPELINE (or all pipelines)."""
    if pipeline:
        paused = is_paused(pipeline, data_dir=_DATA_DIR)
        state_str = "paused" if paused else "active"
        click.echo(f"{pipeline}: {state_str}")
        return

    pauses = list_pauses(data_dir=_DATA_DIR)
    if not pauses:
        click.echo("No pipelines are currently paused.")
        return
    for name, expiry in pauses.items():
        expiry_str = expiry if expiry else "indefinite"
        click.echo(f"{name}: paused (expires: {expiry_str})")
