"""CLI commands for managing pipeline alert silences."""

from __future__ import annotations

from pathlib import Path

import click

from pipewatch.silencer import (
    _SILENCE_FILE,
    get_expiry,
    is_silenced,
    silence_pipeline,
    unsilence_pipeline,
)


@click.group("silence")
def silence_cmd() -> None:
    """Manage alert silences for pipelines."""


@silence_cmd.command("add")
@click.argument("pipeline")
@click.option(
    "--minutes",
    "-m",
    default=60,
    show_default=True,
    help="Duration to silence alerts (minutes).",
)
@click.option(
    "--silence-file",
    default=str(_SILENCE_FILE),
    hidden=True,
    help="Path to silence state file.",
)
def add_cmd(pipeline: str, minutes: int, silence_file: str) -> None:
    """Silence alerts for PIPELINE for the given number of minutes."""
    path = Path(silence_file)
    expiry = silence_pipeline(pipeline, minutes, silence_path=path)
    click.echo(
        f"Silenced '{pipeline}' until {expiry.strftime('%Y-%m-%d %H:%M:%S UTC')}."
    )


@silence_cmd.command("remove")
@click.argument("pipeline")
@click.option("--silence-file", default=str(_SILENCE_FILE), hidden=True)
def remove_cmd(pipeline: str, silence_file: str) -> None:
    """Remove silence for PIPELINE."""
    path = Path(silence_file)
    removed = unsilence_pipeline(pipeline, silence_path=path)
    if removed:
        click.echo(f"Silence removed for '{pipeline}'.")
    else:
        click.echo(f"No active silence found for '{pipeline}'.")
        raise SystemExit(1)


@silence_cmd.command("status")
@click.argument("pipeline")
@click.option("--silence-file", default=str(_SILENCE_FILE), hidden=True)
def status_cmd(pipeline: str, silence_file: str) -> None:
    """Show silence status for PIPELINE."""
    path = Path(silence_file)
    if is_silenced(pipeline, silence_path=path):
        expiry = get_expiry(pipeline, silence_path=path)
        click.echo(
            f"'{pipeline}' is SILENCED until "
            f"{expiry.strftime('%Y-%m-%d %H:%M:%S UTC') if expiry else 'unknown'}."
        )
    else:
        click.echo(f"'{pipeline}' is NOT silenced.")
        raise SystemExit(1)
