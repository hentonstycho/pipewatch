"""CLI commands for managing pipeline notification suppression."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.suppressor import (
    suppress,
    unsuppress,
    is_suppressed,
    list_suppressions,
    SUPPRESSOR_DIR,
)


@click.group("suppress")
def suppress_cmd() -> None:
    """Manage pipeline notification suppression."""


@suppress_cmd.command("add")
@click.argument("pipeline")
@click.option("--reason", default="manually suppressed", show_default=True)
@click.option("--dir", "base_dir", default=str(SUPPRESSOR_DIR), show_default=True)
def add_cmd(pipeline: str, reason: str, base_dir: str) -> None:
    """Suppress notifications for PIPELINE."""
    suppress(pipeline, reason, base_dir=Path(base_dir))
    click.echo(f"Suppressed '{pipeline}': {reason}")


@suppress_cmd.command("remove")
@click.argument("pipeline")
@click.option("--dir", "base_dir", default=str(SUPPRESSOR_DIR), show_default=True)
def remove_cmd(pipeline: str, base_dir: str) -> None:
    """Remove suppression for PIPELINE."""
    removed = unsuppress(pipeline, base_dir=Path(base_dir))
    if removed:
        click.echo(f"Suppression removed for '{pipeline}'.")
    else:
        click.echo(f"No suppression found for '{pipeline}'.")
        raise SystemExit(1)


@suppress_cmd.command("status")
@click.option("--dir", "base_dir", default=str(SUPPRESSOR_DIR), show_default=True)
def status_cmd(base_dir: str) -> None:
    """List all suppressed pipelines."""
    entries = list_suppressions(base_dir=Path(base_dir))
    if not entries:
        click.echo("No pipelines are currently suppressed.")
        return
    for e in entries:
        click.echo(f"  {e['pipeline']:30s}  {e['reason']}  (since {e['suppressed_at']})")
