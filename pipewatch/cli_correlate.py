"""CLI command: pipewatch correlate — show correlated pipeline failures."""
from __future__ import annotations

import click

from pipewatch.config import load_config
from pipewatch.correlator import correlate_failures


@click.group("correlate")
def correlate_cmd() -> None:
    """Identify pipelines that fail together."""


@correlate_cmd.command("run")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history-dir", default=".pipewatch/history", show_default=True)
@click.option("--window", default=5, show_default=True, help="Correlation window in minutes.")
@click.option("--min-size", default=2, show_default=True, help="Minimum group size to display.")
def run_cmd(
    config_path: str,
    history_dir: str,
    window: int,
    min_size: int,
) -> None:
    """Print correlated failure groups."""
    cfg = load_config(config_path)
    groups = correlate_failures(cfg, history_dir=history_dir, window_minutes=window)
    filtered = [g for g in groups if g.size >= min_size]

    if not filtered:
        click.echo("No correlated failures found.")
        return

    for idx, group in enumerate(filtered, start=1):
        click.echo(
            f"Group {idx} — {group.size} pipelines "
            f"[{group.window_start.isoformat()} … {group.window_end.isoformat()}]"
        )
        for pipe in group.pipelines:
            click.echo(f"  • {pipe}")
        click.echo()
