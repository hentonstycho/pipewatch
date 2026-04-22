"""CLI commands for viewing pipeline event logs."""
from __future__ import annotations

import sys
import click

from pipewatch.config import load_config
from pipewatch.eventlog import load_events, event_summary

EVENT_DIR = ".pipewatch/events"


@click.group(name="events")
def eventlog_cmd() -> None:
    """Inspect pipeline event logs."""


@eventlog_cmd.command(name="show")
@click.argument("pipeline")
@click.option("--config", default="pipewatch.yaml", show_default=True)
@click.option("--type", "event_type", default=None, help="Filter by event type.")
@click.option("--last", default=20, show_default=True, help="Number of recent events.")
def show_cmd(pipeline: str, config: str, event_type: str | None, last: int) -> None:
    """Show recent events for a pipeline."""
    cfg = load_config(config)
    known = {p.name for p in cfg.pipelines}
    if pipeline not in known:
        click.echo(f"Unknown pipeline: {pipeline}", err=True)
        sys.exit(2)

    events = load_events(pipeline, base_dir=EVENT_DIR, event_type=event_type)
    if not events:
        click.echo("No events recorded.")
        return

    for e in events[-last:]:
        meta = f" | {e.metadata}" if e.metadata else ""
        click.echo(f"[{e.timestamp}] {e.event_type.upper():10s} {e.message}{meta}")


@eventlog_cmd.command(name="summary")
@click.argument("pipeline")
@click.option("--config", default="pipewatch.yaml", show_default=True)
def summary_cmd(pipeline: str, config: str) -> None:
    """Show event type counts for a pipeline."""
    cfg = load_config(config)
    known = {p.name for p in cfg.pipelines}
    if pipeline not in known:
        click.echo(f"Unknown pipeline: {pipeline}", err=True)
        sys.exit(2)

    events = load_events(pipeline, base_dir=EVENT_DIR)
    counts = event_summary(events)
    if not counts:
        click.echo("No events recorded.")
        return

    click.echo(f"Event summary for '{pipeline}':")
    for etype, count in sorted(counts.items()):
        click.echo(f"  {etype:15s} {count}")
