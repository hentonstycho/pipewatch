"""cli_replay.py – CLI commands for replaying historical pipeline results."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click

from pipewatch.config import PipewatchConfig
from pipewatch.replayer import replay_all, replay_pipeline


def _parse_since(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        raise click.BadParameter(f"Cannot parse datetime: {value!r}")


@click.group(name="replay")
def replay_cmd() -> None:
    """Replay historical check results."""


@replay_cmd.command(name="run")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", "pipeline_name", default=None, help="Limit to one pipeline.")
@click.option("--since", default=None, help="ISO datetime lower bound (UTC).")
@click.option("--limit", default=None, type=int, help="Max events per pipeline.")
@click.option("--history-dir", default=".pipewatch/history", show_default=True)
def run_cmd(
    config_path: str,
    pipeline_name: Optional[str],
    since: Optional[str],
    limit: Optional[int],
    history_dir: str,
) -> None:
    """Print replayed events from history."""
    cfg = PipewatchConfig.load(Path(config_path))
    since_dt = _parse_since(since)
    hist = Path(history_dir)

    if pipeline_name is not None:
        matches = [p for p in cfg.pipelines if p.name == pipeline_name]
        if not matches:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            raise SystemExit(2)
        events = replay_pipeline(matches[0], hist, since=since_dt, limit=limit)
    else:
        events = replay_all(cfg.pipelines, hist, since=since_dt, limit=limit)

    if not events:
        click.echo("No events found.")
        return

    for ev in events:
        status = click.style("OK", fg="green") if ev.result.healthy else click.style("FAIL", fg="red")
        ts = ev.original_ts.isoformat()
        violations = ", ".join(ev.result.violations) if ev.result.violations else ""
        line = f"{ts}  {ev.pipeline:<30} {status}"
        if violations:
            line += f"  [{violations}]"
        click.echo(line)
