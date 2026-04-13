"""CLI sub-command: watchdog — report pipelines that have gone stale."""
from __future__ import annotations

import sys
from typing import Optional

import click

from pipewatch.config import load_config
from pipewatch.watchdog import check_all_staleness, StaleResult


def _fmt_age(seconds: Optional[float]) -> str:
    if seconds is None:
        return "never checked"
    if seconds < 120:
        return f"{seconds:.0f}s ago"
    if seconds < 7200:
        return f"{seconds / 60:.1f}m ago"
    return f"{seconds / 3600:.1f}h ago"


@click.group(name="watchdog")
def watchdog_cmd() -> None:
    """Commands for detecting stale pipelines."""


@watchdog_cmd.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history-dir", default=".pipewatch/history", show_default=True)
@click.option(
    "--threshold",
    default=3600.0,
    show_default=True,
    help="Default staleness threshold in seconds.",
)
@click.option(
    "--fail-on-stale",
    is_flag=True,
    default=False,
    help="Exit with code 1 if any pipeline is stale.",
)
def check_cmd(
    config_path: str,
    history_dir: str,
    threshold: float,
    fail_on_stale: bool,
) -> None:
    """Check all pipelines for staleness."""
    cfg = load_config(config_path)
    results = check_all_staleness(cfg, history_dir, default_threshold_seconds=threshold)

    any_stale = False
    for r in results:
        status = "STALE" if r.is_stale else "OK"
        age_str = _fmt_age(r.age_seconds)
        click.echo(f"[{status}] {r.pipeline}  last seen: {age_str}  (threshold: {r.threshold_seconds:.0f}s)")
        if r.is_stale:
            any_stale = True

    if fail_on_stale and any_stale:
        sys.exit(1)
