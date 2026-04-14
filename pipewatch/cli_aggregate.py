"""CLI command: pipewatch aggregate — show rollup statistics across all pipelines."""
from __future__ import annotations

import sys

import click

from pipewatch.aggregator import aggregate
from pipewatch.config import load_config


@click.group(name="aggregate")
def aggregate_cmd():
    """Rollup statistics across all monitored pipelines."""


@aggregate_cmd.command(name="summary")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history-dir", default=".pipewatch/history", show_default=True)
@click.option(
    "--fail-below",
    type=float,
    default=None,
    help="Exit 1 if healthy-pipeline ratio is below this fraction (0‑1).",
)
def summary_cmd(config_path: str, history_dir: str, fail_below: float | None):
    """Print a rollup health summary for all pipelines."""
    cfg = load_config(config_path)
    stats = aggregate(cfg, history_dir=history_dir)

    healthy_ratio = (
        stats.healthy_pipelines / stats.total_pipelines
        if stats.total_pipelines
        else 1.0
    )

    click.echo(f"Pipelines      : {stats.total_pipelines}")
    click.echo(f"Healthy        : {stats.healthy_pipelines}")
    click.echo(f"Degraded       : {stats.degraded_pipelines}")
    click.echo(f"Total checks   : {stats.total_checks}")
    click.echo(f"Total failures : {stats.total_failures}")
    click.echo(f"Failure rate   : {stats.failure_rate:.1%}")
    if stats.most_failing:
        click.echo(f"Most failing   : {stats.most_failing}")
    else:
        click.echo("Most failing   : —")

    if fail_below is not None and healthy_ratio < fail_below:
        click.echo(
            f"[WARN] Healthy ratio {healthy_ratio:.1%} is below threshold {fail_below:.1%}",
            err=True,
        )
        sys.exit(1)
