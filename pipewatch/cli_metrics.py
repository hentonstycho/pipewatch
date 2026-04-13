"""CLI sub-command: pipewatch metrics — display per-pipeline health metrics."""
from __future__ import annotations

import sys
from typing import Optional

import click

from pipewatch.config import load_config
from pipewatch.metrics import compute_all_metrics, PipelineMetrics


def _fmt_opt(value, fmt=".2f") -> str:
    return format(value, fmt) if value is not None else "n/a"


def _print_metrics(m: PipelineMetrics) -> None:
    click.echo(f"Pipeline : {m.pipeline_name}")
    click.echo(f"  Checks          : {m.total_checks}")
    click.echo(f"  Failures        : {m.total_failures}")
    click.echo(f"  Uptime          : {m.uptime_pct:.2f}%")
    click.echo(f"  Avg row count   : {_fmt_opt(m.avg_row_count, '.0f')}")
    click.echo(f"  Avg error rate  : {_fmt_opt(m.avg_error_rate)}")
    click.echo(f"  Avg latency (s) : {_fmt_opt(m.avg_latency_seconds)}")


@click.command("metrics")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True,
              help="Path to pipewatch config file.")
@click.option("--pipeline", "pipeline_name", default=None,
              help="Show metrics for a single pipeline only.")
@click.option("--history-dir", default=".pipewatch", show_default=True,
              help="Directory where history files are stored.")
@click.option("--fail-below", "fail_below", default=None, type=float,
              help="Exit with code 1 if any pipeline uptime is below this percentage.")
def metrics_cmd(
    config_path: str,
    pipeline_name: Optional[str],
    history_dir: str,
    fail_below: Optional[float],
) -> None:
    """Display aggregated health metrics for monitored pipelines."""
    cfg = load_config(config_path)
    names = [p.name for p in cfg.pipelines]

    if pipeline_name:
        if pipeline_name not in names:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            sys.exit(2)
        names = [pipeline_name]

    all_metrics = compute_all_metrics(names, history_dir=history_dir)

    for m in all_metrics:
        _print_metrics(m)
        click.echo()

    if fail_below is not None:
        degraded = [m for m in all_metrics if m.uptime_pct < fail_below]
        if degraded:
            click.echo(
                f"DEGRADED: {len(degraded)} pipeline(s) below {fail_below}% uptime.",
                err=True,
            )
            sys.exit(1)
