"""CLI sub-command: pipewatch heatmap"""
from __future__ import annotations

import sys

import click

from pipewatch.config import load_config
from pipewatch.heatmap import build_heatmap, format_heatmap


@click.group(name="heatmap")
def heatmap_cmd() -> None:
    """Failure heatmap commands."""


@heatmap_cmd.command(name="show")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history-dir", default=".pipewatch/history", show_default=True)
@click.option("--pipeline", "pipeline_name", default=None, help="Limit to one pipeline.")
@click.option("--fail-if-peak", type=int, default=None,
              help="Exit 1 if any pipeline has >= N failures in its peak hour.")
def show_cmd(
    config_path: str,
    history_dir: str,
    pipeline_name: str | None,
    fail_if_peak: int | None,
) -> None:
    """Print an hourly failure heatmap for all (or one) pipeline(s)."""
    cfg = load_config(config_path)

    if pipeline_name:
        names = [p.name for p in cfg.pipelines]
        if pipeline_name not in names:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            sys.exit(2)

    rows = build_heatmap(cfg, history_dir=history_dir)

    if pipeline_name:
        rows = [r for r in rows if r.pipeline == pipeline_name]

    click.echo(format_heatmap(rows))

    if fail_if_peak is not None:
        for row in rows:
            if max(row.buckets) >= fail_if_peak:
                sys.exit(1)
