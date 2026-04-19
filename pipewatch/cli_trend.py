"""CLI command: pipewatch trend show"""
from __future__ import annotations

import sys
import click

from pipewatch.config import load_config
from pipewatch.trend import analyse_all_trends, analyse_trend


@click.group()
def trend_cmd():
    """Analyse metric trends for pipelines."""


@trend_cmd.command("show")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", default=None, help="Limit to one pipeline.")
@click.option("--window", default=20, show_default=True, help="Number of recent runs to analyse.")
@click.option("--history-dir", default=".pipewatch/history", show_default=True)
@click.option("--fail-on-degrading", is_flag=True, default=False)
def show_cmd(config_path, pipeline, window, history_dir, fail_on_degrading):
    """Show trend direction for each pipeline."""
    cfg = load_config(config_path)

    if pipeline:
        names = [p.name for p in cfg.pipelines]
        if pipeline not in names:
            click.echo(f"Unknown pipeline: {pipeline}", err=True)
            sys.exit(2)
        results = [analyse_trend(pipeline, history_dir=history_dir, window=window)]
    else:
        results = analyse_all_trends(cfg, history_dir=history_dir, window=window)

    any_degrading = False
    for r in results:
        click.echo(str(r))
        if r.direction == "degrading":
            any_degrading = True

    if fail_on_degrading and any_degrading:
        sys.exit(1)
