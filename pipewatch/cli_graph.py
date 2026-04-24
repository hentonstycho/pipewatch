"""CLI command for displaying ASCII sparkline graphs of pipeline health."""
from __future__ import annotations

import sys
import click

from pipewatch.config import load_config
from pipewatch.grapher import build_graph, build_all_graphs


@click.group(name="graph")
def graph_cmd() -> None:
    """Display ASCII sparkline graphs of pipeline health."""


@graph_cmd.command(name="show")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history-dir", default=".pipewatch/history", show_default=True)
@click.option("--window", default=30, show_default=True, help="Number of recent checks to include.")
@click.option("--pipeline", "pipeline_name", default=None, help="Limit to a single pipeline.")
def show_cmd(
    config_path: str,
    history_dir: str,
    window: int,
    pipeline_name: str | None,
) -> None:
    """Print sparkline health graphs for pipelines."""
    cfg = load_config(config_path)

    if pipeline_name is not None:
        names = [p.name for p in cfg.pipelines]
        if pipeline_name not in names:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            sys.exit(2)
        result = build_graph(pipeline_name, history_dir, window)
        if result is None:
            click.echo(f"{pipeline_name}: no history available")
        else:
            click.echo(str(result))
        return

    results = build_all_graphs(cfg, history_dir, window)
    if not results:
        click.echo("No history available for any pipeline.")
        return

    for r in results:
        click.echo(str(r))
