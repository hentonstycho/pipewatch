"""CLI command: export pipeline metrics in Prometheus format."""
from __future__ import annotations

import sys

import click

from pipewatch.config import PipewatchConfig
from pipewatch.metrics import compute_all_metrics
from pipewatch.exporter import render_metrics


@click.command("export")
@click.option("--pipeline", "pipeline_name", default=None,
              help="Export metrics for a single pipeline only.")
@click.option("--output", "output_path", default="-",
              type=click.Path(writable=True, allow_dash=True),
              help="Write output to a file instead of stdout.")
@click.pass_context
def export_cmd(ctx: click.Context, pipeline_name: str | None, output_path: str) -> None:
    """Export pipeline health metrics in Prometheus exposition format."""
    cfg: PipewatchConfig = ctx.obj["config"]

    if pipeline_name is not None:
        matched = [p for p in cfg.pipelines if p.name == pipeline_name]
        if not matched:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            sys.exit(2)
        pipelines = matched
    else:
        pipelines = cfg.pipelines

    all_metrics = compute_all_metrics(pipelines)
    output = render_metrics(all_metrics)

    if output_path == "-":
        click.echo(output)
    else:
        with open(output_path, "w") as fh:
            fh.write(output + "\n")
        click.echo(f"Metrics written to {output_path}")
