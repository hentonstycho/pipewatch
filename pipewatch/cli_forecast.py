"""CLI sub-command: pipewatch forecast — show metric trend forecasts."""
from __future__ import annotations

import sys
from typing import Optional

import click

from pipewatch.config import PipewatchConfig
from pipewatch.forecaster import ForecastResult, forecast_all, forecast_pipeline
from pipewatch.metrics import compute_all_metrics, compute_metrics

_TREND_ICON = {
    "improving": "\u2197",   # ↗
    "degrading": "\u2198",   # ↘
    "stable": "\u2192",      # →
    "unknown": "?",
}


def _print_forecast(results: list[ForecastResult]) -> None:
    for r in results:
        icon = _TREND_ICON.get(r.trend, "?")
        current_s = f"{r.current:.4f}" if r.current is not None else "n/a"
        forecast_s = f"{r.forecasted:.4f}" if r.forecasted is not None else "n/a"
        delta_s = ""
        if r.delta is not None:
            sign = "+" if r.delta >= 0 else ""
            delta_s = f"  (delta {sign}{r.delta:.4f})"
        click.echo(
            f"  {r.metric:<28} current={current_s:<10} "
            f"forecast={forecast_s:<10} {icon} {r.trend}{delta_s}"
        )


@click.group("forecast")
def forecast_cmd() -> None:
    """Forecast future metric trends for pipelines."""


@forecast_cmd.command("run")
@click.option("--pipeline", default=None, help="Limit to a single pipeline.")
@click.option("--steps", default=1, show_default=True, help="Steps ahead to forecast.")
@click.option("--fail-on-degraded", is_flag=True, default=False,
              help="Exit with code 1 if any metric is degrading.")
@click.pass_context
def run_cmd(
    ctx: click.Context,
    pipeline: Optional[str],
    steps: int,
    fail_on_degraded: bool,
) -> None:
    """Print metric forecasts for one or all pipelines."""
    cfg: PipewatchConfig = ctx.obj

    if pipeline:
        names = [p.name for p in cfg.pipelines]
        if pipeline not in names:
            click.echo(f"Unknown pipeline: {pipeline}", err=True)
            sys.exit(2)
        pipe_cfg = next(p for p in cfg.pipelines if p.name == pipeline)
        metrics_list = [compute_metrics(pipe_cfg)]
    else:
        metrics_list = compute_all_metrics(cfg.pipelines)

    all_results = forecast_all(metrics_list, steps_ahead=steps)

    # Group by pipeline
    from itertools import groupby
    all_results.sort(key=lambda r: r.pipeline)
    for pipe_name, group in groupby(all_results, key=lambda r: r.pipeline):
        click.echo(f"Pipeline: {pipe_name}")
        _print_forecast(list(group))

    if fail_on_degraded and any(r.trend == "degrading" for r in all_results):
        sys.exit(1)
