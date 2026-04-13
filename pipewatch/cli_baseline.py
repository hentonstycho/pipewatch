"""CLI commands for baseline capture and comparison."""
from __future__ import annotations

import sys

import click

from pipewatch.baseline import diff_baseline, list_baselines, save_baseline
from pipewatch.config import PipewatchConfig
from pipewatch.metrics import compute_all_metrics


def _fmt_delta(value: float | None) -> str:
    if value is None:
        return "n/a"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.4f}"


@click.group(name="baseline")
def baseline_cmd() -> None:
    """Capture and compare pipeline metric baselines."""


@baseline_cmd.command(name="capture")
@click.argument("name")
@click.pass_obj
def capture_cmd(config: PipewatchConfig, name: str) -> None:
    """Capture current metrics as baseline NAME."""
    metrics = compute_all_metrics(config)
    path = save_baseline(name, metrics)
    click.echo(f"Baseline '{name}' saved to {path}")


@baseline_cmd.command(name="list")
@click.pass_obj
def list_cmd(config: PipewatchConfig) -> None:  # noqa: ARG001
    """List all saved baselines."""
    names = list_baselines()
    if not names:
        click.echo("No baselines saved yet.")
        return
    for n in names:
        click.echo(n)


@baseline_cmd.command(name="diff")
@click.argument("name")
@click.option(
    "--pipeline",
    default=None,
    help="Restrict diff to a single pipeline.",
)
@click.option(
    "--fail-on-regression",
    is_flag=True,
    default=False,
    help="Exit 1 if any metric regressed.",
)
@click.pass_obj
def diff_cmd(
    config: PipewatchConfig,
    name: str,
    pipeline: str | None,
    fail_on_regression: bool,
) -> None:
    """Show metric deltas between current metrics and baseline NAME."""
    if pipeline and pipeline not in {p.name for p in config.pipelines}:
        click.echo(f"Unknown pipeline: {pipeline}", err=True)
        sys.exit(2)

    current = compute_all_metrics(config)
    if pipeline:
        current = {k: v for k, v in current.items() if k == pipeline}

    try:
        deltas = diff_baseline(name, current)
    except FileNotFoundError as exc:
        click.echo(str(exc), err=True)
        sys.exit(2)

    regressed = False
    for pipe_name, fields in deltas.items():
        click.echo(f"[{pipe_name}]")
        for field, delta in fields.items():
            marker = ""
            if delta is not None and delta > 0 and field != "avg_row_count":
                marker = "  ⚠ regression"
                regressed = True
            click.echo(f"  {field}: {_fmt_delta(delta)}{marker}")

    if fail_on_regression and regressed:
        sys.exit(1)
