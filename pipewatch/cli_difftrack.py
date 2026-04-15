"""CLI commands for comparing metric snapshots and reporting regressions."""
from __future__ import annotations

import click

from pipewatch.config import PipewatchConfig, load_config
from pipewatch.baseline import load_baseline, list_baselines
from pipewatch.metrics import compute_all_metrics
from pipewatch.difftracker import diff_metrics, any_regressions


@click.group("difftrack")
def difftrack_cmd() -> None:
    """Compare current metrics against a saved baseline for regressions."""


@difftrack_cmd.command("run")
@click.option("--config", default="pipewatch.yaml", show_default=True)
@click.option("--baseline-label", default=None, help="Baseline label to compare against.")
@click.option("--pipeline", default=None, help="Limit to a single pipeline.")
@click.option("--threshold", default=10.0, show_default=True, help="Regression threshold %.")
@click.option("--fail-on-regression", is_flag=True, default=False)
def run_cmd(
    config: str,
    baseline_label: str | None,
    pipeline: str | None,
    threshold: float,
    fail_on_regression: bool,
) -> None:
    """Run a diff between current metrics and a stored baseline."""
    cfg: PipewatchConfig = load_config(config)

    pipelines = (
        [p for p in cfg.pipelines if p.name == pipeline]
        if pipeline
        else cfg.pipelines
    )

    if pipeline and not pipelines:
        click.echo(f"Unknown pipeline: {pipeline}", err=True)
        raise SystemExit(2)

    found_regression = False

    for pipe_cfg in pipelines:
        baseline = load_baseline(pipe_cfg.name, label=baseline_label)
        if baseline is None:
            click.echo(f"  {pipe_cfg.name}: no baseline found — skipping")
            continue

        current_map = compute_all_metrics(cfg)
        current = current_map.get(pipe_cfg.name)
        if current is None:
            click.echo(f"  {pipe_cfg.name}: no current metrics — skipping")
            continue

        diffs = diff_metrics(baseline, current, regression_threshold_pct=threshold)
        click.echo(f"\n{pipe_cfg.name}:")
        for d in diffs:
            marker = "[REGRESSION]" if d.regressed else "[ok]      "
            click.echo(f"  {marker} {d}")

        if any_regressions(diffs):
            found_regression = True

    if fail_on_regression and found_regression:
        raise SystemExit(1)
