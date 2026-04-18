"""CLI commands for error budget reporting."""
from __future__ import annotations

import sys

import click

from pipewatch.budgeter import compute_all_budgets, compute_budget
from pipewatch.config import load_config


@click.group()
def budget_cmd():
    """Error budget commands."""


@budget_cmd.command("show")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", default=None, help="Filter to a single pipeline.")
@click.option("--window", default=None, type=int, help="Limit to last N runs.")
@click.option(
    "--fail-exhausted",
    is_flag=True,
    default=False,
    help="Exit 1 if any budget is exhausted.",
)
def show_cmd(config_path: str, pipeline: str | None, window: int | None, fail_exhausted: bool):
    """Show error budget status for pipelines."""
    cfg = load_config(config_path)

    if pipeline:
        names = [p.name for p in cfg.pipelines]
        if pipeline not in names:
            click.echo(f"Unknown pipeline: {pipeline}", err=True)
            sys.exit(2)
        slo = 0.95
        for p in cfg.pipelines:
            if p.name == pipeline:
                slo = getattr(p.thresholds, "slo_target", None) or 0.95
        results = [compute_budget(pipeline, slo, window=window)]
    else:
        results = compute_all_budgets(cfg, window=window)

    any_exhausted = False
    for r in results:
        icon = "\u274c" if r.exhausted else "\u2705"
        click.echo(f"{icon} {r}")
        if r.exhausted:
            any_exhausted = True

    if fail_exhausted and any_exhausted:
        sys.exit(1)
