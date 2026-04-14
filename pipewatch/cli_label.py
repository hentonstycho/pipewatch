"""cli_label.py — CLI commands for displaying severity labels for pipelines."""
from __future__ import annotations

import sys

import click

from pipewatch.config import PipewatchConfig
from pipewatch.checker import check_all_pipelines
from pipewatch.labeler import label_all, SEVERITY_CRITICAL, SEVERITY_WARNING


_SEVERITY_COLOUR = {
    "critical": "red",
    "warning": "yellow",
    "info": "cyan",
    "ok": "green",
}


@click.group(name="label")
def label_cmd() -> None:
    """Severity labelling commands."""


@label_cmd.command(name="show")
@click.option("--pipeline", default=None, help="Filter to a single pipeline.")
@click.option(
    "--history-dir",
    default=".pipewatch/history",
    show_default=True,
    help="Path to history directory.",
)
@click.option(
    "--warning-after",
    default=2,
    show_default=True,
    type=int,
    help="Consecutive failures before WARNING.",
)
@click.option(
    "--critical-after",
    default=5,
    show_default=True,
    type=int,
    help="Consecutive failures before CRITICAL.",
)
@click.option(
    "--fail-on-critical",
    is_flag=True,
    default=False,
    help="Exit with code 1 if any pipeline is CRITICAL.",
)
@click.pass_obj
def show_cmd(
    cfg: PipewatchConfig,
    pipeline: str | None,
    history_dir: str,
    warning_after: int,
    critical_after: int,
    fail_on_critical: bool,
) -> None:
    """Show severity labels for all (or one) pipeline(s)."""
    pipelines = cfg.pipelines
    if pipeline:
        pipelines = [p for p in pipelines if p.name == pipeline]
        if not pipelines:
            click.echo(f"Unknown pipeline: {pipeline}", err=True)
            sys.exit(2)

    results = check_all_pipelines(pipelines)
    labeled = label_all(results, history_dir, warning_after, critical_after)

    for lr in labeled:
        colour = _SEVERITY_COLOUR.get(lr.severity, "white")
        tag = click.style(lr.severity.upper(), fg=colour, bold=True)
        line = f"{lr.result.pipeline:<30} {tag}"
        if lr.reason:
            line += f"  — {lr.reason}"
        click.echo(line)

    if fail_on_critical and any(lr.severity == SEVERITY_CRITICAL for lr in labeled):
        sys.exit(1)
