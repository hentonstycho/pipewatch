"""CLI command: pipewatch score — show per-pipeline health scores."""

from __future__ import annotations

import sys
from typing import Optional

import click

from pipewatch.config import load_config
from pipewatch.scorer import score_all, score_pipeline, PipelineScore


def _colour(grade: str) -> str:
    mapping = {"A": "green", "B": "cyan", "C": "yellow", "D": "yellow", "F": "red"}
    return mapping.get(grade, "white")


def _print_score(ps: PipelineScore) -> None:
    grade_str = click.style(f"[{ps.grade}]", fg=_colour(ps.grade), bold=True)
    click.echo(f"{grade_str}  {ps.pipeline:<30}  score={ps.score:>5.1f}")
    for reason in ps.reasons:
        click.echo(f"       • {reason}")


@click.group("score")
def score_cmd() -> None:
    """Health scoring commands."""


@score_cmd.command("show")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", "pipeline_name", default=None, help="Score a single pipeline.")
@click.option("--window", default=20, show_default=True, help="Number of recent checks to consider.")
@click.option("--fail-below", "fail_below", default=None, type=float,
              help="Exit 1 if any pipeline score is below this value.")
def show_cmd(
    config_path: str,
    pipeline_name: Optional[str],
    window: int,
    fail_below: Optional[float],
) -> None:
    """Display health scores for pipelines."""
    cfg = load_config(config_path)

    if pipeline_name:
        matches = [p for p in cfg.pipelines if p.name == pipeline_name]
        if not matches:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            sys.exit(2)
        scores = [score_pipeline(matches[0], window=window)]
    else:
        scores = score_all(cfg.pipelines, window=window)

    for ps in scores:
        _print_score(ps)

    if fail_below is not None and any(ps.score < fail_below for ps in scores):
        sys.exit(1)
