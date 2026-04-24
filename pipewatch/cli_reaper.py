"""cli_reaper.py – CLI surface for the reaper feature."""
from __future__ import annotations

import sys

import click

from pipewatch.config import load_config
from pipewatch.reaper import reap_all, reap_pipeline


@click.group("reaper")
def reaper_cmd() -> None:
    """Detect pipelines that have stopped producing results."""


@reaper_cmd.command("check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", "pipeline_name", default=None, help="Check a single pipeline.")
@click.option(
    "--threshold-hours",
    default=24.0,
    show_default=True,
    help="Hours without results before a pipeline is considered dead.",
)
@click.option(
    "--fail-on-dead",
    is_flag=True,
    default=False,
    help="Exit with code 1 if any dead pipelines are found.",
)
def check_cmd(
    config_path: str,
    pipeline_name: str | None,
    threshold_hours: float,
    fail_on_dead: bool,
) -> None:
    """Report dead (stalled) pipelines."""
    cfg = load_config(config_path)

    if pipeline_name is not None:
        names = [p.name for p in cfg.pipelines]
        if pipeline_name not in names:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            sys.exit(2)
        results = [
            reap_pipeline(
                pipeline_name,
                history_dir=cfg.history_dir,
                threshold_hours=threshold_hours,
            )
        ]
    else:
        results = reap_all(cfg, threshold_hours=threshold_hours)

    any_dead = False
    for r in results:
        icon = "💀" if r.dead else "✅"
        if r.age_hours is None:
            age_str = "never checked"
        else:
            age_str = f"{r.age_hours:.1f}h ago"
        click.echo(f"{icon}  {r.pipeline:<30} last seen: {age_str}")
        if r.dead:
            any_dead = True

    if fail_on_dead and any_dead:
        sys.exit(1)
