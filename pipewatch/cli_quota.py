"""CLI commands for inspecting pipeline alert quotas."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.config import load_config
from pipewatch.quota import get_quota, record_alert

_DEFAULT_LIMIT = 10
_DEFAULT_DIR = Path(".pipewatch/quotas")


@click.group("quota")
def quota_cmd():
    """Manage per-pipeline daily alert quotas."""


@quota_cmd.command("status")
@click.option("--config", "cfg_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", default=None, help="Filter to a single pipeline.")
@click.option("--limit", default=_DEFAULT_LIMIT, show_default=True, help="Daily alert limit.")
def status_cmd(cfg_path: str, pipeline: str | None, limit: int):
    """Show daily alert quota status for all (or one) pipeline."""
    cfg = load_config(Path(cfg_path))
    names = [pipeline] if pipeline else [p.name for p in cfg.pipelines]
    unknown = [n for n in names if n not in {p.name for p in cfg.pipelines}]
    if unknown:
        click.echo(f"Unknown pipeline(s): {', '.join(unknown)}", err=True)
        raise SystemExit(2)
    for name in names:
        result = get_quota(name, limit, _DEFAULT_DIR)
        marker = " [EXHAUSTED]" if result.exhausted else ""
        click.echo(f"{result.pipeline}: {result.count}/{result.limit}{marker}")


@quota_cmd.command("reset")
@click.argument("pipeline")
@click.option("--config", "cfg_path", default="pipewatch.yaml", show_default=True)
def reset_cmd(pipeline: str, cfg_path: str):
    """Reset the daily quota counter for a pipeline."""
    cfg = load_config(Path(cfg_path))
    names = {p.name for p in cfg.pipelines}
    if pipeline not in names:
        click.echo(f"Unknown pipeline: {pipeline}", err=True)
        raise SystemExit(2)
    path = _DEFAULT_DIR / f"{pipeline}.json"
    if path.exists():
        path.unlink()
    click.echo(f"Quota reset for {pipeline}.")
