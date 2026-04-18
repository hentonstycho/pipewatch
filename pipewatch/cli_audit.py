"""CLI commands for inspecting the pipewatch audit log."""
from __future__ import annotations

import json
import sys

import click

from pipewatch.auditor import audit_summary, load_audit_log
from pipewatch.config import load_config


@click.group(name="audit")
def audit_cmd() -> None:
    """Inspect the pipewatch audit log."""


@audit_cmd.command(name="log")
@click.option("--pipeline", default=None, help="Filter by pipeline name.")
@click.option("--limit", default=20, show_default=True, help="Max entries to show.")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--failed-only", is_flag=True, default=False, help="Show only failed checks.")
def log_cmd(pipeline: str | None, limit: int, config_path: str, failed_only: bool) -> None:
    """Print recent audit log entries."""
    cfg = load_config(config_path)
    entries = load_audit_log(cfg.data_dir)
    if pipeline:
        known = {p.name for p in cfg.pipelines}
        if pipeline not in known:
            click.echo(f"Unknown pipeline: {pipeline}", err=True)
            sys.exit(2)
        entries = [e for e in entries if e["pipeline"] == pipeline]
    if failed_only:
        entries = [e for e in entries if not e["healthy"]]
    for entry in entries[-limit:]:
        status = "OK" if entry["healthy"] else "FAIL"
        viols = ", ".join(entry.get("violations") or []) or "-"
        click.echo(
            f"{entry['timestamp']}  [{status}]  {entry['pipeline']}  {viols}"
        )


@audit_cmd.command(name="summary")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, help="Output JSON.")
def summary_cmd(config_path: str, as_json: bool) -> None:
    """Print a summary of all audit log entries."""
    cfg = load_config(config_path)
    summary = audit_summary(cfg.data_dir)
    if as_json:
        click.echo(json.dumps(summary, indent=2))
    else:
        click.echo(f"Total checks  : {summary['total_checks']}")
        click.echo(f"Healthy       : {summary['healthy_checks']}")
        click.echo(f"Failed        : {summary['failed_checks']}")
        click.echo(f"Pipelines     : {', '.join(summary['pipelines']) or '-'}")
        click.echo(f"Last checked  : {summary['last_checked'] or '-'}")
