"""CLI commands for managing pipeline snapshots."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import click

from pipewatch.checker import check_all_pipelines
from pipewatch.config import PipewatchConfig
from pipewatch.snapshotter import (
    diff_snapshots,
    list_snapshots,
    load_snapshot,
    save_snapshot,
)


@click.group("snapshot")
def snapshot_cmd() -> None:
    """Create and compare pipeline check snapshots."""


@snapshot_cmd.command("take")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--label", default=None, help="Optional label for the snapshot file.")
@click.option("--snapshot-dir", default=None, help="Directory to store snapshots.")
def take_cmd(config_path: str, label: Optional[str], snapshot_dir: Optional[str]) -> None:
    """Run checks and save results as a snapshot."""
    cfg = PipewatchConfig.load(config_path)
    results = check_all_pipelines(cfg)
    sdir = Path(snapshot_dir) if snapshot_dir else None
    path = save_snapshot(results, label=label, snapshot_dir=sdir)
    click.echo(f"Snapshot saved: {path}")


@snapshot_cmd.command("list")
@click.option("--snapshot-dir", default=None, help="Directory to list snapshots from.")
def list_cmd(snapshot_dir: Optional[str]) -> None:
    """List all available snapshots."""
    sdir = Path(snapshot_dir) if snapshot_dir else None
    snapshots = list_snapshots(sdir)
    if not snapshots:
        click.echo("No snapshots found.")
        return
    for p in snapshots:
        snap = load_snapshot(p)
        label = snap.get("label") or "-"
        created = snap.get("created_at", "?")
        count = len(snap.get("results", []))
        click.echo(f"{p.name}  label={label}  created={created}  pipelines={count}")


@snapshot_cmd.command("diff")
@click.argument("old_path")
@click.argument("new_path")
def diff_cmd(old_path: str, new_path: str) -> None:
    """Show differences between two snapshot files."""
    old_file = Path(old_path)
    new_file = Path(new_path)

    if not old_file.exists():
        raise click.BadParameter(f"File not found: {old_path}", param_hint="'OLD_PATH'")
    if not new_file.exists():
        raise click.BadParameter(f"File not found: {new_path}", param_hint="'NEW_PATH'")

    old = load_snapshot(old_file)
    new = load_snapshot(new_file)
    lines = diff_snapshots(old, new)
    if not lines:
        click.echo("No differences found.")
        return
    for line in lines:
        click.echo(line)
