"""Snapshot pipeline check results to a JSON file for diffing and auditing."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.checker import CheckResult

_DEFAULT_SNAPSHOT_DIR = Path(".pipewatch/snapshots")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _snapshot_path(snapshot_dir: Path, label: Optional[str] = None) -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = _now_utc().replace(":", "-")
    name = f"{label}_{ts}.json" if label else f"{ts}.json"
    return snapshot_dir / name


def save_snapshot(
    results: List[CheckResult],
    label: Optional[str] = None,
    snapshot_dir: Optional[Path] = None,
) -> Path:
    """Serialise *results* to a timestamped JSON snapshot file."""
    directory = snapshot_dir or _DEFAULT_SNAPSHOT_DIR
    path = _snapshot_path(directory, label)
    payload = {
        "created_at": _now_utc(),
        "label": label,
        "results": [
            {
                "pipeline": r.pipeline,
                "healthy": r.healthy,
                "violations": r.violations,
                "checked_at": r.checked_at,
            }
            for r in results
        ],
    }
    path.write_text(json.dumps(payload, indent=2))
    return path


def load_snapshot(path: Path) -> dict:
    """Load a previously saved snapshot from *path*."""
    return json.loads(path.read_text())


def list_snapshots(snapshot_dir: Optional[Path] = None) -> List[Path]:
    """Return all snapshot files sorted by modification time (oldest first)."""
    directory = snapshot_dir or _DEFAULT_SNAPSHOT_DIR
    if not directory.exists():
        return []
    files = sorted(directory.glob("*.json"), key=lambda p: p.stat().st_mtime)
    return files


def diff_snapshots(old: dict, new: dict) -> List[str]:
    """Return a list of human-readable change lines between two snapshots."""
    lines: List[str] = []
    old_map = {r["pipeline"]: r for r in old.get("results", [])}
    new_map = {r["pipeline"]: r for r in new.get("results", [])}

    all_pipelines = sorted(set(old_map) | set(new_map))
    for name in all_pipelines:
        if name not in old_map:
            lines.append(f"[NEW]     {name}: healthy={new_map[name]['healthy']}")
        elif name not in new_map:
            lines.append(f"[REMOVED] {name}")
        else:
            o, n = old_map[name], new_map[name]
            if o["healthy"] != n["healthy"]:
                state = "RECOVERED" if n["healthy"] else "DEGRADED"
                lines.append(f"[{state}] {name}: {o['healthy']} -> {n['healthy']}")
            elif set(o["violations"]) != set(n["violations"]):
                lines.append(
                    f"[CHANGED] {name}: violations {o['violations']} -> {n['violations']}"
                )
    if not lines:
        lines.append("No changes detected between snapshots.")
    return lines
