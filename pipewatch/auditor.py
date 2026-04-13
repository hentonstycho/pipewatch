"""Audit log: record every check action with timestamp and outcome."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from pipewatch.checker import CheckResult

_AUDIT_FILENAME = "audit.jsonl"


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _audit_path(data_dir: str = ".pipewatch") -> Path:
    return Path(data_dir) / _AUDIT_FILENAME


def record_audit(
    result: CheckResult,
    action: str = "check",
    data_dir: str = ".pipewatch",
) -> None:
    """Append a single audit entry for *result* to the audit log."""
    path = _audit_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": _now_utc(),
        "action": action,
        "pipeline": result.pipeline_name,
        "healthy": result.healthy,
        "violations": result.violations,
    }
    with path.open("a") as fh:
        fh.write(json.dumps(entry) + "\n")


def load_audit_log(data_dir: str = ".pipewatch") -> List[dict]:
    """Return all audit entries as a list of dicts (oldest first)."""
    path = _audit_path(data_dir)
    if not path.exists():
        return []
    entries: List[dict] = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


def audit_summary(data_dir: str = ".pipewatch") -> dict:
    """Return a high-level summary of the audit log."""
    entries = load_audit_log(data_dir)
    total = len(entries)
    healthy = sum(1 for e in entries if e.get("healthy"))
    pipelines = list({e["pipeline"] for e in entries})
    last_checked = entries[-1]["timestamp"] if entries else None
    return {
        "total_checks": total,
        "healthy_checks": healthy,
        "failed_checks": total - healthy,
        "pipelines": sorted(pipelines),
        "last_checked": last_checked,
    }
