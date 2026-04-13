"""Persist and retrieve pipeline check results for trend analysis."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipewatch.checker import CheckResult

DEFAULT_HISTORY_DIR = Path.home() / ".pipewatch" / "history"


def _history_path(pipeline_name: str, history_dir: Path) -> Path:
    safe_name = pipeline_name.replace("/", "_").replace(" ", "_")
    return history_dir / f"{safe_name}.jsonl"


def record_result(result: CheckResult, history_dir: Optional[Path] = None) -> None:
    """Append a CheckResult to the pipeline's history file."""
    history_dir = history_dir or DEFAULT_HISTORY_DIR
    history_dir.mkdir(parents=True, exist_ok=True)

    path = _history_path(result.pipeline_name, history_dir)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline_name": result.pipeline_name,
        "healthy": result.healthy,
        "violations": result.violations,
    }
    with path.open("a") as fh:
        fh.write(json.dumps(entry) + "\n")


def load_history(
    pipeline_name: str,
    limit: int = 50,
    history_dir: Optional[Path] = None,
) -> list[dict]:
    """Return the most recent *limit* records for a pipeline."""
    history_dir = history_dir or DEFAULT_HISTORY_DIR
    path = _history_path(pipeline_name, history_dir)

    if not path.exists():
        return []

    lines = path.read_text().splitlines()
    recent = lines[-limit:] if len(lines) > limit else lines
    return [json.loads(line) for line in recent if line.strip()]


def consecutive_failures(
    pipeline_name: str,
    history_dir: Optional[Path] = None,
) -> int:
    """Return the number of consecutive failures at the tail of history."""
    records = load_history(pipeline_name, limit=100, history_dir=history_dir)
    count = 0
    for record in reversed(records):
        if not record["healthy"]:
            count += 1
        else:
            break
    return count
