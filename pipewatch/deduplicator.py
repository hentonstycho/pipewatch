"""Deduplicator – suppress repeated notifications for the same failure.

Tracks which (pipeline, violation) pairs have already triggered a
notification so that re-runs do not flood Slack / email until the
pipeline recovers and fails again.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipewatch.checker import CheckResult

_DEFAULT_DIR = Path(os.environ.get("PIPEWATCH_HOME", Path.home() / ".pipewatch"))


def _dedup_path(base_dir: Path) -> Path:
    return base_dir / "dedup.json"


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_state(base_dir: Path) -> dict:
    path = _dedup_path(base_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_state(state: dict, base_dir: Path) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    _dedup_path(base_dir).write_text(json.dumps(state, indent=2))


def _key(result: CheckResult) -> str:
    """Stable key representing a specific failure mode for a pipeline."""
    return f"{result.pipeline_name}::{result.violation or 'ok'}"


def is_duplicate(result: CheckResult, base_dir: Optional[Path] = None) -> bool:
    """Return True if this (pipeline, violation) was already notified and the
    pipeline has not recovered since."""
    base_dir = base_dir or _DEFAULT_DIR
    state = _load_state(base_dir)
    return _key(result) in state


def mark_notified(result: CheckResult, base_dir: Optional[Path] = None) -> None:
    """Record that a notification was sent for this failure."""
    base_dir = base_dir or _DEFAULT_DIR
    state = _load_state(base_dir)
    state[_key(result)] = {"notified_at": _now_utc(), "violation": result.violation}
    _save_state(state, base_dir)


def clear_pipeline(pipeline_name: str, base_dir: Optional[Path] = None) -> int:
    """Remove all dedup entries for *pipeline_name* (called on recovery).
    Returns the number of entries removed."""
    base_dir = base_dir or _DEFAULT_DIR
    state = _load_state(base_dir)
    prefix = f"{pipeline_name}::"
    keys_to_remove = [k for k in state if k.startswith(prefix)]
    for k in keys_to_remove:
        del state[k]
    if keys_to_remove:
        _save_state(state, base_dir)
    return len(keys_to_remove)


def should_notify(result: CheckResult, base_dir: Optional[Path] = None) -> bool:
    """High-level helper: returns True when a notification should be sent.

    * Healthy results always return False (no notification needed).
    * Failing results return True only on the *first* occurrence; subsequent
      identical failures are suppressed until the pipeline recovers.
    """
    if result.healthy:
        clear_pipeline(result.pipeline_name, base_dir)
        return False
    if is_duplicate(result, base_dir):
        return False
    mark_notified(result, base_dir)
    return True
