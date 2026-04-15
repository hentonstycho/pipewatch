"""Throttler: rate-limit notifications so the same pipeline cannot spam
alerts more than once per configurable window."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from pipewatch.checker import CheckResult

_DEFAULT_DIR = Path(os.getenv("PIPEWATCH_DATA_DIR", ".pipewatch")) / "throttle"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _throttle_path(data_dir: Path) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "throttle_state.json"


def _load_state(data_dir: Path) -> dict[str, str]:
    path = _throttle_path(data_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_state(data_dir: Path, state: dict[str, str]) -> None:
    _throttle_path(data_dir).write_text(json.dumps(state, indent=2))


def is_throttled(
    result: CheckResult,
    window_minutes: int = 60,
    data_dir: Path = _DEFAULT_DIR,
) -> bool:
    """Return True if a notification for *result.pipeline* was already sent
    within *window_minutes* and should therefore be suppressed."""
    state = _load_state(data_dir)
    last_str: Optional[str] = state.get(result.pipeline)
    if last_str is None:
        return False
    last_sent = datetime.fromisoformat(last_str)
    return (_now_utc() - last_sent) < timedelta(minutes=window_minutes)


def mark_notified(
    result: CheckResult,
    data_dir: Path = _DEFAULT_DIR,
) -> None:
    """Record that a notification was just dispatched for *result.pipeline*."""
    state = _load_state(data_dir)
    state[result.pipeline] = _now_utc().isoformat()
    _save_state(data_dir, state)


def clear_throttle(
    pipeline: str,
    data_dir: Path = _DEFAULT_DIR,
) -> bool:
    """Remove the throttle entry for *pipeline*. Returns True if it existed."""
    state = _load_state(data_dir)
    if pipeline not in state:
        return False
    del state[pipeline]
    _save_state(data_dir, state)
    return True
