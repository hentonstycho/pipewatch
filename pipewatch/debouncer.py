"""Debouncer: suppress notifications until a failure persists for N consecutive checks."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from pipewatch.checker import CheckResult


@dataclass
class DebounceState:
    pipeline: str
    consecutive_failures: int
    notified: bool


def _debounce_path(base_dir: Path) -> Path:
    return base_dir / "debounce_state.json"


def _load_state(base_dir: Path) -> Dict[str, dict]:
    path = _debounce_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def _save_state(base_dir: Path, state: Dict[str, dict]) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    with _debounce_path(base_dir).open("w") as fh:
        json.dump(state, fh)


def evaluate(
    result: CheckResult,
    threshold: int,
    base_dir: Path,
) -> bool:
    """Return True if the failure should trigger a notification.

    A notification is triggered only when consecutive failures first reach
    *threshold*.  Subsequent failures do NOT re-trigger until the pipeline
    recovers (so the caller is notified exactly once per failure run).
    """
    state = _load_state(base_dir)
    entry = state.get(result.pipeline_name, {"consecutive_failures": 0, "notified": False})

    if result.healthy:
        # Reset on recovery
        state[result.pipeline_name] = {"consecutive_failures": 0, "notified": False}
        _save_state(base_dir, state)
        return False

    entry["consecutive_failures"] += 1
    should_notify = (
        entry["consecutive_failures"] >= threshold and not entry["notified"]
    )
    if should_notify:
        entry["notified"] = True
    state[result.pipeline_name] = entry
    _save_state(base_dir, state)
    return should_notify


def get_state(pipeline: str, base_dir: Path) -> Optional[DebounceState]:
    """Return the current debounce state for *pipeline*, or None if unknown."""
    raw = _load_state(base_dir)
    if pipeline not in raw:
        return None
    e = raw[pipeline]
    return DebounceState(
        pipeline=pipeline,
        consecutive_failures=e.get("consecutive_failures", 0),
        notified=e.get("notified", False),
    )
