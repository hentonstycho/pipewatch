"""Retry tracking: record check attempts and surface pipelines with repeated transient errors."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from pipewatch.checker import CheckResult

_DEFAULT_DIR = Path(".pipewatch/retries")


@dataclass
class RetryState:
    pipeline: str
    attempts: int = 0
    last_error: Optional[str] = None
    resolved: bool = False


def _retry_path(pipeline: str, base_dir: Path = _DEFAULT_DIR) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{pipeline}.json"


def _load_state(pipeline: str, base_dir: Path = _DEFAULT_DIR) -> RetryState:
    path = _retry_path(pipeline, base_dir)
    if not path.exists():
        return RetryState(pipeline=pipeline)
    data = json.loads(path.read_text())
    return RetryState(**data)


def _save_state(state: RetryState, base_dir: Path = _DEFAULT_DIR) -> None:
    path = _retry_path(state.pipeline, base_dir)
    path.write_text(json.dumps(state.__dict__))


def record_attempt(result: CheckResult, base_dir: Path = _DEFAULT_DIR) -> RetryState:
    """Increment attempt counter if failing; reset if healthy."""
    state = _load_state(result.pipeline, base_dir)
    if result.healthy:
        state.attempts = 0
        state.last_error = None
        state.resolved = True
    else:
        state.attempts += 1
        state.last_error = result.message
        state.resolved = False
    _save_state(state, base_dir)
    return state


def get_state(pipeline: str, base_dir: Path = _DEFAULT_DIR) -> RetryState:
    return _load_state(pipeline, base_dir)


def exceeds_threshold(pipeline: str, max_attempts: int, base_dir: Path = _DEFAULT_DIR) -> bool:
    """Return True if the pipeline has failed more times than the allowed threshold."""
    state = _load_state(pipeline, base_dir)
    return state.attempts >= max_attempts


def reset(pipeline: str, base_dir: Path = _DEFAULT_DIR) -> None:
    """Manually reset retry state for a pipeline."""
    state = RetryState(pipeline=pipeline, resolved=True)
    _save_state(state, base_dir)
