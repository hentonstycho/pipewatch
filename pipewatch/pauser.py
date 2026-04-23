"""pauser.py – pause and resume pipelines from being checked.

A paused pipeline is skipped during check_all_pipelines runs and
scheduled ticks.  Pauses are stored as JSON in the pipewatch data dir.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

_DATA_DIR = Path(os.environ.get("PIPEWATCH_DATA_DIR", ".pipewatch"))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _pause_path(data_dir: Path) -> Path:
    return data_dir / "pauses.json"


def _load_pauses(data_dir: Path) -> Dict[str, Optional[str]]:
    """Return mapping of pipeline_name -> ISO expiry (or None = indefinite)."""
    path = _pause_path(data_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_pauses(data_dir: Path, state: Dict[str, Optional[str]]) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    _pause_path(data_dir).write_text(json.dumps(state, indent=2))


def pause_pipeline(
    name: str,
    minutes: Optional[int] = None,
    data_dir: Path = _DATA_DIR,
) -> Optional[str]:
    """Pause *name*.  Returns ISO expiry string, or None for indefinite."""
    state = _load_pauses(data_dir)
    expiry: Optional[str] = None
    if minutes is not None:
        from datetime import timedelta
        expiry = (_now_utc() + timedelta(minutes=minutes)).isoformat()
    state[name] = expiry
    _save_pauses(data_dir, state)
    return expiry


def unpause_pipeline(name: str, data_dir: Path = _DATA_DIR) -> bool:
    """Remove pause for *name*.  Returns True if a pause was removed."""
    state = _load_pauses(data_dir)
    if name not in state:
        return False
    del state[name]
    _save_pauses(data_dir, state)
    return True


def is_paused(name: str, data_dir: Path = _DATA_DIR) -> bool:
    """Return True if *name* is currently paused (and pause has not expired)."""
    state = _load_pauses(data_dir)
    if name not in state:
        return False
    expiry = state[name]
    if expiry is None:
        return True
    return _now_utc() < datetime.fromisoformat(expiry)


def list_pauses(data_dir: Path = _DATA_DIR) -> Dict[str, Optional[str]]:
    """Return all active (non-expired) pauses."""
    state = _load_pauses(data_dir)
    now = _now_utc()
    return {
        name: expiry
        for name, expiry in state.items()
        if expiry is None or now < datetime.fromisoformat(expiry)
    }
