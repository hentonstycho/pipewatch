"""Silence (mute) alerts for specific pipelines for a given duration."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

_SILENCE_FILE = Path(".pipewatch") / "silences.json"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _load_silences(silence_path: Path = _SILENCE_FILE) -> dict[str, str]:
    """Return mapping of pipeline_name -> ISO expiry timestamp."""
    if not silence_path.exists():
        return {}
    try:
        return json.loads(silence_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_silences(silences: dict[str, str], silence_path: Path = _SILENCE_FILE) -> None:
    silence_path.parent.mkdir(parents=True, exist_ok=True)
    silence_path.write_text(json.dumps(silences, indent=2))


def silence_pipeline(
    pipeline_name: str,
    duration_minutes: int,
    silence_path: Path = _SILENCE_FILE,
) -> datetime:
    """Silence alerts for *pipeline_name* for *duration_minutes* minutes.

    Returns the expiry datetime.
    """
    silences = _load_silences(silence_path)
    expiry = _now_utc() + timedelta(minutes=duration_minutes)
    silences[pipeline_name] = expiry.isoformat()
    _save_silences(silences, silence_path)
    return expiry


def unsilence_pipeline(
    pipeline_name: str,
    silence_path: Path = _SILENCE_FILE,
) -> bool:
    """Remove silence for *pipeline_name*. Returns True if an entry was removed."""
    silences = _load_silences(silence_path)
    if pipeline_name in silences:
        del silences[pipeline_name]
        _save_silences(silences, silence_path)
        return True
    return False


def is_silenced(
    pipeline_name: str,
    silence_path: Path = _SILENCE_FILE,
) -> bool:
    """Return True if *pipeline_name* currently has an active silence."""
    silences = _load_silences(silence_path)
    expiry_str = silences.get(pipeline_name)
    if expiry_str is None:
        return False
    try:
        expiry = datetime.fromisoformat(expiry_str)
    except ValueError:
        return False
    return _now_utc() < expiry


def get_expiry(
    pipeline_name: str,
    silence_path: Path = _SILENCE_FILE,
) -> Optional[datetime]:
    """Return the expiry datetime for *pipeline_name*, or None if not silenced."""
    silences = _load_silences(silence_path)
    expiry_str = silences.get(pipeline_name)
    if expiry_str is None:
        return None
    try:
        return datetime.fromisoformat(expiry_str)
    except ValueError:
        return None
