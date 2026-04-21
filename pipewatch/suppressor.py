"""Suppress repeated notifications for pipelines that are in a known-bad state."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

SUPPRESSOR_DIR = Path(".pipewatch/suppressor")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _state_path(base_dir: Path, pipeline: str) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{pipeline}.json"


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _save_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state))


def suppress(pipeline: str, reason: str, base_dir: Path = SUPPRESSOR_DIR) -> None:
    """Mark a pipeline as suppressed with a reason."""
    path = _state_path(base_dir, pipeline)
    state = {
        "pipeline": pipeline,
        "reason": reason,
        "suppressed_at": _now_utc().isoformat(),
    }
    _save_state(path, state)


def unsuppress(pipeline: str, base_dir: Path = SUPPRESSOR_DIR) -> bool:
    """Remove suppression for a pipeline. Returns True if it existed."""
    path = _state_path(base_dir, pipeline)
    if path.exists():
        path.unlink()
        return True
    return False


def is_suppressed(pipeline: str, base_dir: Path = SUPPRESSOR_DIR) -> bool:
    """Return True if the pipeline is currently suppressed."""
    path = _state_path(base_dir, pipeline)
    return path.exists()


def get_suppression(pipeline: str, base_dir: Path = SUPPRESSOR_DIR) -> Optional[dict]:
    """Return suppression state dict or None."""
    path = _state_path(base_dir, pipeline)
    if not path.exists():
        return None
    return _load_state(path)


def list_suppressions(base_dir: Path = SUPPRESSOR_DIR) -> list[dict]:
    """Return all active suppressions."""
    if not base_dir.exists():
        return []
    results = []
    for p in sorted(base_dir.glob("*.json")):
        try:
            results.append(json.loads(p.read_text()))
        except json.JSONDecodeError:
            continue
    return results


def unsuppress_all(base_dir: Path = SUPPRESSOR_DIR) -> int:
    """Remove all active suppressions. Returns the number of suppressions removed."""
    if not base_dir.exists():
        return 0
    removed = 0
    for p in base_dir.glob("*.json"):
        try:
            p.unlink()
            removed += 1
        except OSError:
            continue
    return removed
