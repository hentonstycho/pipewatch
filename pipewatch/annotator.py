"""Pipeline annotation support — attach freeform notes to pipelines."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _annotations_path(base_dir: str, pipeline: str) -> Path:
    return Path(base_dir) / f"{pipeline}.annotations.json"


def _load_annotations(base_dir: str, pipeline: str) -> List[dict]:
    path = _annotations_path(base_dir, pipeline)
    if not path.exists():
        return []
    with path.open() as fh:
        return json.load(fh)


def _save_annotations(base_dir: str, pipeline: str, entries: List[dict]) -> None:
    path = _annotations_path(base_dir, pipeline)
    Path(base_dir).mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(entries, fh, indent=2)


def add_annotation(base_dir: str, pipeline: str, note: str, author: Optional[str] = None) -> dict:
    """Append a note to the pipeline's annotation list and return the new entry."""
    entries = _load_annotations(base_dir, pipeline)
    entry = {
        "timestamp": _now_utc(),
        "note": note,
        "author": author or os.environ.get("USER", "unknown"),
    }
    entries.append(entry)
    _save_annotations(base_dir, pipeline, entries)
    return entry


def get_annotations(base_dir: str, pipeline: str) -> List[dict]:
    """Return all annotations for a pipeline, oldest first."""
    return _load_annotations(base_dir, pipeline)


def clear_annotations(base_dir: str, pipeline: str) -> int:
    """Remove all annotations for a pipeline. Returns count removed."""
    entries = _load_annotations(base_dir, pipeline)
    count = len(entries)
    _save_annotations(base_dir, pipeline, [])
    return count


def delete_annotation(base_dir: str, pipeline: str, index: int) -> bool:
    """Delete annotation at *index* (0-based). Returns True if removed."""
    entries = _load_annotations(base_dir, pipeline)
    if index < 0 or index >= len(entries):
        return False
    entries.pop(index)
    _save_annotations(base_dir, pipeline, entries)
    return True
