"""staletracker.py – detect pipelines whose check results have gone stale.

A pipeline is considered stale when its most-recent history entry is older
than a configurable max_age_minutes threshold.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.config import PipewatchConfig


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class StaleEntry:
    pipeline: str
    last_checked: Optional[datetime]
    age_minutes: Optional[float]
    is_stale: bool
    max_age_minutes: float

    def __str__(self) -> str:
        if self.last_checked is None:
            return f"{self.pipeline}: never checked (stale)"
        flag = "STALE" if self.is_stale else "ok"
        return (
            f"{self.pipeline}: last checked {self.age_minutes:.1f} min ago [{flag}]"
        )


def _last_checked(history_dir: Path, pipeline: str) -> Optional[datetime]:
    """Return the timestamp of the most recent history entry, or None."""
    path = history_dir / f"{pipeline}.jsonl"
    if not path.exists():
        return None
    last_line: Optional[str] = None
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                last_line = line
    if last_line is None:
        return None
    try:
        data = json.loads(last_line)
        ts = data.get("checked_at") or data.get("timestamp")
        if ts is None:
            return None
        return datetime.fromisoformat(ts)
    except (json.JSONDecodeError, ValueError):
        return None


def track_pipeline(
    pipeline: str,
    max_age_minutes: float,
    history_dir: Path,
) -> StaleEntry:
    """Return a StaleEntry for a single pipeline."""
    last = _last_checked(history_dir, pipeline)
    if last is None:
        return StaleEntry(
            pipeline=pipeline,
            last_checked=None,
            age_minutes=None,
            is_stale=True,
            max_age_minutes=max_age_minutes,
        )
    now = _now_utc()
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    age = (now - last).total_seconds() / 60.0
    return StaleEntry(
        pipeline=pipeline,
        last_checked=last,
        age_minutes=age,
        is_stale=age > max_age_minutes,
        max_age_minutes=max_age_minutes,
    )


def track_all(
    config: PipewatchConfig,
    history_dir: Path,
    default_max_age_minutes: float = 60.0,
) -> List[StaleEntry]:
    """Return StaleEntry objects for every pipeline in *config*."""
    results: List[StaleEntry] = []
    for pipeline in config.pipelines:
        max_age = getattr(pipeline, "max_age_minutes", None) or default_max_age_minutes
        results.append(track_pipeline(pipeline.name, max_age, history_dir))
    return results
