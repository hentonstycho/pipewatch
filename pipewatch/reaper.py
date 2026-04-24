"""reaper.py – Detect and report pipelines that have not produced any
results within a configurable look-back window ("dead" pipelines).
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
class ReaperResult:
    pipeline: str
    last_seen: Optional[datetime]  # None when no history exists
    age_hours: Optional[float]     # None when no history exists
    dead: bool

    def __str__(self) -> str:  # pragma: no cover
        if self.dead:
            age = f"{self.age_hours:.1f}h" if self.age_hours is not None else "never"
            return f"[DEAD] {self.pipeline} – last seen {age} ago"
        return f"[ALIVE] {self.pipeline}"


def _history_path(history_dir: str, pipeline: str) -> Path:
    return Path(history_dir) / f"{pipeline}.jsonl"


def _last_seen(history_dir: str, pipeline: str) -> Optional[datetime]:
    path = _history_path(history_dir, pipeline)
    if not path.exists():
        return None
    last: Optional[datetime] = None
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                ts = datetime.fromisoformat(entry["checked_at"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if last is None or ts > last:
                    last = ts
            except (KeyError, ValueError):
                continue
    return last


def reap_pipeline(
    pipeline: str,
    *,
    history_dir: str,
    threshold_hours: float = 24.0,
    now: Optional[datetime] = None,
) -> ReaperResult:
    """Return a ReaperResult for *pipeline*."""
    now = now or _now_utc()
    last = _last_seen(history_dir, pipeline)
    if last is None:
        return ReaperResult(pipeline=pipeline, last_seen=None, age_hours=None, dead=True)
    age = (now - last).total_seconds() / 3600.0
    return ReaperResult(
        pipeline=pipeline,
        last_seen=last,
        age_hours=round(age, 2),
        dead=age > threshold_hours,
    )


def reap_all(
    cfg: PipewatchConfig,
    *,
    threshold_hours: float = 24.0,
    now: Optional[datetime] = None,
) -> List[ReaperResult]:
    """Run reap_pipeline for every pipeline defined in *cfg*."""
    return [
        reap_pipeline(
            p.name,
            history_dir=cfg.history_dir,
            threshold_hours=threshold_hours,
            now=now,
        )
        for p in cfg.pipelines
    ]
