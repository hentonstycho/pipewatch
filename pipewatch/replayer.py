"""replayer.py – replay historical check results through the alerting pipeline.

Useful for testing threshold/notification configs against real history without
waiting for live runs.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional

from pipewatch.checker import CheckResult
from pipewatch.config import PipelineConfig
from pipewatch.history import _history_path


@dataclass
class ReplayEvent:
    pipeline: str
    result: CheckResult
    original_ts: datetime

    def __str__(self) -> str:  # pragma: no cover
        status = "OK" if self.result.healthy else "FAIL"
        return f"[{self.original_ts.isoformat()}] {self.pipeline}: {status}"


def _parse_ts(raw: str) -> datetime:
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return datetime.now(timezone.utc)


def _load_events(pipeline: str, history_dir: Path) -> Iterator[ReplayEvent]:
    path = _history_path(pipeline, history_dir)
    if not path.exists():
        return
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            result = CheckResult(
                pipeline=data.get("pipeline", pipeline),
                healthy=data.get("healthy", True),
                violations=data.get("violations", []),
                metrics=data.get("metrics", {}),
            )
            ts = _parse_ts(data.get("checked_at", ""))
            yield ReplayEvent(pipeline=pipeline, result=result, original_ts=ts)


def replay_pipeline(
    cfg: PipelineConfig,
    history_dir: Path,
    *,
    since: Optional[datetime] = None,
    limit: Optional[int] = None,
) -> List[ReplayEvent]:
    """Return replay events for *cfg* ordered oldest-first."""
    events = list(_load_events(cfg.name, history_dir))
    if since is not None:
        events = [e for e in events if e.original_ts >= since]
    events.sort(key=lambda e: e.original_ts)
    if limit is not None:
        events = events[-limit:]
    return events


def replay_all(
    pipelines: List[PipelineConfig],
    history_dir: Path,
    *,
    since: Optional[datetime] = None,
    limit: Optional[int] = None,
) -> List[ReplayEvent]:
    """Merge replay events for all pipelines, sorted by timestamp."""
    merged: List[ReplayEvent] = []
    for cfg in pipelines:
        merged.extend(replay_pipeline(cfg, history_dir, since=since, limit=limit))
    merged.sort(key=lambda e: e.original_ts)
    return merged
