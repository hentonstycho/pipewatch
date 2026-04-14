"""Correlate failures across pipelines to surface shared root causes."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from pipewatch.history import load_history
from pipewatch.checker import CheckResult
from pipewatch.config import PipewatchConfig


@dataclass
class CorrelationGroup:
    """A set of pipelines that failed within the same time window."""
    window_start: datetime
    window_end: datetime
    pipelines: List[str] = field(default_factory=list)

    @property
    def size(self) -> int:
        return len(self.pipelines)


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)


def _failure_times(pipeline: str, history_dir: str) -> List[datetime]:
    """Return UTC datetimes for every failing result in history."""
    times: List[datetime] = []
    for entry in load_history(pipeline, history_dir=history_dir):
        if not entry.get("healthy", True):
            try:
                times.append(_parse_ts(entry["checked_at"]))
            except (KeyError, ValueError):
                pass
    return times


def correlate_failures(
    config: PipewatchConfig,
    history_dir: str = ".pipewatch/history",
    window_minutes: int = 5,
) -> List[CorrelationGroup]:
    """Find groups of pipelines that failed within *window_minutes* of each other."""
    window = timedelta(minutes=window_minutes)
    failure_map: Dict[str, List[datetime]] = {}

    for pipeline in config.pipelines:
        times = _failure_times(pipeline.name, history_dir)
        if times:
            failure_map[pipeline.name] = times

    # Collect all (time, pipeline) events and sort chronologically
    events: List[tuple[datetime, str]] = [
        (t, name)
        for name, times in failure_map.items()
        for t in times
    ]
    events.sort(key=lambda e: e[0])

    groups: List[CorrelationGroup] = []
    used: set[int] = set()

    for i, (ts_i, pipe_i) in enumerate(events):
        if i in used:
            continue
        group = CorrelationGroup(
            window_start=ts_i,
            window_end=ts_i + window,
            pipelines=[pipe_i],
        )
        used.add(i)
        for j, (ts_j, pipe_j) in enumerate(events):
            if j in used:
                continue
            if pipe_j != pipe_i and ts_i <= ts_j <= ts_i + window:
                group.pipelines.append(pipe_j)
                used.add(j)
        if group.size > 1:
            groups.append(group)

    return groups
