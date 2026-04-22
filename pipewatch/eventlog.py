"""Event log — records discrete pipeline lifecycle events for audit and replay."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _event_path(base_dir: str, pipeline: str) -> Path:
    return Path(base_dir) / f"{pipeline}.events.jsonl"


@dataclass
class Event:
    pipeline: str
    event_type: str  # e.g. "check", "alert", "silence", "recover"
    message: str
    timestamp: str
    metadata: Optional[dict] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Event":
        return cls(
            pipeline=d["pipeline"],
            event_type=d["event_type"],
            message=d["message"],
            timestamp=d["timestamp"],
            metadata=d.get("metadata"),
        )


def record_event(
    pipeline: str,
    event_type: str,
    message: str,
    base_dir: str = ".pipewatch/events",
    metadata: Optional[dict] = None,
) -> Event:
    """Append a new event to the pipeline event log."""
    path = _event_path(base_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    event = Event(
        pipeline=pipeline,
        event_type=event_type,
        message=message,
        timestamp=_now_utc(),
        metadata=metadata,
    )
    with path.open("a") as fh:
        fh.write(json.dumps(event.to_dict()) + "\n")
    return event


def load_events(
    pipeline: str,
    base_dir: str = ".pipewatch/events",
    event_type: Optional[str] = None,
) -> List[Event]:
    """Load all events for a pipeline, optionally filtered by type."""
    path = _event_path(base_dir, pipeline)
    if not path.exists():
        return []
    events: List[Event] = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                e = Event.from_dict(json.loads(line))
                if event_type is None or e.event_type == event_type:
                    events.append(e)
            except (KeyError, json.JSONDecodeError):
                continue
    return events


def event_summary(events: List[Event]) -> dict:
    """Return counts per event_type from a list of events."""
    summary: dict = {}
    for e in events:
        summary[e.event_type] = summary.get(e.event_type, 0) + 1
    return summary
