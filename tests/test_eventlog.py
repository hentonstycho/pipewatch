"""Tests for pipewatch.eventlog."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.eventlog import (
    Event,
    record_event,
    load_events,
    event_summary,
)


@pytest.fixture()
def event_dir(tmp_path: Path) -> str:
    return str(tmp_path / "events")


def test_record_event_creates_file(event_dir: str) -> None:
    record_event("pipe_a", "check", "All good", base_dir=event_dir)
    path = Path(event_dir) / "pipe_a.events.jsonl"
    assert path.exists()


def test_record_event_returns_event(event_dir: str) -> None:
    e = record_event("pipe_a", "alert", "Threshold breached", base_dir=event_dir)
    assert isinstance(e, Event)
    assert e.pipeline == "pipe_a"
    assert e.event_type == "alert"
    assert e.message == "Threshold breached"
    assert e.timestamp  # non-empty


def test_record_event_appends_json_lines(event_dir: str) -> None:
    record_event("pipe_b", "check", "ok", base_dir=event_dir)
    record_event("pipe_b", "alert", "fail", base_dir=event_dir)
    path = Path(event_dir) / "pipe_b.events.jsonl"
    lines = [l for l in path.read_text().splitlines() if l.strip()]
    assert len(lines) == 2
    for line in lines:
        obj = json.loads(line)
        assert "event_type" in obj


def test_record_event_stores_metadata(event_dir: str) -> None:
    meta = {"row_count": 42}
    e = record_event("pipe_c", "check", "checked", base_dir=event_dir, metadata=meta)
    assert e.metadata == meta


def test_load_events_empty_for_unknown_pipeline(event_dir: str) -> None:
    result = load_events("no_such_pipe", base_dir=event_dir)
    assert result == []


def test_load_events_returns_all(event_dir: str) -> None:
    record_event("pipe_d", "check", "a", base_dir=event_dir)
    record_event("pipe_d", "recover", "b", base_dir=event_dir)
    events = load_events("pipe_d", base_dir=event_dir)
    assert len(events) == 2
    assert events[0].event_type == "check"
    assert events[1].event_type == "recover"


def test_load_events_filters_by_type(event_dir: str) -> None:
    record_event("pipe_e", "check", "a", base_dir=event_dir)
    record_event("pipe_e", "alert", "b", base_dir=event_dir)
    record_event("pipe_e", "check", "c", base_dir=event_dir)
    events = load_events("pipe_e", base_dir=event_dir, event_type="check")
    assert len(events) == 2
    assert all(e.event_type == "check" for e in events)


def test_event_summary_counts_types(event_dir: str) -> None:
    record_event("pipe_f", "check", "a", base_dir=event_dir)
    record_event("pipe_f", "alert", "b", base_dir=event_dir)
    record_event("pipe_f", "alert", "c", base_dir=event_dir)
    events = load_events("pipe_f", base_dir=event_dir)
    summary = event_summary(events)
    assert summary["check"] == 1
    assert summary["alert"] == 2


def test_event_summary_empty_list() -> None:
    assert event_summary([]) == {}


def test_event_round_trips_via_dict(event_dir: str) -> None:
    e = record_event("pipe_g", "silence", "silenced", base_dir=event_dir, metadata={"minutes": 30})
    d = e.to_dict()
    restored = Event.from_dict(d)
    assert restored.pipeline == e.pipeline
    assert restored.event_type == e.event_type
    assert restored.metadata == e.metadata
