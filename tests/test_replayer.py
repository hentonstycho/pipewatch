"""Tests for pipewatch/replayer.py."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.config import PipelineConfig, ThresholdConfig
from pipewatch.replayer import ReplayEvent, replay_all, replay_pipeline


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    d = tmp_path / "history"
    d.mkdir()
    return d


def _make_config(name: str) -> PipelineConfig:
    return PipelineConfig(
        name=name,
        source="test",
        thresholds=ThresholdConfig(),
    )


def _write_entry(hist_dir: Path, pipeline: str, healthy: bool, ts: str, violations: list | None = None) -> None:
    path = hist_dir / f"{pipeline}.jsonl"
    entry = {
        "pipeline": pipeline,
        "healthy": healthy,
        "violations": violations or [],
        "metrics": {},
        "checked_at": ts,
    }
    with path.open("a") as fh:
        fh.write(json.dumps(entry) + "\n")


def test_replay_pipeline_empty_history(hist_dir: Path) -> None:
    cfg = _make_config("pipe_a")
    events = replay_pipeline(cfg, hist_dir)
    assert events == []


def test_replay_pipeline_returns_events(hist_dir: Path) -> None:
    _write_entry(hist_dir, "pipe_a", True, "2024-01-01T00:00:00")
    _write_entry(hist_dir, "pipe_a", False, "2024-01-02T00:00:00", ["row_count"])
    cfg = _make_config("pipe_a")
    events = replay_pipeline(cfg, hist_dir)
    assert len(events) == 2
    assert events[0].result.healthy is True
    assert events[1].result.healthy is False
    assert "row_count" in events[1].result.violations


def test_replay_pipeline_sorted_oldest_first(hist_dir: Path) -> None:
    _write_entry(hist_dir, "pipe_a", True, "2024-01-03T00:00:00")
    _write_entry(hist_dir, "pipe_a", True, "2024-01-01T00:00:00")
    cfg = _make_config("pipe_a")
    events = replay_pipeline(cfg, hist_dir)
    assert events[0].original_ts < events[1].original_ts


def test_replay_pipeline_since_filters(hist_dir: Path) -> None:
    _write_entry(hist_dir, "pipe_a", True, "2024-01-01T00:00:00")
    _write_entry(hist_dir, "pipe_a", False, "2024-06-01T00:00:00")
    cfg = _make_config("pipe_a")
    since = datetime(2024, 3, 1, tzinfo=timezone.utc)
    events = replay_pipeline(cfg, hist_dir, since=since)
    assert len(events) == 1
    assert events[0].result.healthy is False


def test_replay_pipeline_limit(hist_dir: Path) -> None:
    for i in range(5):
        _write_entry(hist_dir, "pipe_a", True, f"2024-01-0{i+1}T00:00:00")
    cfg = _make_config("pipe_a")
    events = replay_pipeline(cfg, hist_dir, limit=2)
    assert len(events) == 2


def test_replay_all_merges_and_sorts(hist_dir: Path) -> None:
    _write_entry(hist_dir, "pipe_a", True, "2024-01-02T00:00:00")
    _write_entry(hist_dir, "pipe_b", False, "2024-01-01T00:00:00")
    cfgs = [_make_config("pipe_a"), _make_config("pipe_b")]
    events = replay_all(cfgs, hist_dir)
    assert len(events) == 2
    assert events[0].pipeline == "pipe_b"
    assert events[1].pipeline == "pipe_a"


def test_replay_event_str() -> None:
    from pipewatch.checker import CheckResult
    r = CheckResult(pipeline="p", healthy=False, violations=["err"], metrics={})
    ev = ReplayEvent(pipeline="p", result=r, original_ts=datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert "p" in str(ev)
    assert "FAIL" in str(ev)
