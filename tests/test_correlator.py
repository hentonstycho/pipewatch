"""Tests for pipewatch.correlator."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.correlator import correlate_failures, CorrelationGroup
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


BASE_TS = datetime(2024, 6, 1, 8, 0, 0, tzinfo=timezone.utc)


def _cfg(*names: str) -> PipewatchConfig:
    pipelines = [
        PipelineConfig(
            name=n,
            thresholds=ThresholdConfig(),
        )
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(),
    )


def _write_history(history_dir: Path, pipeline: str, entries: list[dict]) -> None:
    path = history_dir / f"{pipeline}.jsonl"
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _entry(healthy: bool, offset_seconds: int = 0) -> dict:
    ts = (BASE_TS + timedelta(seconds=offset_seconds)).isoformat()
    return {"healthy": healthy, "checked_at": ts, "pipeline": "x"}


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    d = tmp_path / "history"
    d.mkdir()
    return d


def test_no_failures_returns_empty(hist_dir):
    _write_history(hist_dir, "a", [_entry(True), _entry(True)])
    _write_history(hist_dir, "b", [_entry(True)])
    cfg = _cfg("a", "b")
    groups = correlate_failures(cfg, history_dir=str(hist_dir))
    assert groups == []


def test_simultaneous_failures_grouped(hist_dir):
    _write_history(hist_dir, "a", [_entry(False, 0)])
    _write_history(hist_dir, "b", [_entry(False, 60)])  # 1 min later — within 5-min window
    cfg = _cfg("a", "b")
    groups = correlate_failures(cfg, history_dir=str(hist_dir), window_minutes=5)
    assert len(groups) == 1
    assert set(groups[0].pipelines) == {"a", "b"}


def test_distant_failures_not_grouped(hist_dir):
    _write_history(hist_dir, "a", [_entry(False, 0)])
    _write_history(hist_dir, "b", [_entry(False, 600)])  # 10 min later
    cfg = _cfg("a", "b")
    groups = correlate_failures(cfg, history_dir=str(hist_dir), window_minutes=5)
    assert groups == []


def test_three_pipeline_group(hist_dir):
    _write_history(hist_dir, "a", [_entry(False, 0)])
    _write_history(hist_dir, "b", [_entry(False, 30)])
    _write_history(hist_dir, "c", [_entry(False, 90)])
    cfg = _cfg("a", "b", "c")
    groups = correlate_failures(cfg, history_dir=str(hist_dir), window_minutes=5)
    assert len(groups) == 1
    assert groups[0].size == 3


def test_healthy_entries_ignored(hist_dir):
    _write_history(hist_dir, "a", [_entry(True, 0), _entry(False, 0)])
    _write_history(hist_dir, "b", [_entry(True, 30), _entry(True, 60)])
    cfg = _cfg("a", "b")
    groups = correlate_failures(cfg, history_dir=str(hist_dir))
    # Only pipeline a has a failure — no group possible
    assert groups == []


def test_missing_history_file_skipped(hist_dir):
    # Only write history for one pipeline; the other has no file
    _write_history(hist_dir, "a", [_entry(False, 0)])
    cfg = _cfg("a", "b")
    groups = correlate_failures(cfg, history_dir=str(hist_dir))
    assert groups == []
