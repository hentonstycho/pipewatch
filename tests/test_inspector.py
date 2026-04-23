"""Tests for pipewatch.inspector."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig
from pipewatch.inspector import inspect_pipeline, inspect_all, InspectionResult


@pytest.fixture()
def hist_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _write_history(hist_dir: str, pipeline: str, entries: list) -> None:
    path = Path(hist_dir) / f"{pipeline}.jsonl"
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _make_config(*names: str) -> PipewatchConfig:
    pipelines = [
        PipelineConfig(
            name=n,
            source="dummy",
            thresholds=ThresholdConfig(),
        )
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(),
    )


# ---------------------------------------------------------------------------

def test_inspect_pipeline_unknown_returns_none(hist_dir):
    cfg = _make_config("pipe_a")
    result = inspect_pipeline("does_not_exist", cfg, history_dir=hist_dir)
    assert result is None


def test_inspect_pipeline_no_history(hist_dir):
    cfg = _make_config("pipe_a")
    result = inspect_pipeline("pipe_a", cfg, history_dir=hist_dir)
    assert isinstance(result, InspectionResult)
    assert result.total_runs == 0
    assert result.success_rate is None
    assert result.last_status is None


def test_inspect_pipeline_all_healthy(hist_dir):
    cfg = _make_config("pipe_a")
    entries = [
        {"healthy": True, "checked_at": "2024-01-01T00:00:00Z"},
        {"healthy": True, "checked_at": "2024-01-01T01:00:00Z"},
    ]
    _write_history(hist_dir, "pipe_a", entries)
    result = inspect_pipeline("pipe_a", cfg, history_dir=hist_dir)
    assert result.total_runs == 2
    assert result.healthy_runs == 2
    assert result.failed_runs == 0
    assert result.success_rate == 100.0
    assert result.last_status == "ok"


def test_inspect_pipeline_with_failures(hist_dir):
    cfg = _make_config("pipe_b")
    entries = [
        {"healthy": True, "checked_at": "2024-01-01T00:00:00Z"},
        {"healthy": False, "checked_at": "2024-01-01T01:00:00Z", "message": "row count too low"},
        {"healthy": False, "checked_at": "2024-01-01T02:00:00Z", "message": "latency exceeded"},
    ]
    _write_history(hist_dir, "pipe_b", entries)
    result = inspect_pipeline("pipe_b", cfg, history_dir=hist_dir)
    assert result.failed_runs == 2
    assert result.last_status == "fail"
    assert "row count too low" in result.failure_messages
    assert "latency exceeded" in result.failure_messages


def test_inspect_pipeline_limit_respected(hist_dir):
    cfg = _make_config("pipe_c")
    entries = [{"healthy": True, "checked_at": "2024-01-01T00:00:00Z"}] * 100
    _write_history(hist_dir, "pipe_c", entries)
    result = inspect_pipeline("pipe_c", cfg, history_dir=hist_dir, limit=10)
    assert result.total_runs == 10


def test_inspect_pipeline_str_representation(hist_dir):
    cfg = _make_config("pipe_d")
    entries = [{"healthy": True, "checked_at": "2024-01-01T00:00:00Z"}]
    _write_history(hist_dir, "pipe_d", entries)
    result = inspect_pipeline("pipe_d", cfg, history_dir=hist_dir)
    text = str(result)
    assert "pipe_d" in text
    assert "success_rate=100.0%" in text


def test_inspect_all_returns_all_pipelines(hist_dir):
    cfg = _make_config("alpha", "beta", "gamma")
    results = inspect_all(cfg, history_dir=hist_dir)
    assert len(results) == 3
    names = {r.pipeline for r in results}
    assert names == {"alpha", "beta", "gamma"}
