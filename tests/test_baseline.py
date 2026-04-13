"""Tests for pipewatch.baseline."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.baseline import (
    diff_baseline,
    list_baselines,
    load_baseline,
    save_baseline,
)
from pipewatch.metrics import PipelineMetrics


@pytest.fixture()
def base_dir(tmp_path: Path) -> str:
    return str(tmp_path / "baselines")


def _metrics(avg_row_count=100.0, avg_error_rate=0.01, avg_latency_seconds=5.0):
    return PipelineMetrics(
        avg_row_count=avg_row_count,
        avg_error_rate=avg_error_rate,
        avg_latency_seconds=avg_latency_seconds,
        total_runs=10,
        failure_count=1,
    )


def test_save_baseline_creates_file(base_dir):
    path = save_baseline("v1", {"pipe_a": _metrics()}, baselines_dir=base_dir)
    assert path.exists()
    data = json.loads(path.read_text())
    assert data["name"] == "v1"
    assert "pipe_a" in data["metrics"]


def test_load_baseline_returns_none_when_missing(base_dir):
    result = load_baseline("nonexistent", baselines_dir=base_dir)
    assert result is None


def test_load_baseline_round_trips(base_dir):
    save_baseline("v2", {"pipe_a": _metrics(avg_row_count=200.0)}, baselines_dir=base_dir)
    data = load_baseline("v2", baselines_dir=base_dir)
    assert data is not None
    assert data["metrics"]["pipe_a"]["avg_row_count"] == 200.0


def test_list_baselines_empty(base_dir):
    assert list_baselines(baselines_dir=base_dir) == []


def test_list_baselines_returns_names(base_dir):
    save_baseline("alpha", {}, baselines_dir=base_dir)
    save_baseline("beta", {}, baselines_dir=base_dir)
    names = list_baselines(baselines_dir=base_dir)
    assert names == ["alpha", "beta"]


def test_diff_baseline_zero_delta(base_dir):
    m = _metrics()
    save_baseline("v1", {"pipe_a": m}, baselines_dir=base_dir)
    deltas = diff_baseline("v1", {"pipe_a": m}, baselines_dir=base_dir)
    assert deltas["pipe_a"]["avg_error_rate"] == pytest.approx(0.0)
    assert deltas["pipe_a"]["avg_latency_seconds"] == pytest.approx(0.0)


def test_diff_baseline_positive_delta(base_dir):
    old = _metrics(avg_error_rate=0.01)
    new = _metrics(avg_error_rate=0.05)
    save_baseline("v1", {"pipe_a": old}, baselines_dir=base_dir)
    deltas = diff_baseline("v1", {"pipe_a": new}, baselines_dir=base_dir)
    assert deltas["pipe_a"]["avg_error_rate"] == pytest.approx(0.04)


def test_diff_baseline_missing_pipeline_gives_none(base_dir):
    save_baseline("v1", {}, baselines_dir=base_dir)
    deltas = diff_baseline("v1", {"pipe_a": _metrics()}, baselines_dir=base_dir)
    assert deltas["pipe_a"]["avg_error_rate"] is None


def test_diff_baseline_raises_when_not_found(base_dir):
    with pytest.raises(FileNotFoundError):
        diff_baseline("missing", {}, baselines_dir=base_dir)
