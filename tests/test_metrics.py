"""Tests for pipewatch.metrics."""
from __future__ import annotations

import os
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult
from pipewatch.history import record_result
from pipewatch.metrics import compute_metrics, compute_all_metrics, PipelineMetrics


@pytest.fixture()
def history_dir(tmp_path):
    return str(tmp_path)


def _make_result(name: str, healthy: bool, row_count=100, error_rate=0.01, latency=1.5):
    return CheckResult(
        pipeline_name=name,
        healthy=healthy,
        violations=[],
        row_count=row_count,
        error_rate=error_rate,
        latency_seconds=latency,
        checked_at=datetime.now(timezone.utc).isoformat(),
    )


def test_compute_metrics_empty_history(history_dir):
    m = compute_metrics("pipe_a", history_dir=history_dir)
    assert m.pipeline_name == "pipe_a"
    assert m.total_checks == 0
    assert m.uptime_pct == 100.0
    assert m.avg_row_count is None


def test_compute_metrics_all_healthy(history_dir):
    for _ in range(4):
        record_result(_make_result("pipe_b", healthy=True, row_count=200), history_dir=history_dir)
    m = compute_metrics("pipe_b", history_dir=history_dir)
    assert m.total_checks == 4
    assert m.total_failures == 0
    assert m.uptime_pct == 100.0
    assert m.avg_row_count == pytest.approx(200.0)


def test_compute_metrics_with_failures(history_dir):
    record_result(_make_result("pipe_c", healthy=True, error_rate=0.02), history_dir=history_dir)
    record_result(_make_result("pipe_c", healthy=False, error_rate=0.10), history_dir=history_dir)
    m = compute_metrics("pipe_c", history_dir=history_dir)
    assert m.total_checks == 2
    assert m.total_failures == 1
    assert m.uptime_pct == pytest.approx(50.0)
    assert m.avg_error_rate == pytest.approx(0.06)


def test_compute_metrics_avg_latency(history_dir):
    for lat in [1.0, 2.0, 3.0]:
        record_result(_make_result("pipe_d", healthy=True, latency=lat), history_dir=history_dir)
    m = compute_metrics("pipe_d", history_dir=history_dir)
    assert m.avg_latency_seconds == pytest.approx(2.0)


def test_compute_all_metrics(history_dir):
    record_result(_make_result("p1", healthy=True), history_dir=history_dir)
    record_result(_make_result("p2", healthy=False), history_dir=history_dir)
    results = compute_all_metrics(["p1", "p2"], history_dir=history_dir)
    assert len(results) == 2
    names = {r.pipeline_name for r in results}
    assert names == {"p1", "p2"}


def test_compute_all_metrics_empty_list(history_dir):
    assert compute_all_metrics([], history_dir=history_dir) == []
