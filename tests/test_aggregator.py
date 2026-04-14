"""Tests for pipewatch.aggregator."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.aggregator import aggregate, RollupStats
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


def _make_config(names):
    pipelines = [
        PipelineConfig(
            name=n,
            source="db",
            thresholds=ThresholdConfig(),
        )
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(),
    )


def _write_result(history_dir, name, healthy, ts=None):
    ts = ts or datetime.now(timezone.utc).isoformat()
    path = Path(history_dir) / f"{name}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(json.dumps({"pipeline": name, "healthy": healthy, "timestamp": ts, "violations": []}) + "\n")


@pytest.fixture()
def hist_dir(tmp_path):
    return str(tmp_path / "history")


def test_aggregate_empty_history(hist_dir):
    cfg = _make_config(["alpha", "beta"])
    stats = aggregate(cfg, history_dir=hist_dir)
    assert stats.total_pipelines == 2
    assert stats.total_checks == 0
    assert stats.failure_rate == 0.0
    assert stats.most_failing is None


def test_aggregate_all_healthy(hist_dir):
    cfg = _make_config(["alpha", "beta"])
    _write_result(hist_dir, "alpha", True)
    _write_result(hist_dir, "beta", True)
    stats = aggregate(cfg, history_dir=hist_dir)
    assert stats.healthy_pipelines == 2
    assert stats.degraded_pipelines == 0
    assert stats.total_failures == 0


def test_aggregate_some_failures(hist_dir):
    cfg = _make_config(["alpha", "beta"])
    _write_result(hist_dir, "alpha", False)
    _write_result(hist_dir, "alpha", False)
    _write_result(hist_dir, "beta", True)
    stats = aggregate(cfg, history_dir=hist_dir)
    assert stats.total_failures == 2
    assert stats.most_failing == "alpha"
    assert stats.degraded_pipelines == 1
    assert stats.healthy_pipelines == 1


def test_aggregate_failure_rate(hist_dir):
    cfg = _make_config(["alpha"])
    _write_result(hist_dir, "alpha", True)
    _write_result(hist_dir, "alpha", False)
    stats = aggregate(cfg, history_dir=hist_dir)
    assert stats.failure_rate == pytest.approx(0.5)


def test_aggregate_pipelines_list(hist_dir):
    cfg = _make_config(["x", "y", "z"])
    stats = aggregate(cfg, history_dir=hist_dir)
    assert set(stats.pipelines) == {"x", "y", "z"}
