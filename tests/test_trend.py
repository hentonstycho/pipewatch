"""Tests for pipewatch.trend"""
from __future__ import annotations

import json
import os
import pytest

from pipewatch.trend import analyse_trend, analyse_all_trends, _slope, TrendResult
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


@pytest.fixture()
def hist_dir(tmp_path):
    return str(tmp_path)


def _write_history(hist_dir, pipeline, entries):
    path = os.path.join(hist_dir, f"{pipeline}.jsonl")
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def _make_config(names):
    pipelines = [
        PipelineConfig(name=n, query="SELECT 1", thresholds=ThresholdConfig())
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(),
    )


def test_slope_flat():
    assert _slope([0.0, 0.0, 0.0]) == pytest.approx(0.0)


def test_slope_increasing():
    s = _slope([0.0, 0.5, 1.0])
    assert s > 0


def test_slope_decreasing():
    s = _slope([1.0, 0.5, 0.0])
    assert s < 0


def test_insufficient_data_when_fewer_than_3(hist_dir):
    _write_history(hist_dir, "pipe", [{"healthy": True}, {"healthy": False}])
    r = analyse_trend("pipe", hist_dir)
    assert r.direction == "insufficient_data"
    assert r.slope is None


def test_stable_when_all_healthy(hist_dir):
    _write_history(hist_dir, "pipe", [{"healthy": True}] * 10)
    r = analyse_trend("pipe", hist_dir)
    assert r.direction == "stable"
    assert r.slope == pytest.approx(0.0)


def test_degrading_when_failures_increasing(hist_dir):
    entries = [{"healthy": True}] * 5 + [{"healthy": False}] * 10
    _write_history(hist_dir, "pipe", entries)
    r = analyse_trend("pipe", hist_dir)
    assert r.direction == "degrading"


def test_improving_when_failures_decreasing(hist_dir):
    entries = [{"healthy": False}] * 10 + [{"healthy": True}] * 5
    _write_history(hist_dir, "pipe", entries)
    r = analyse_trend("pipe", hist_dir)
    assert r.direction == "improving"


def test_analyse_all_trends_returns_one_per_pipeline(hist_dir):
    cfg = _make_config(["a", "b"])
    _write_history(hist_dir, "a", [{"healthy": True}] * 5)
    _write_history(hist_dir, "b", [{"healthy": True}] * 5)
    results = analyse_all_trends(cfg, hist_dir)
    assert len(results) == 2
    assert {r.pipeline for r in results} == {"a", "b"}


def test_str_contains_pipeline_name(hist_dir):
    _write_history(hist_dir, "mypipe", [{"healthy": True}] * 5)
    r = analyse_trend("mypipe", hist_dir)
    assert "mypipe" in str(r)
