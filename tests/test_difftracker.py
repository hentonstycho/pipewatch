"""Tests for pipewatch.difftracker."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.difftracker import (
    MetricRegression,
    diff_metrics,
    any_regressions,
    _pct,
)


def _make(pipeline: str = "orders", **kwargs) -> PipelineMetrics:
    defaults = dict(
        pipeline=pipeline,
        total_checks=10,
        failure_count=1,
        success_rate=90.0,
        error_rate=10.0,
        avg_latency=5.0,
        avg_row_count=1000.0,
    )
    defaults.update(kwargs)
    return PipelineMetrics(**defaults)


# --- _pct helper ---

def test_pct_increase():
    assert _pct(100.0, 120.0) == pytest.approx(20.0)


def test_pct_decrease():
    assert _pct(100.0, 80.0) == pytest.approx(-20.0)


def test_pct_zero_previous_returns_zero():
    assert _pct(0.0, 50.0) == 0.0


# --- diff_metrics ---

def test_diff_metrics_returns_entry_per_field():
    prev = _make()
    curr = _make()
    diffs = diff_metrics(prev, curr)
    fields = {d.field for d in diffs}
    assert {"error_rate", "avg_latency", "success_rate", "avg_row_count"} == fields


def test_diff_metrics_no_regression_when_equal():
    prev = _make()
    curr = _make()
    diffs = diff_metrics(prev, curr)
    assert not any(d.regressed for d in diffs)


def test_diff_metrics_error_rate_regression():
    prev = _make(error_rate=5.0)
    curr = _make(error_rate=20.0)  # +300% — well above 10% threshold
    diffs = diff_metrics(prev, curr)
    er = next(d for d in diffs if d.field == "error_rate")
    assert er.regressed is True


def test_diff_metrics_latency_regression():
    prev = _make(avg_latency=2.0)
    curr = _make(avg_latency=3.0)  # +50%
    diffs = diff_metrics(prev, curr, regression_threshold_pct=10.0)
    lat = next(d for d in diffs if d.field == "avg_latency")
    assert lat.regressed is True


def test_diff_metrics_success_rate_regression():
    prev = _make(success_rate=95.0)
    curr = _make(success_rate=70.0)  # -26.3%
    diffs = diff_metrics(prev, curr, regression_threshold_pct=10.0)
    sr = next(d for d in diffs if d.field == "success_rate")
    assert sr.regressed is True


def test_diff_metrics_small_change_not_regression():
    prev = _make(error_rate=10.0)
    curr = _make(error_rate=10.5)  # +5% — below default 10% threshold
    diffs = diff_metrics(prev, curr)
    er = next(d for d in diffs if d.field == "error_rate")
    assert er.regressed is False


def test_diff_metrics_pipeline_name_propagated():
    prev = _make(pipeline="sales")
    curr = _make(pipeline="sales")
    diffs = diff_metrics(prev, curr)
    assert all(d.pipeline == "sales" for d in diffs)


def test_diff_metrics_row_count_drop_regression():
    """A significant drop in avg_row_count should be flagged as a regression."""
    prev = _make(avg_row_count=1000.0)
    curr = _make(avg_row_count=500.0)  # -50% — well below default threshold
    diffs = diff_metrics(prev, curr, regression_threshold_pct=10.0)
    rc = next(d for d in diffs if d.field == "avg_row_count")
    assert rc.regressed is True


# --- any_regressions ---

def test_any_regressions_true_when_at_least_one():
    diffs = [MetricRegression("p", "error_rate", 1.0, 2.0, 100.0, True)]
    assert any_regressions(diffs) is True


def test_any_regressions_false_when_none():
    diffs = [MetricRegression("p", "error_rate", 1.0, 1.0, 0.0, False)]
    assert any_regressions(diffs) is False


def test_any_regressions_empty_list():
    """An empty diff list should never report regressions."""
    assert any_regressions([]) is False
