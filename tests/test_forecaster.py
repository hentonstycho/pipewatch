"""Tests for pipewatch.forecaster."""
from __future__ import annotations

import pytest

from pipewatch.forecaster import (
    ForecastResult,
    _linear_forecast,
    _trend_label,
    forecast_all,
    forecast_pipeline,
)
from pipewatch.metrics import PipelineMetrics


def _metrics(
    pipeline: str = "pipe_a",
    run_count: int = 10,
    success_rate: float | None = 0.9,
    error_rate: float | None = 0.1,
    avg_latency_seconds: float | None = 1.5,
) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline=pipeline,
        run_count=run_count,
        success_rate=success_rate,
        error_rate=error_rate,
        avg_latency_seconds=avg_latency_seconds,
    )


# ---------------------------------------------------------------------------
# _linear_forecast
# ---------------------------------------------------------------------------

def test_linear_forecast_returns_none_for_single_point():
    assert _linear_forecast([1.0]) is None


def test_linear_forecast_flat_series_returns_last_value():
    result = _linear_forecast([0.5, 0.5, 0.5])
    assert result == pytest.approx(0.5)


def test_linear_forecast_increasing_series():
    result = _linear_forecast([1.0, 2.0, 3.0], steps_ahead=1)
    assert result == pytest.approx(4.0)


def test_linear_forecast_decreasing_series():
    result = _linear_forecast([3.0, 2.0, 1.0], steps_ahead=1)
    assert result == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# _trend_label
# ---------------------------------------------------------------------------

def test_trend_label_unknown_when_none():
    assert _trend_label(None, None, "error_rate") == "unknown"


def test_trend_label_stable_when_no_change():
    assert _trend_label(0.5, 0.5, "success_rate") == "stable"


def test_trend_label_degrading_when_error_rate_rises():
    assert _trend_label(0.1, 0.2, "error_rate") == "degrading"


def test_trend_label_improving_when_error_rate_falls():
    assert _trend_label(0.2, 0.1, "error_rate") == "improving"


def test_trend_label_improving_when_success_rate_rises():
    assert _trend_label(0.8, 0.9, "success_rate") == "improving"


def test_trend_label_degrading_when_success_rate_falls():
    assert _trend_label(0.9, 0.7, "success_rate") == "degrading"


# ---------------------------------------------------------------------------
# forecast_pipeline
# ---------------------------------------------------------------------------

def test_forecast_pipeline_returns_three_results():
    results = forecast_pipeline(_metrics())
    assert len(results) == 3
    metrics_names = {r.metric for r in results}
    assert metrics_names == {"success_rate", "error_rate", "avg_latency_seconds"}


def test_forecast_pipeline_unknown_when_value_is_none():
    m = _metrics(success_rate=None, error_rate=None, avg_latency_seconds=None)
    results = forecast_pipeline(m)
    assert all(r.trend == "unknown" for r in results)


def test_forecast_pipeline_delta_is_none_when_unknown():
    m = _metrics(success_rate=None)
    result = next(r for r in forecast_pipeline(m) if r.metric == "success_rate")
    assert result.delta is None


def test_forecast_pipeline_delta_computed():
    m = _metrics(success_rate=0.8)
    result = next(r for r in forecast_pipeline(m) if r.metric == "success_rate")
    # Flat series → delta should be ~0
    assert result.delta == pytest.approx(0.0, abs=1e-6)


# ---------------------------------------------------------------------------
# forecast_all
# ---------------------------------------------------------------------------

def test_forecast_all_covers_all_pipelines():
    metrics_list = [_metrics("a"), _metrics("b"), _metrics("c")]
    results = forecast_all(metrics_list)
    pipelines_seen = {r.pipeline for r in results}
    assert pipelines_seen == {"a", "b", "c"}


def test_forecast_all_returns_three_results_per_pipeline():
    metrics_list = [_metrics("x"), _metrics("y")]
    results = forecast_all(metrics_list)
    assert len(results) == 6
