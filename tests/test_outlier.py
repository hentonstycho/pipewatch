"""Tests for pipewatch.outlier."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.outlier import OutlierResult, detect_outliers, _mean, _std, _z_score


def _m(pipeline: str, row_count=100.0, error_rate=0.01, latency=1.0) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline=pipeline,
        avg_row_count=row_count,
        avg_error_rate=error_rate,
        avg_latency_seconds=latency,
        total_checks=10,
        failure_rate=0.0,
    )


def test_mean_basic():
    assert _mean([1.0, 2.0, 3.0]) == pytest.approx(2.0)


def test_std_basic():
    mu = _mean([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
    sigma = _std([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0], mu)
    assert sigma == pytest.approx(2.0)


def test_z_score_zero_std():
    assert _z_score(5.0, 5.0, 0.0) == 0.0


def test_detect_outliers_empty_returns_empty():
    assert detect_outliers([]) == []


def test_detect_outliers_single_pipeline_returns_empty():
    results = detect_outliers([_m("pipe_a")])
    assert results == []


def test_detect_outliers_no_outliers_when_similar():
    metrics = [_m(f"pipe_{i}", row_count=100.0 + i) for i in range(5)]
    results = detect_outliers(metrics, threshold=2.0)
    assert results == []


def test_detect_outliers_finds_row_count_outlier():
    metrics = [
        _m("normal_1", row_count=100.0),
        _m("normal_2", row_count=102.0),
        _m("normal_3", row_count=98.0),
        _m("normal_4", row_count=101.0),
        _m("spike", row_count=500.0),
    ]
    results = detect_outliers(metrics, threshold=2.0)
    pipelines = [r.pipeline for r in results]
    assert "spike" in pipelines


def test_detect_outliers_result_fields():
    metrics = [
        _m("a", latency=1.0),
        _m("b", latency=1.1),
        _m("c", latency=1.0),
        _m("d", latency=1.05),
        _m("outlier", latency=50.0),
    ]
    results = detect_outliers(metrics, threshold=1.5)
    r = next(x for x in results if x.pipeline == "outlier" and x.field == "avg_latency_seconds")
    assert r.value == pytest.approx(50.0)
    assert r.z_score > 1.5


def test_outlier_result_str():
    r = OutlierResult(pipeline="p", field="avg_row_count", value=500.0, mean=100.0, std=10.0, z_score=40.0)
    s = str(r)
    assert "p" in s
    assert "avg_row_count" in s
    assert "40.00" in s


def test_detect_outliers_none_values_skipped():
    metrics = [
        PipelineMetrics(pipeline="a", avg_row_count=None, avg_error_rate=0.01, avg_latency_seconds=1.0, total_checks=5, failure_rate=0.0),
        _m("b"),
        _m("c"),
    ]
    # Should not raise even with None row_count
    results = detect_outliers(metrics, threshold=2.0)
    assert isinstance(results, list)
