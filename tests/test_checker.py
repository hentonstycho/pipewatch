"""Tests for pipewatch.checker module."""

import pytest

from pipewatch.checker import CheckResult, check_all_pipelines, check_pipeline
from pipewatch.config import PipelineConfig, ThresholdConfig


@pytest.fixture
def pipeline() -> PipelineConfig:
    return PipelineConfig(
        name="orders_etl",
        thresholds=ThresholdConfig(
            min_rows=100,
            max_error_rate=0.05,
            max_latency_seconds=300,
        ),
    )


def test_check_pipeline_ok(pipeline):
    metrics = {"row_count": 500, "error_rate": 0.01, "latency_seconds": 120}
    result = check_pipeline(pipeline, metrics)
    assert result.status == "ok"
    assert result.is_healthy is True
    assert result.violations == []
    assert result.pipeline_name == "orders_etl"


def test_check_pipeline_row_count_violation(pipeline):
    metrics = {"row_count": 50, "error_rate": 0.01, "latency_seconds": 120}
    result = check_pipeline(pipeline, metrics)
    assert result.status == "critical"
    assert any("row_count" in v for v in result.violations)


def test_check_pipeline_error_rate_violation(pipeline):
    metrics = {"row_count": 500, "error_rate": 0.10, "latency_seconds": 120}
    result = check_pipeline(pipeline, metrics)
    assert result.status == "critical"
    assert any("error_rate" in v for v in result.violations)


def test_check_pipeline_latency_violation(pipeline):
    metrics = {"row_count": 500, "error_rate": 0.01, "latency_seconds": 600}
    result = check_pipeline(pipeline, metrics)
    assert result.status == "critical"
    assert any("latency_seconds" in v for v in result.violations)


def test_check_pipeline_multiple_violations(pipeline):
    metrics = {"row_count": 10, "error_rate": 0.99, "latency_seconds": 9999}
    result = check_pipeline(pipeline, metrics)
    assert result.status == "critical"
    assert len(result.violations) == 3


def test_check_pipeline_missing_metrics_no_violation(pipeline):
    """Missing metrics should not trigger violations."""
    result = check_pipeline(pipeline, {})
    assert result.status == "ok"
    assert result.violations == []


def test_check_pipeline_none_threshold_skipped():
    """Thresholds set to None should never trigger violations."""
    p = PipelineConfig(
        name="no_thresholds",
        thresholds=ThresholdConfig(min_rows=None, max_error_rate=None, max_latency_seconds=None),
    )
    metrics = {"row_count": 0, "error_rate": 1.0, "latency_seconds": 99999}
    result = check_pipeline(p, metrics)
    assert result.is_healthy is True


def test_check_all_pipelines(pipeline):
    pipelines = [
        pipeline,
        PipelineConfig(
            name="users_etl",
            thresholds=ThresholdConfig(min_rows=10, max_error_rate=None, max_latency_seconds=None),
        ),
    ]
    metrics_map = {
        "orders_etl": {"row_count": 500, "error_rate": 0.01, "latency_seconds": 100},
        "users_etl": {"row_count": 5},
    }
    results = check_all_pipelines(pipelines, metrics_map)
    assert len(results) == 2
    orders_result = next(r for r in results if r.pipeline_name == "orders_etl")
    users_result = next(r for r in results if r.pipeline_name == "users_etl")
    assert orders_result.is_healthy is True
    assert users_result.is_healthy is False
