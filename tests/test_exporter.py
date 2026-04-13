"""Tests for pipewatch.exporter."""
from __future__ import annotations

import math

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.exporter import render_metrics, _gauge


def _make_metrics(
    name: str = "pipe_a",
    success_rate: float | None = 0.9,
    avg_latency: float | None = 42.5,
    avg_error_rate: float | None = 0.05,
    total_checks: int = 10,
    consecutive_failures: int = 1,
) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name=name,
        success_rate=success_rate,
        avg_latency=avg_latency,
        avg_error_rate=avg_error_rate,
        total_checks=total_checks,
        consecutive_failures=consecutive_failures,
    )


def test_gauge_contains_help_and_type():
    line = _gauge("my_metric", "A test metric.", {"pipeline": "p1"}, 1.0)
    assert "# HELP pipewatch_my_metric" in line
    assert "# TYPE pipewatch_my_metric gauge" in line


def test_gauge_label_rendered():
    line = _gauge("x", "desc", {"pipeline": "alpha"}, 3.14)
    assert 'pipeline="alpha"' in line
    assert "3.14" in line


def test_render_metrics_contains_pipeline_name():
    m = _make_metrics(name="etl_orders")
    output = render_metrics([m])
    assert 'pipeline="etl_orders"' in output


def test_render_metrics_all_keys_present():
    m = _make_metrics()
    output = render_metrics([m])
    for key in ("success_rate", "avg_latency_seconds", "avg_error_rate",
                "total_checks", "consecutive_failures"):
        assert f"pipewatch_{key}" in output


def test_render_metrics_nan_for_none_values():
    m = _make_metrics(success_rate=None, avg_latency=None, avg_error_rate=None)
    output = render_metrics([m])
    assert "nan" in output.lower() or "NaN" in output


def test_render_metrics_multiple_pipelines():
    metrics = [_make_metrics(name="a"), _make_metrics(name="b")]
    output = render_metrics(metrics)
    assert 'pipeline="a"' in output
    assert 'pipeline="b"' in output


def test_render_metrics_eof_marker():
    output = render_metrics([])
    assert "# EOF" in output


def test_render_metrics_total_checks_value():
    m = _make_metrics(total_checks=7)
    output = render_metrics([m])
    assert "7.0" in output or "7" in output
