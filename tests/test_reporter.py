"""Tests for pipewatch.reporter."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.checker import CheckResult
from pipewatch.reporter import PipelineSummary, Report, build_report, format_report, _summarise


TS = "2024-01-15T10:00:00"


def _result(healthy: bool, pipeline: str = "orders") -> CheckResult:
    return CheckResult(
        pipeline_name=pipeline,
        healthy=healthy,
        violations=["err"] if not healthy else [],
        checked_at=TS,
    )


# --- _summarise ---

def test_summarise_all_ok():
    results = [_result(True)] * 5
    s = _summarise("orders", results)
    assert s.total_checks == 5
    assert s.failures == 0
    assert s.success_rate == 100.0
    assert s.last_status == "OK"


def test_summarise_some_failures():
    results = [_result(True), _result(False), _result(False)]
    s = _summarise("orders", results)
    assert s.failures == 2
    assert s.success_rate == pytest.approx(33.3, rel=1e-2)
    assert s.last_status == "FAIL"


def test_summarise_empty():
    s = _summarise("orders", [])
    assert s.total_checks == 0
    assert s.success_rate == 0.0
    assert s.last_status is None
    assert s.last_checked is None


def test_summarise_last_checked_parsed():
    results = [_result(True)]
    s = _summarise("orders", results)
    assert isinstance(s.last_checked, datetime)


# --- build_report ---

def test_build_report_calls_load_history():
    fake_results = [_result(True), _result(False)]
    with patch("pipewatch.reporter.load_history", return_value=fake_results) as mock_load:
        report = build_report(["orders", "users"])
    assert mock_load.call_count == 2
    assert len(report.summaries) == 2


def test_build_report_overall_health_healthy():
    with patch("pipewatch.reporter.load_history", return_value=[_result(True)]):
        report = build_report(["orders"])
    assert report.overall_health == "healthy"


def test_build_report_overall_health_degraded():
    with patch("pipewatch.reporter.load_history", return_value=[_result(False)]):
        report = build_report(["orders"])
    assert report.overall_health == "degraded"


# --- format_report ---

def test_format_report_contains_pipeline_name():
    summary = PipelineSummary(
        pipeline_name="orders",
        total_checks=3,
        failures=1,
        success_rate=66.7,
        last_checked=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
        last_status="FAIL",
    )
    report = Report(generated_at=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc), summaries=[summary])
    text = format_report(report)
    assert "orders" in text
    assert "FAIL" in text
    assert "66.7%" in text
    assert "DEGRADED" in text
