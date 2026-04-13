"""Tests for pipewatch.comparator."""
from pipewatch.metrics import PipelineMetrics
from pipewatch.comparator import (
    compare_metrics,
    format_comparison,
    MetricDiff,
    ComparisonReport,
)


def _metrics(
    avg_row_count=100.0,
    avg_error_rate=0.01,
    avg_latency_seconds=5.0,
    failure_rate=0.0,
    total_checks=10,
) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline="test_pipe",
        avg_row_count=avg_row_count,
        avg_error_rate=avg_error_rate,
        avg_latency_seconds=avg_latency_seconds,
        failure_rate=failure_rate,
        total_checks=total_checks,
    )


def test_compare_metrics_returns_comparison_report():
    before = _metrics()
    after = _metrics(avg_row_count=200.0)
    report = compare_metrics("test_pipe", before, after)
    assert isinstance(report, ComparisonReport)
    assert report.pipeline == "test_pipe"


def test_compare_metrics_diffs_all_fields():
    before = _metrics()
    after = _metrics()
    report = compare_metrics("test_pipe", before, after)
    metric_names = {d.metric for d in report.diffs}
    assert "avg_row_count" in metric_names
    assert "avg_error_rate" in metric_names
    assert "avg_latency_seconds" in metric_names
    assert "failure_rate" in metric_names
    assert "total_checks" in metric_names


def test_metric_diff_delta_computed_correctly():
    diff = MetricDiff(pipeline="p", metric="avg_row_count", before=100.0, after=150.0)
    assert diff.delta == 50.0


def test_metric_diff_pct_change():
    diff = MetricDiff(pipeline="p", metric="avg_row_count", before=100.0, after=150.0)
    assert diff.pct_change == 50.0


def test_metric_diff_pct_change_none_when_before_zero():
    diff = MetricDiff(pipeline="p", metric="avg_row_count", before=0.0, after=10.0)
    assert diff.pct_change is None


def test_metric_diff_delta_none_when_missing():
    diff = MetricDiff(pipeline="p", metric="avg_row_count", before=None, after=10.0)
    assert diff.delta is None


def test_has_changes_true_when_values_differ():
    before = _metrics(avg_row_count=100.0)
    after = _metrics(avg_row_count=200.0)
    report = compare_metrics("test_pipe", before, after)
    assert report.has_changes is True


def test_has_changes_false_when_identical():
    before = _metrics()
    after = _metrics()
    report = compare_metrics("test_pipe", before, after)
    assert report.has_changes is False


def test_format_comparison_contains_pipeline_name():
    before = _metrics()
    after = _metrics(avg_row_count=200.0)
    report = compare_metrics("test_pipe", before, after)
    text = format_comparison(report)
    assert "test_pipe" in text


def test_format_comparison_shows_arrow():
    before = _metrics()
    after = _metrics(avg_row_count=200.0)
    report = compare_metrics("test_pipe", before, after)
    text = format_comparison(report)
    assert "->" in text


def test_format_comparison_shows_pct_change():
    before = _metrics(avg_row_count=100.0)
    after = _metrics(avg_row_count=200.0)
    report = compare_metrics("test_pipe", before, after)
    text = format_comparison(report)
    assert "+100.0%" in text
