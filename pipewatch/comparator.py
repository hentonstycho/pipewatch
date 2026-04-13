"""Compare two snapshots or baselines and return a structured diff."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class MetricDiff:
    pipeline: str
    metric: str
    before: Optional[float]
    after: Optional[float]

    @property
    def delta(self) -> Optional[float]:
        if self.before is None or self.after is None:
            return None
        return self.after - self.before

    @property
    def pct_change(self) -> Optional[float]:
        if self.before is None or self.after is None:
            return None
        if self.before == 0:
            return None
        return (self.after - self.before) / abs(self.before) * 100


@dataclass
class ComparisonReport:
    pipeline: str
    diffs: list[MetricDiff]

    @property
    def has_changes(self) -> bool:
        return any(d.delta != 0 for d in self.diffs if d.delta is not None)


_METRIC_FIELDS = (
    "avg_row_count",
    "avg_error_rate",
    "avg_latency_seconds",
    "failure_rate",
    "total_checks",
)


def compare_metrics(
    pipeline: str,
    before: PipelineMetrics,
    after: PipelineMetrics,
) -> ComparisonReport:
    """Return a ComparisonReport describing how metrics changed."""
    diffs: list[MetricDiff] = []
    for field in _METRIC_FIELDS:
        b_val = getattr(before, field, None)
        a_val = getattr(after, field, None)
        diffs.append(MetricDiff(pipeline=pipeline, metric=field, before=b_val, after=a_val))
    return ComparisonReport(pipeline=pipeline, diffs=diffs)


def format_comparison(report: ComparisonReport) -> str:
    """Return a human-readable text summary of a ComparisonReport."""
    lines = [f"Pipeline: {report.pipeline}"]
    for diff in report.diffs:
        if diff.before is None and diff.after is None:
            continue
        before_s = "n/a" if diff.before is None else f"{diff.before:.4g}"
        after_s = "n/a" if diff.after is None else f"{diff.after:.4g}"
        delta_s = ""
        if diff.pct_change is not None:
            sign = "+" if diff.pct_change >= 0 else ""
            delta_s = f"  ({sign}{diff.pct_change:.1f}%)"
        lines.append(f"  {diff.metric:<28} {before_s:>10} -> {after_s:>10}{delta_s}")
    return "\n".join(lines)
