"""Track metric diffs between consecutive checks and flag regressions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class MetricRegression:
    pipeline: str
    field: str
    previous: float
    current: float
    pct_change: float
    regressed: bool

    def __str__(self) -> str:
        direction = "▲" if self.pct_change > 0 else "▼"
        sign = "+" if self.pct_change >= 0 else ""
        return (
            f"{self.pipeline} [{self.field}]: "
            f"{self.previous:.3f} → {self.current:.3f} "
            f"({direction}{sign}{self.pct_change:.1f}%)"
        )


_REGRESSION_FIELDS = {
    "error_rate": True,   # higher is worse
    "avg_latency": True,  # higher is worse
    "success_rate": False, # lower is worse
    "avg_row_count": False, # lower is worse
}


def _pct(previous: float, current: float) -> float:
    if previous == 0.0:
        return 0.0
    return ((current - previous) / previous) * 100.0


def diff_metrics(
    previous: PipelineMetrics,
    current: PipelineMetrics,
    regression_threshold_pct: float = 10.0,
) -> list[MetricRegression]:
    """Compare two PipelineMetrics snapshots and return regression entries."""
    results: list[MetricRegression] = []

    for field, higher_is_worse in _REGRESSION_FIELDS.items():
        prev_val: Optional[float] = getattr(previous, field, None)
        curr_val: Optional[float] = getattr(current, field, None)

        if prev_val is None or curr_val is None:
            continue

        change = _pct(prev_val, curr_val)
        regressed = (
            change > regression_threshold_pct
            if higher_is_worse
            else change < -regression_threshold_pct
        )

        results.append(
            MetricRegression(
                pipeline=current.pipeline,
                field=field,
                previous=prev_val,
                current=curr_val,
                pct_change=change,
                regressed=regressed,
            )
        )

    return results


def any_regressions(diffs: list[MetricRegression]) -> bool:
    """Return True if any entry in *diffs* is marked as a regression."""
    return any(d.regressed for d in diffs)


def filter_regressions(diffs: list[MetricRegression]) -> list[MetricRegression]:
    """Return only the entries from *diffs* that are marked as regressions.

    Useful for reporting or alerting when you only care about the fields
    that actually crossed the regression threshold.
    """
    return [d for d in diffs if d.regressed]
