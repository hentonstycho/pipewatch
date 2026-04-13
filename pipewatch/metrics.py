"""Metrics collection and aggregation for pipeline check results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import CheckResult
from pipewatch.history import load_history


@dataclass
class PipelineMetrics:
    pipeline_name: str
    total_checks: int = 0
    total_failures: int = 0
    avg_row_count: Optional[float] = None
    avg_error_rate: Optional[float] = None
    avg_latency_seconds: Optional[float] = None
    uptime_pct: float = 100.0


def _average(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def compute_metrics(pipeline_name: str, history_dir: str = ".pipewatch") -> PipelineMetrics:
    """Compute aggregate metrics for a pipeline from its history."""
    records: List[CheckResult] = load_history(pipeline_name, history_dir=history_dir)

    if not records:
        return PipelineMetrics(pipeline_name=pipeline_name)

    total = len(records)
    failures = sum(1 for r in records if not r.healthy)

    row_counts = [r.row_count for r in records if r.row_count is not None]
    error_rates = [r.error_rate for r in records if r.error_rate is not None]
    latencies = [r.latency_seconds for r in records if r.latency_seconds is not None]

    uptime = ((total - failures) / total) * 100.0 if total else 100.0

    return PipelineMetrics(
        pipeline_name=pipeline_name,
        total_checks=total,
        total_failures=failures,
        avg_row_count=_average(row_counts),
        avg_error_rate=_average(error_rates),
        avg_latency_seconds=_average(latencies),
        uptime_pct=round(uptime, 2),
    )


def compute_all_metrics(
    pipeline_names: List[str], history_dir: str = ".pipewatch"
) -> List[PipelineMetrics]:
    """Compute metrics for every pipeline in the given list."""
    return [compute_metrics(name, history_dir=history_dir) for name in pipeline_names]
