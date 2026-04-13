"""Pipeline health checking logic."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.config import PipelineConfig, PipewatchConfig


@dataclass
class CheckResult:
    pipeline_name: str
    healthy: bool
    violations: List[str]
    row_count: Optional[float] = None
    error_rate: Optional[float] = None
    latency_seconds: Optional[float] = None
    checked_at: Optional[str] = None


def is_healthy(result: CheckResult) -> bool:
    return result.healthy


def check_pipeline(
    pipeline: PipelineConfig,
    row_count: Optional[float] = None,
    error_rate: Optional[float] = None,
    latency_seconds: Optional[float] = None,
    checked_at: Optional[str] = None,
) -> CheckResult:
    """Evaluate a single pipeline against its configured thresholds."""
    violations: List[str] = []
    t = pipeline.thresholds

    if t is not None:
        if t.min_row_count is not None and row_count is not None:
            if row_count < t.min_row_count:
                violations.append(
                    f"row_count {row_count} below min {t.min_row_count}"
                )
        if t.max_error_rate is not None and error_rate is not None:
            if error_rate > t.max_error_rate:
                violations.append(
                    f"error_rate {error_rate} above max {t.max_error_rate}"
                )
        if t.max_latency_seconds is not None and latency_seconds is not None:
            if latency_seconds > t.max_latency_seconds:
                violations.append(
                    f"latency {latency_seconds}s above max {t.max_latency_seconds}s"
                )

    return CheckResult(
        pipeline_name=pipeline.name,
        healthy=len(violations) == 0,
        violations=violations,
        row_count=row_count,
        error_rate=error_rate,
        latency_seconds=latency_seconds,
        checked_at=checked_at,
    )


def check_all_pipelines(
    config: PipewatchConfig,
    metrics: dict,
) -> List[CheckResult]:
    """Check every pipeline defined in config using provided metrics dict.

    ``metrics`` is keyed by pipeline name; values are dicts with optional
    keys: row_count, error_rate, latency_seconds, checked_at.
    """
    results = []
    for pipeline in config.pipelines:
        m = metrics.get(pipeline.name, {})
        results.append(
            check_pipeline(
                pipeline,
                row_count=m.get("row_count"),
                error_rate=m.get("error_rate"),
                latency_seconds=m.get("latency_seconds"),
                checked_at=m.get("checked_at"),
            )
        )
    return results
