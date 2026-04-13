"""Pipeline health checker — evaluates metrics against configured thresholds."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pipewatch.config import PipelineConfig, ThresholdConfig


@dataclass
class CheckResult:
    pipeline_name: str
    status: str  # "ok", "warning", "critical"
    violations: list[str] = field(default_factory=list)
    checked_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def is_healthy(self) -> bool:
        return self.status == "ok"


def check_pipeline(pipeline: PipelineConfig, metrics: dict) -> CheckResult:
    """Evaluate pipeline metrics against its thresholds.

    Args:
        pipeline: The pipeline configuration with thresholds.
        metrics: A dict of current metric values, e.g.:
                 {"row_count": 500, "error_rate": 0.03, "latency_seconds": 120}

    Returns:
        A CheckResult describing the health status.
    """
    violations: list[str] = []
    thresholds: ThresholdConfig = pipeline.thresholds

    row_count: Optional[int] = metrics.get("row_count")
    error_rate: Optional[float] = metrics.get("error_rate")
    latency: Optional[float] = metrics.get("latency_seconds")

    if thresholds.min_rows is not None and row_count is not None:
        if row_count < thresholds.min_rows:
            violations.append(
                f"row_count {row_count} is below min_rows {thresholds.min_rows}"
            )

    if thresholds.max_error_rate is not None and error_rate is not None:
        if error_rate > thresholds.max_error_rate:
            violations.append(
                f"error_rate {error_rate:.4f} exceeds max_error_rate {thresholds.max_error_rate}"
            )

    if thresholds.max_latency_seconds is not None and latency is not None:
        if latency > thresholds.max_latency_seconds:
            violations.append(
                f"latency_seconds {latency} exceeds max_latency_seconds {thresholds.max_latency_seconds}"
            )

    if violations:
        status = "critical"
    else:
        status = "ok"

    return CheckResult(
        pipeline_name=pipeline.name,
        status=status,
        violations=violations,
    )


def check_all_pipelines(pipelines: list[PipelineConfig], metrics_map: dict[str, dict]) -> list[CheckResult]:
    """Run health checks for all configured pipelines.

    Args:
        pipelines: List of pipeline configurations.
        metrics_map: Dict mapping pipeline name to its current metrics dict.

    Returns:
        List of CheckResult objects, one per pipeline.
    """
    results = []
    for pipeline in pipelines:
        metrics = metrics_map.get(pipeline.name, {})
        results.append(check_pipeline(pipeline, metrics))
    return results
