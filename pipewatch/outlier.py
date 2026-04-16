"""Detect pipelines with metric values that are statistical outliers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class OutlierResult:
    pipeline: str
    field: str
    value: float
    mean: float
    std: float
    z_score: float

    def __str__(self) -> str:
        return (
            f"{self.pipeline} [{self.field}] value={self.value:.3f} "
            f"mean={self.mean:.3f} std={self.std:.3f} z={self.z_score:.2f}"
        )


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _std(values: List[float], mean: float) -> float:
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def _z_score(value: float, mean: float, std: float) -> float:
    if std == 0.0:
        return 0.0
    return (value - mean) / std


def detect_outliers(
    all_metrics: List[PipelineMetrics],
    threshold: float = 2.0,
) -> List[OutlierResult]:
    """Return outlier results for any pipeline whose metric z-score exceeds *threshold*."""
    results: List[OutlierResult] = []
    fields = ["avg_row_count", "avg_error_rate", "avg_latency_seconds"]

    for field in fields:
        values = [
            (m.pipeline, getattr(m, field))
            for m in all_metrics
            if getattr(m, field) is not None
        ]
        if len(values) < 2:
            continue
        raw = [v for _, v in values]
        mu = _mean(raw)
        sigma = _std(raw, mu)
        for pipeline, val in values:
            z = _z_score(val, mu, sigma)
            if abs(z) >= threshold:
                results.append(
                    OutlierResult(
                        pipeline=pipeline,
                        field=field,
                        value=val,
                        mean=mu,
                        std=sigma,
                        z_score=z,
                    )
                )
    return results
