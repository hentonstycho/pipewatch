"""Classify pipelines into health tiers based on recent check history."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.config import PipewatchConfig
from pipewatch.history import load_history


TIER_CRITICAL = "critical"
TIER_DEGRADED = "degraded"
TIER_WARNING = "warning"
TIER_HEALTHY = "healthy"


@dataclass
class ClassificationResult:
    pipeline: str
    tier: str
    failure_rate: float
    sample_size: int

    def __str__(self) -> str:
        return f"{self.pipeline}: {self.tier} ({self.failure_rate:.0%} failure rate, n={self.sample_size})"


def _tier(failure_rate: float) -> str:
    if failure_rate >= 0.75:
        return TIER_CRITICAL
    if failure_rate >= 0.50:
        return TIER_DEGRADED
    if failure_rate >= 0.25:
        return TIER_WARNING
    return TIER_HEALTHY


def classify_pipeline(
    pipeline_name: str,
    history_dir: str = ".pipewatch/history",
    window: int = 20,
) -> ClassificationResult:
    entries = load_history(pipeline_name, history_dir=history_dir)
    recent = entries[-window:] if len(entries) > window else entries
    if not recent:
        return ClassificationResult(
            pipeline=pipeline_name, tier=TIER_HEALTHY, failure_rate=0.0, sample_size=0
        )
    failures = sum(1 for e in recent if not e.get("healthy", True))
    rate = failures / len(recent)
    return ClassificationResult(
        pipeline=pipeline_name,
        tier=_tier(rate),
        failure_rate=rate,
        sample_size=len(recent),
    )


def classify_all(
    config: PipewatchConfig,
    history_dir: str = ".pipewatch/history",
    window: int = 20,
) -> List[ClassificationResult]:
    return [
        classify_pipeline(p.name, history_dir=history_dir, window=window)
        for p in config.pipelines
    ]
