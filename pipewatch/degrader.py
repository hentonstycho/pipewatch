"""Degradation detector: flags pipelines whose health score has declined
over a rolling window compared to an earlier baseline window."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.config import PipewatchConfig
from pipewatch.history import load_history


@dataclass
class DegradationResult:
    pipeline: str
    recent_failure_rate: float   # 0.0 – 1.0
    baseline_failure_rate: float # 0.0 – 1.0
    delta: float                 # recent - baseline  (positive = worse)
    degraded: bool

    def __str__(self) -> str:
        direction = "degraded" if self.degraded else "stable"
        return (
            f"{self.pipeline}: {direction} "
            f"(baseline={self.baseline_failure_rate:.1%}, "
            f"recent={self.recent_failure_rate:.1%}, "
            f"delta={self.delta:+.1%})"
        )


def _failure_rate(entries: list) -> float:
    if not entries:
        return 0.0
    return sum(1 for e in entries if not e.get("healthy", True)) / len(entries)


def detect_degradation(
    pipeline: str,
    *,
    history_dir: str = ".pipewatch/history",
    window: int = 10,
    threshold: float = 0.20,
) -> Optional[DegradationResult]:
    """Compare the most-recent *window* runs against the previous *window* runs.

    Returns ``None`` when there is insufficient history (<= window entries).
    *threshold* is the minimum increase in failure-rate that counts as degradation.
    """
    entries = load_history(pipeline, history_dir=history_dir)
    if len(entries) <= window:
        return None

    recent = entries[-window:]
    baseline = entries[-2 * window : -window]

    r_rate = _failure_rate(recent)
    b_rate = _failure_rate(baseline)
    delta = r_rate - b_rate

    return DegradationResult(
        pipeline=pipeline,
        recent_failure_rate=r_rate,
        baseline_failure_rate=b_rate,
        delta=delta,
        degraded=delta >= threshold,
    )


def detect_all_degradations(
    config: PipewatchConfig,
    *,
    history_dir: str = ".pipewatch/history",
    window: int = 10,
    threshold: float = 0.20,
) -> List[DegradationResult]:
    results: List[DegradationResult] = []
    for pipeline in config.pipelines:
        result = detect_degradation(
            pipeline.name,
            history_dir=history_dir,
            window=window,
            threshold=threshold,
        )
        if result is not None:
            results.append(result)
    return results
