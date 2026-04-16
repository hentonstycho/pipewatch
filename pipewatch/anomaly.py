"""Anomaly detection: flag pipelines whose recent failure rate spikes above a rolling baseline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_history
from pipewatch.config import PipewatchConfig


@dataclass
class AnomalyResult:
    pipeline: str
    baseline_failure_rate: float  # rolling average over baseline_window
    recent_failure_rate: float    # average over recent_window
    spike_ratio: float            # recent / baseline (inf when baseline == 0)
    is_anomaly: bool

    def __str__(self) -> str:
        status = "ANOMALY" if self.is_anomaly else "ok"
        return (
            f"{self.pipeline}: {status} "
            f"(baseline={self.baseline_failure_rate:.2%}, "
            f"recent={self.recent_failure_rate:.2%}, "
            f"ratio={self.spike_ratio:.2f})"
        )


def _failure_rate(results: list) -> float:
    if not results:
        return 0.0
    return sum(1 for r in results if not r.get("healthy", True)) / len(results)


def detect_anomaly(
    pipeline: str,
    *,
    history_dir: str = ".pipewatch/history",
    baseline_window: int = 20,
    recent_window: int = 5,
    spike_threshold: float = 2.0,
) -> AnomalyResult:
    """Compare recent failure rate to a rolling baseline for one pipeline."""
    entries = load_history(pipeline, history_dir=history_dir)
    baseline_entries = entries[-baseline_window:] if len(entries) >= baseline_window else entries
    recent_entries = entries[-recent_window:] if len(entries) >= recent_window else entries

    baseline_rate = _failure_rate(baseline_entries)
    recent_rate = _failure_rate(recent_entries)

    if baseline_rate == 0.0:
        ratio = float("inf") if recent_rate > 0 else 1.0
    else:
        ratio = recent_rate / baseline_rate

    return AnomalyResult(
        pipeline=pipeline,
        baseline_failure_rate=baseline_rate,
        recent_failure_rate=recent_rate,
        spike_ratio=ratio,
        is_anomaly=ratio >= spike_threshold and recent_rate > 0,
    )


def detect_all_anomalies(
    config: PipewatchConfig,
    *,
    history_dir: str = ".pipewatch/history",
    baseline_window: int = 20,
    recent_window: int = 5,
    spike_threshold: float = 2.0,
) -> List[AnomalyResult]:
    return [
        detect_anomaly(
            p.name,
            history_dir=history_dir,
            baseline_window=baseline_window,
            recent_window=recent_window,
            spike_threshold=spike_threshold,
        )
        for p in config.pipelines
    ]
