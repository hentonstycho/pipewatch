"""Aggregate a single 0-100 health score for each pipeline from recent history."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.config import PipewatchConfig
from pipewatch.history import load_history


@dataclass
class HealthScore:
    pipeline: str
    score: int          # 0-100
    grade: str          # A/B/C/D/F
    total: int
    failures: int
    avg_latency: Optional[float]

    def __str__(self) -> str:
        return f"{self.pipeline}: {self.score}/100 ({self.grade})"


def _grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def _avg(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def compute_health_score(
    pipeline_name: str,
    history_dir: str = ".pipewatch/history",
    window: int = 50,
) -> HealthScore:
    """Compute health score from the last *window* history entries."""
    entries = load_history(pipeline_name, history_dir=history_dir)
    recent = entries[-window:] if len(entries) > window else entries

    total = len(recent)
    if total == 0:
        return HealthScore(
            pipeline=pipeline_name, score=100, grade="A",
            total=0, failures=0, avg_latency=None,
        )

    failures = sum(1 for e in recent if not e.get("healthy", True))
    success_rate = (total - failures) / total

    latencies = [e["latency_seconds"] for e in recent if "latency_seconds" in e]
    avg_latency = _avg(latencies)

    score = int(round(success_rate * 100))
    return HealthScore(
        pipeline=pipeline_name,
        score=score,
        grade=_grade(score),
        total=total,
        failures=failures,
        avg_latency=avg_latency,
    )


def compute_all_health_scores(
    config: PipewatchConfig,
    history_dir: str = ".pipewatch/history",
    window: int = 50,
) -> List[HealthScore]:
    return [
        compute_health_score(p.name, history_dir=history_dir, window=window)
        for p in config.pipelines
    ]
