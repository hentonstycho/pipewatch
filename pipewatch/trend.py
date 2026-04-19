"""Trend analysis: classify pipeline metric trends over recent history."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_history
from pipewatch.config import PipewatchConfig


@dataclass
class TrendResult:
    pipeline: str
    direction: str          # 'improving', 'degrading', 'stable', 'insufficient_data'
    slope: Optional[float]  # failures per run (linear)
    window: int             # number of runs analysed

    def __str__(self) -> str:
        icon = {"improving": "↑", "degrading": "↓", "stable": "→", "insufficient_data": "?"}[
            self.direction
        ]
        return f"{self.pipeline}: {icon} {self.direction} (slope={self.slope:.3f}, n={self.window})"


def _slope(values: List[float]) -> float:
    n = len(values)
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    den = sum((x - x_mean) ** 2 for x in xs)
    return num / den if den else 0.0


def analyse_trend(pipeline: str, history_dir: str, window: int = 20) -> TrendResult:
    entries = load_history(pipeline, history_dir=history_dir)
    recent = entries[-window:]
    if len(recent) < 3:
        return TrendResult(pipeline=pipeline, direction="insufficient_data", slope=None, window=len(recent))

    failures = [0.0 if e.get("healthy") else 1.0 for e in recent]
    s = _slope(failures)

    if s > 0.05:
        direction = "degrading"
    elif s < -0.05:
        direction = "improving"
    else:
        direction = "stable"

    return TrendResult(pipeline=pipeline, direction=direction, slope=s, window=len(recent))


def analyse_all_trends(config: PipewatchConfig, history_dir: str, window: int = 20) -> List[TrendResult]:
    return [analyse_trend(p.name, history_dir=history_dir, window=window) for p in config.pipelines]
