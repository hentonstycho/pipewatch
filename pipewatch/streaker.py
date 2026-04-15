"""streaker.py – track and report consecutive-success streaks per pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_history
from pipewatch.config import PipewatchConfig


@dataclass
class StreakResult:
    pipeline: str
    current_streak: int          # consecutive successes (0 if last run failed)
    best_streak: int             # longest ever consecutive-success run
    last_status: Optional[str]   # "ok" | "fail" | None when no history


def _compute_streak(pipeline: str, history_dir: str) -> StreakResult:
    """Compute current and best success streak for a single pipeline."""
    entries = load_history(pipeline, history_dir=history_dir)

    if not entries:
        return StreakResult(
            pipeline=pipeline,
            current_streak=0,
            best_streak=0,
            last_status=None,
        )

    current = 0
    best = 0
    running = 0

    for entry in entries:  # oldest-first order from load_history
        if entry.get("healthy", False):
            running += 1
            if running > best:
                best = running
        else:
            running = 0

    current = running  # running at end of list = current streak

    last_status = "ok" if entries[-1].get("healthy", False) else "fail"

    return StreakResult(
        pipeline=pipeline,
        current_streak=current,
        best_streak=best,
        last_status=last_status,
    )


def compute_all_streaks(
    config: PipewatchConfig,
    history_dir: str = ".pipewatch/history",
) -> List[StreakResult]:
    """Return streak results for every pipeline in *config*."""
    return [
        _compute_streak(p.name, history_dir=history_dir)
        for p in config.pipelines
    ]
