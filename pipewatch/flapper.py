"""Flap detection: identify pipelines that oscillate between healthy and failing."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

from pipewatch.config import PipewatchConfig
from pipewatch.history import load_history


@dataclass
class FlapResult:
    pipeline: str
    transitions: int
    is_flapping: bool
    recent_statuses: List[bool]  # True = healthy


def _count_transitions(statuses: List[bool]) -> int:
    """Count the number of healthy<->failing transitions in a status list."""
    return sum(1 for a, b in zip(statuses, statuses[1:]) if a != b)


def detect_flap(
    pipeline: str,
    history_dir: Path,
    window: int = 10,
    threshold: int = 4,
) -> FlapResult:
    """Return a FlapResult for *pipeline* based on the last *window* results.

    A pipeline is considered flapping when the number of healthy/failing
    transitions within the window meets or exceeds *threshold*.
    """
    records = load_history(pipeline, history_dir=history_dir)
    recent = records[-window:] if len(records) >= window else records
    statuses = [r.get("healthy", True) for r in recent]
    transitions = _count_transitions(statuses)
    return FlapResult(
        pipeline=pipeline,
        transitions=transitions,
        is_flapping=transitions >= threshold,
        recent_statuses=statuses,
    )


def detect_all_flaps(
    config: PipewatchConfig,
    history_dir: Path,
    window: int = 10,
    threshold: int = 4,
) -> List[FlapResult]:
    """Run flap detection for every pipeline in *config*."""
    return [
        detect_flap(p.name, history_dir, window=window, threshold=threshold)
        for p in config.pipelines
    ]
