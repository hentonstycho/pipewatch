"""sentinel.py – detect pipelines that have never successfully run.

A pipeline is considered "never healthy" if its entire recorded history
contains no passing CheckResult.  This is distinct from staleness (which
only cares about recency) and from degradation (which needs a baseline of
healthy runs).  Sentinel catches brand-new or permanently broken pipelines
that have never produced a green result.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from pipewatch.config import PipewatchConfig


@dataclass
class SentinelResult:
    pipeline: str
    total_runs: int
    healthy_runs: int
    triggered: bool  # True when zero healthy runs found

    def __str__(self) -> str:
        if self.triggered:
            return (
                f"{self.pipeline}: SENTINEL – no healthy run in "
                f"{self.total_runs} recorded attempt(s)"
            )
        return (
            f"{self.pipeline}: ok "
            f"({self.healthy_runs}/{self.total_runs} healthy)"
        )


def _history_path(history_dir: str, pipeline: str) -> Path:
    return Path(history_dir) / f"{pipeline}.jsonl"


def _count_runs(history_dir: str, pipeline: str) -> tuple[int, int]:
    """Return (total_runs, healthy_runs) for *pipeline*."""
    path = _history_path(history_dir, pipeline)
    if not path.exists():
        return 0, 0
    total = 0
    healthy = 0
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        total += 1
        if entry.get("healthy", False):
            healthy += 1
    return total, healthy


def check_sentinel(
    pipeline: str,
    history_dir: str = ".pipewatch/history",
) -> SentinelResult:
    """Return a SentinelResult for a single pipeline."""
    total, healthy = _count_runs(history_dir, pipeline)
    return SentinelResult(
        pipeline=pipeline,
        total_runs=total,
        healthy_runs=healthy,
        triggered=(total > 0 and healthy == 0),
    )


def check_all_sentinels(
    config: PipewatchConfig,
    history_dir: str = ".pipewatch/history",
) -> List[SentinelResult]:
    """Return SentinelResult for every pipeline in *config*."""
    return [
        check_sentinel(p.name, history_dir=history_dir)
        for p in config.pipelines
    ]
