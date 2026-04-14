"""Aggregate check results across pipelines into rollup statistics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import CheckResult
from pipewatch.history import load_history
from pipewatch.config import PipewatchConfig


@dataclass
class RollupStats:
    total_pipelines: int = 0
    healthy_pipelines: int = 0
    degraded_pipelines: int = 0
    total_checks: int = 0
    total_failures: int = 0
    failure_rate: float = 0.0
    most_failing: Optional[str] = None
    pipelines: List[str] = field(default_factory=list)


def _failure_count(results: List[CheckResult]) -> int:
    return sum(1 for r in results if not r.healthy)


def aggregate(cfg: PipewatchConfig, history_dir: str = ".pipewatch/history") -> RollupStats:
    """Build a RollupStats across all configured pipelines."""
    stats = RollupStats()
    stats.total_pipelines = len(cfg.pipelines)
    stats.pipelines = [p.name for p in cfg.pipelines]

    worst_name: Optional[str] = None
    worst_count: int = 0

    for pipeline in cfg.pipelines:
        results = load_history(pipeline.name, history_dir=history_dir)
        failures = _failure_count(results)
        stats.total_checks += len(results)
        stats.total_failures += failures

        if failures > worst_count:
            worst_count = failures
            worst_name = pipeline.name

        last = results[-1] if results else None
        if last is not None and last.healthy:
            stats.healthy_pipelines += 1
        elif last is not None:
            stats.degraded_pipelines += 1

    stats.failure_rate = (
        stats.total_failures / stats.total_checks if stats.total_checks else 0.0
    )
    stats.most_failing = worst_name if worst_count > 0 else None
    return stats
