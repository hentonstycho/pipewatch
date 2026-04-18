"""Error budget tracking: computes remaining error budget based on SLO targets."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.config import PipewatchConfig
from pipewatch.history import load_history


@dataclass
class BudgetResult:
    pipeline: str
    slo_target: float          # e.g. 0.95 = 95% success rate
    actual_rate: float
    budget_remaining: float    # fraction remaining (can be negative)
    total_runs: int
    failed_runs: int
    exhausted: bool

    def __str__(self) -> str:
        status = "EXHAUSTED" if self.exhausted else "OK"
        return (
            f"{self.pipeline}: SLO={self.slo_target:.0%} "
            f"actual={self.actual_rate:.1%} "
            f"budget_remaining={self.budget_remaining:+.1%} [{status}]"
        )


def _success_rate(entries: list) -> tuple[int, int, float]:
    if not entries:
        return 0, 0, 1.0
    total = len(entries)
    failed = sum(1 for e in entries if not e.get("healthy", True))
    return total, failed, (total - failed) / total


def compute_budget(
    pipeline_name: str,
    slo_target: float,
    history_dir: str = ".pipewatch/history",
    window: Optional[int] = None,
) -> BudgetResult:
    entries = load_history(pipeline_name, history_dir=history_dir)
    if window is not None:
        entries = entries[-window:]
    total, failed, actual_rate = _success_rate(entries)
    budget_remaining = actual_rate - slo_target
    return BudgetResult(
        pipeline=pipeline_name,
        slo_target=slo_target,
        actual_rate=actual_rate,
        budget_remaining=budget_remaining,
        total_runs=total,
        failed_runs=failed,
        exhausted=budget_remaining < 0,
    )


def compute_all_budgets(
    config: PipewatchConfig,
    history_dir: str = ".pipewatch/history",
    window: Optional[int] = None,
) -> List[BudgetResult]:
    results = []
    for pipeline in config.pipelines:
        slo = getattr(pipeline.thresholds, "slo_target", None) or 0.95
        results.append(
            compute_budget(pipeline.name, slo, history_dir=history_dir, window=window)
        )
    return results
