"""Tests for pipewatch.budgeter."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.budgeter import BudgetResult, _success_rate, compute_budget, compute_all_budgets
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


)
def hist_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _write_history(hist_dir: str, name: str, entries: list[dict]):
    path = Path(hist_dir) / f"{name}.jsonl"
    with path.open("w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def test_success_rate_all_healthy():
    entries = [{"healthy": True}] * 10
    total, failed, rate = _success_rate(entries)
    assert total == 10
    assert failed == 0
    assert rate == 1.0


def test_success_rate_mixed():
    entries = [{"healthy": True}] * 8 + [{"healthy": False}] * 2
    total, failed, rate = _success_rate(entries)
    assert total == 10
    assert failed == 2
    assert abs(rate - 0.8) < 1e-9


def test_success_rate_empty():
    total, failed, rate = _success_rate([])
    assert total == 0
    assert rate == 1.0


def test_compute_budget_ok(hist_dir):
    _write_history(hist_dir, "pipe_a", [{"healthy": True}] * 10)
    result = compute_budget("pipe_a", slo_target=0.95, history_dir=hist_dir)
    assert not result.exhausted
    assert result.actual_rate == 1.0
    assert result.budget_remaining > 0


def test_compute_budget_exhausted(hist_dir):
    entries = [{"healthy": True}] * 7 + [{"healthy": False}] * 3
    _write_history(hist_dir, "pipe_b", entries)
    result = compute_budget("pipe_b", slo_target=0.95, history_dir=hist_dir)
    assert result.exhausted
    assert result.budget_remaining < 0


def test_compute_budget_window(hist_dir):
    # First 5 ok, last 5 all failing — window=5 should exhaust budget
    entries = [{"healthy": True}] * 5 + [{"healthy": False}] * 5
    _write_history(hist_dir, "pipe_c", entries)
    result = compute_budget("pipe_c", slo_target=0.95, history_dir=hist_dir, window=5)
    assert result.exhausted
    assert result.total_runs == 5


def test_compute_all_budgets(hist_dir):
    _write_history(hist_dir, "alpha", [{"healthy": True}] * 5)
    _write_history(hist_dir, "beta", [{"healthy": False}] * 5)
    cfg = PipewatchConfig(
        pipelines=[
            PipelineConfig(name="alpha", thresholds=ThresholdConfig(), source=""),
            PipelineConfig(name="beta", thresholds=ThresholdConfig(), source=""),
        ],
        notifications=NotificationConfig(),
    )
    results = compute_all_budgets(cfg, history_dir=hist_dir)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"alpha", "beta"}


def test_budget_result_str_exhausted(hist_dir):
    r = BudgetResult(
        pipeline="p", slo_target=0.95, actual_rate=0.80,
        budget_remaining=-0.15, total_runs=20, failed_runs=4, exhausted=True,
    )
    assert "EXHAUSTED" in str(r)
    assert "p" in str(r)
