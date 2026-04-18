"""Tests for pipewatch.cli_budget."""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_budget import budget_cmd
from pipewatch.budgeter import BudgetResult
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


@pytest.fixture()
def runner():
    return CliRunner()


def _make_cfg(names=("pipe_a", "pipe_b")):
    return PipewatchConfig(
        pipelines=[PipelineConfig(name=n, thresholds=ThresholdConfig(), source="") for n in names],
        notifications=NotificationConfig(),
    )


def _make_result(name: str, exhausted: bool) -> BudgetResult:
    rate = 0.80 if exhausted else 1.0
    return BudgetResult(
        pipeline=name, slo_target=0.95, actual_rate=rate,
        budget_remaining=rate - 0.95, total_runs=10, failed_runs=2 if exhausted else 0,
        exhausted=exhausted,
    )


def test_show_cmd_prints_output(runner):
    cfg = _make_cfg()
    results = [_make_result("pipe_a", False), _make_result("pipe_b", False)]
    with patch("pipewatch.cli_budget.load_config", return_value=cfg), \
         patch("pipewatch.cli_budget.compute_all_budgets", return_value=results):
        out = runner.invoke(budget_cmd, ["show"])
    assert "pipe_a" in out.output
    assert "pipe_b" in out.output


def test_show_cmd_exits_1_when_exhausted_and_flag_set(runner):
    cfg = _make_cfg()
    results = [_make_result("pipe_a", True)]
    with patch("pipewatch.cli_budget.load_config", return_value=cfg), \
         patch("pipewatch.cli_budget.compute_all_budgets", return_value=results):
        out = runner.invoke(budget_cmd, ["show", "--fail-exhausted"])
    assert out.exit_code == 1


def test_show_cmd_no_exit_1_when_healthy(runner):
    cfg = _make_cfg()
    results = [_make_result("pipe_a", False)]
    with patch("pipewatch.cli_budget.load_config", return_value=cfg), \
         patch("pipewatch.cli_budget.compute_all_budgets", return_value=results):
        out = runner.invoke(budget_cmd, ["show", "--fail-exhausted"])
    assert out.exit_code == 0


def test_show_cmd_unknown_pipeline_exits_2(runner):
    cfg = _make_cfg()
    with patch("pipewatch.cli_budget.load_config", return_value=cfg):
        out = runner.invoke(budget_cmd, ["show", "--pipeline", "nonexistent"])
    assert out.exit_code == 2
