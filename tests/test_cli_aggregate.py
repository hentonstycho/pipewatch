"""Tests for pipewatch.cli_aggregate."""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_aggregate import aggregate_cmd, summary_cmd
from pipewatch.aggregator import RollupStats
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


def _make_cfg():
    return PipewatchConfig(
        pipelines=[
            PipelineConfig(name="pipe_a", source="db", thresholds=ThresholdConfig()),
            PipelineConfig(name="pipe_b", source="db", thresholds=ThresholdConfig()),
        ],
        notifications=NotificationConfig(),
    )


@pytest.fixture()
def runner():
    return CliRunner()


def _mock_stats(healthy=2, degraded=0, total=2, checks=10, failures=1, most_failing=None):
    return RollupStats(
        total_pipelines=total,
        healthy_pipelines=healthy,
        degraded_pipelines=degraded,
        total_checks=checks,
        total_failures=failures,
        failure_rate=failures / checks if checks else 0.0,
        most_failing=most_failing,
        pipelines=["pipe_a", "pipe_b"],
    )


def test_summary_cmd_prints_output(runner):
    with patch("pipewatch.cli_aggregate.load_config", return_value=_make_cfg()), \
         patch("pipewatch.cli_aggregate.aggregate", return_value=_mock_stats()):
        result = runner.invoke(summary_cmd, ["--config", "pipewatch.yaml"])
    assert result.exit_code == 0
    assert "Pipelines" in result.output
    assert "Healthy" in result.output
    assert "Failure rate" in result.output


def test_summary_cmd_shows_most_failing(runner):
    stats = _mock_stats(healthy=1, degraded=1, failures=3, most_failing="pipe_a")
    with patch("pipewatch.cli_aggregate.load_config", return_value=_make_cfg()), \
         patch("pipewatch.cli_aggregate.aggregate", return_value=stats):
        result = runner.invoke(summary_cmd, [])
    assert "pipe_a" in result.output


def test_summary_cmd_exits_1_when_below_threshold(runner):
    stats = _mock_stats(healthy=1, degraded=1, total=2)
    with patch("pipewatch.cli_aggregate.load_config", return_value=_make_cfg()), \
         patch("pipewatch.cli_aggregate.aggregate", return_value=stats):
        result = runner.invoke(summary_cmd, ["--fail-below", "0.9"])
    assert result.exit_code == 1


def test_summary_cmd_no_exit_when_above_threshold(runner):
    stats = _mock_stats(healthy=2, degraded=0, total=2)
    with patch("pipewatch.cli_aggregate.load_config", return_value=_make_cfg()), \
         patch("pipewatch.cli_aggregate.aggregate", return_value=stats):
        result = runner.invoke(summary_cmd, ["--fail-below", "0.5"])
    assert result.exit_code == 0


def test_summary_cmd_no_most_failing_shows_dash(runner):
    stats = _mock_stats(failures=0, most_failing=None)
    with patch("pipewatch.cli_aggregate.load_config", return_value=_make_cfg()), \
         patch("pipewatch.cli_aggregate.aggregate", return_value=stats):
        result = runner.invoke(summary_cmd, [])
    assert "—" in result.output
