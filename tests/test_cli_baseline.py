"""Tests for pipewatch.cli_baseline."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pipewatch.baseline import save_baseline
from pipewatch.cli_baseline import baseline_cmd
from pipewatch.metrics import PipelineMetrics


def _make_metrics(avg_error_rate=0.01, avg_latency_seconds=3.0):
    return PipelineMetrics(
        avg_row_count=50.0,
        avg_error_rate=avg_error_rate,
        avg_latency_seconds=avg_latency_seconds,
        total_runs=5,
        failure_count=0,
    )


@pytest.fixture()
def _mock_config(tmp_path):
    cfg = MagicMock()
    cfg.pipelines = [MagicMock(name="pipe_a")]
    cfg.pipelines[0].name = "pipe_a"
    return cfg


@pytest.fixture()
def runner():
    return CliRunner()


def test_capture_cmd_prints_path(runner, _mock_config, tmp_path):
    metrics = {"pipe_a": _make_metrics()}
    with patch("pipewatch.cli_baseline.compute_all_metrics", return_value=metrics), \
         patch("pipewatch.cli_baseline.save_baseline", return_value=tmp_path / "v1.json") as mock_save:
        result = runner.invoke(baseline_cmd, ["capture", "v1"], obj=_mock_config)
    assert result.exit_code == 0
    assert "v1" in result.output
    mock_save.assert_called_once()


def test_list_cmd_no_baselines(runner, _mock_config, tmp_path):
    with patch("pipewatch.cli_baseline.list_baselines", return_value=[]):
        result = runner.invoke(baseline_cmd, ["list"], obj=_mock_config)
    assert result.exit_code == 0
    assert "No baselines" in result.output


def test_list_cmd_shows_names(runner, _mock_config):
    with patch("pipewatch.cli_baseline.list_baselines", return_value=["v1", "v2"]):
        result = runner.invoke(baseline_cmd, ["list"], obj=_mock_config)
    assert "v1" in result.output
    assert "v2" in result.output


def test_diff_cmd_prints_deltas(runner, _mock_config):
    metrics = {"pipe_a": _make_metrics()}
    deltas = {"pipe_a": {"avg_row_count": 0.0, "avg_error_rate": 0.0, "avg_latency_seconds": 0.0}}
    with patch("pipewatch.cli_baseline.compute_all_metrics", return_value=metrics), \
         patch("pipewatch.cli_baseline.diff_baseline", return_value=deltas):
        result = runner.invoke(baseline_cmd, ["diff", "v1"], obj=_mock_config)
    assert result.exit_code == 0
    assert "pipe_a" in result.output


def test_diff_cmd_exits_1_on_regression_with_flag(runner, _mock_config):
    metrics = {"pipe_a": _make_metrics()}
    deltas = {"pipe_a": {"avg_row_count": 0.0, "avg_error_rate": 0.05, "avg_latency_seconds": 0.0}}
    with patch("pipewatch.cli_baseline.compute_all_metrics", return_value=metrics), \
         patch("pipewatch.cli_baseline.diff_baseline", return_value=deltas):
        result = runner.invoke(
            baseline_cmd, ["diff", "v1", "--fail-on-regression"], obj=_mock_config
        )
    assert result.exit_code == 1


def test_diff_cmd_unknown_pipeline_exits_2(runner, _mock_config):
    result = runner.invoke(
        baseline_cmd, ["diff", "v1", "--pipeline", "ghost"], obj=_mock_config
    )
    assert result.exit_code == 2


def test_diff_cmd_missing_baseline_exits_2(runner, _mock_config):
    metrics = {"pipe_a": _make_metrics()}
    with patch("pipewatch.cli_baseline.compute_all_metrics", return_value=metrics), \
         patch("pipewatch.cli_baseline.diff_baseline", side_effect=FileNotFoundError("not found")):
        result = runner.invoke(baseline_cmd, ["diff", "missing"], obj=_mock_config)
    assert result.exit_code == 2
