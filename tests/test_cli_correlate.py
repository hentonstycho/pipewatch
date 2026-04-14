"""Tests for pipewatch.cli_correlate."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_correlate import correlate_cmd
from pipewatch.correlator import CorrelationGroup
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


BASE_TS = datetime(2024, 6, 1, 8, 0, 0, tzinfo=timezone.utc)


def _mock_config() -> PipewatchConfig:
    return PipewatchConfig(
        pipelines=[
            PipelineConfig(name="pipe_a", thresholds=ThresholdConfig()),
            PipelineConfig(name="pipe_b", thresholds=ThresholdConfig()),
        ],
        notifications=NotificationConfig(),
    )


@pytest.fixture()
def runner():
    return CliRunner()


def test_run_cmd_no_groups_prints_message(runner):
    with patch("pipewatch.cli_correlate.load_config", return_value=_mock_config()), \
         patch("pipewatch.cli_correlate.correlate_failures", return_value=[]):
        result = runner.invoke(correlate_cmd, ["run"])
    assert result.exit_code == 0
    assert "No correlated failures" in result.output


def test_run_cmd_prints_group(runner):
    group = CorrelationGroup(
        window_start=BASE_TS,
        window_end=BASE_TS + timedelta(minutes=5),
        pipelines=["pipe_a", "pipe_b"],
    )
    with patch("pipewatch.cli_correlate.load_config", return_value=_mock_config()), \
         patch("pipewatch.cli_correlate.correlate_failures", return_value=[group]):
        result = runner.invoke(correlate_cmd, ["run"])
    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "pipe_b" in result.output
    assert "Group 1" in result.output


def test_run_cmd_min_size_filters_small_groups(runner):
    group = CorrelationGroup(
        window_start=BASE_TS,
        window_end=BASE_TS + timedelta(minutes=5),
        pipelines=["pipe_a", "pipe_b"],
    )
    with patch("pipewatch.cli_correlate.load_config", return_value=_mock_config()), \
         patch("pipewatch.cli_correlate.correlate_failures", return_value=[group]):
        result = runner.invoke(correlate_cmd, ["run", "--min-size", "3"])
    assert result.exit_code == 0
    assert "No correlated failures" in result.output


def test_run_cmd_passes_window_to_correlate(runner):
    with patch("pipewatch.cli_correlate.load_config", return_value=_mock_config()) as _lc, \
         patch("pipewatch.cli_correlate.correlate_failures", return_value=[]) as mock_cf:
        runner.invoke(correlate_cmd, ["run", "--window", "10"])
    mock_cf.assert_called_once()
    _, kwargs = mock_cf.call_args
    assert kwargs.get("window_minutes") == 10
