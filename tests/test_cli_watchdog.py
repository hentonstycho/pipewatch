"""Tests for pipewatch.cli_watchdog."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_watchdog import watchdog_cmd
from pipewatch.watchdog import StaleResult


FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _stale_results(stale: bool) -> list[StaleResult]:
    return [
        StaleResult(
            pipeline="pipe_x",
            last_checked=FIXED_NOW - timedelta(seconds=200),
            age_seconds=200.0,
            threshold_seconds=3600.0,
            is_stale=stale,
        )
    ]


@patch("pipewatch.cli_watchdog.load_config")
@patch("pipewatch.cli_watchdog.check_all_staleness")
def test_check_cmd_prints_ok(mock_check: MagicMock, mock_cfg: MagicMock, runner: CliRunner) -> None:
    mock_check.return_value = _stale_results(stale=False)
    result = runner.invoke(watchdog_cmd, ["check"])
    assert result.exit_code == 0
    assert "OK" in result.output
    assert "pipe_x" in result.output


@patch("pipewatch.cli_watchdog.load_config")
@patch("pipewatch.cli_watchdog.check_all_staleness")
def test_check_cmd_prints_stale(mock_check: MagicMock, mock_cfg: MagicMock, runner: CliRunner) -> None:
    mock_check.return_value = _stale_results(stale=True)
    result = runner.invoke(watchdog_cmd, ["check"])
    assert result.exit_code == 0
    assert "STALE" in result.output


@patch("pipewatch.cli_watchdog.load_config")
@patch("pipewatch.cli_watchdog.check_all_staleness")
def test_check_cmd_exits_1_when_stale_and_flag_set(
    mock_check: MagicMock, mock_cfg: MagicMock, runner: CliRunner
) -> None:
    mock_check.return_value = _stale_results(stale=True)
    result = runner.invoke(watchdog_cmd, ["check", "--fail-on-stale"])
    assert result.exit_code == 1


@patch("pipewatch.cli_watchdog.load_config")
@patch("pipewatch.cli_watchdog.check_all_staleness")
def test_check_cmd_no_exit_1_when_healthy_and_flag_set(
    mock_check: MagicMock, mock_cfg: MagicMock, runner: CliRunner
) -> None:
    mock_check.return_value = _stale_results(stale=False)
    result = runner.invoke(watchdog_cmd, ["check", "--fail-on-stale"])
    assert result.exit_code == 0


@patch("pipewatch.cli_watchdog.load_config")
@patch("pipewatch.cli_watchdog.check_all_staleness")
def test_check_cmd_passes_threshold_to_checker(
    mock_check: MagicMock, mock_cfg: MagicMock, runner: CliRunner
) -> None:
    mock_check.return_value = []
    runner.invoke(watchdog_cmd, ["check", "--threshold", "7200"])
    _, kwargs = mock_check.call_args
    assert kwargs.get("default_threshold_seconds") == 7200.0
