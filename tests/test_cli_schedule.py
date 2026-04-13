"""Tests for pipewatch.cli_schedule."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pipewatch.checker import CheckResult
from pipewatch.cli_schedule import schedule_cmd


def _make_result(name: str, healthy: bool) -> CheckResult:
    return CheckResult(
        pipeline_name=name,
        healthy=healthy,
        violations=[],
        last_checked="2024-01-01T00:00:00+00:00",
    )


@patch("pipewatch.cli_schedule.run_scheduler")
@patch("pipewatch.cli_schedule.dispatch_notifications")
@patch("pipewatch.cli_schedule.record_result")
@patch("pipewatch.cli_schedule.check_all_pipelines")
@patch("pipewatch.cli_schedule.load_config")
def test_schedule_cmd_invokes_scheduler(mock_load, mock_check, mock_record, mock_notify, mock_sched):
    mock_load.return_value = MagicMock(notifications=MagicMock())
    mock_check.return_value = [_make_result("pipe_a", True)]

    runner = CliRunner()
    result = runner.invoke(schedule_cmd, ["--interval", "10s"])

    assert result.exit_code == 0
    mock_sched.assert_called_once()
    _, kwargs = mock_sched.call_args
    assert kwargs.get("stop_on_error") is False


@patch("pipewatch.cli_schedule.run_scheduler")
@patch("pipewatch.cli_schedule.dispatch_notifications")
@patch("pipewatch.cli_schedule.record_result")
@patch("pipewatch.cli_schedule.check_all_pipelines")
@patch("pipewatch.cli_schedule.load_config")
def test_schedule_cmd_tick_records_and_notifies(mock_load, mock_check, mock_record, mock_notify, mock_sched):
    """The task passed to run_scheduler calls record_result and dispatch_notifications."""
    cfg_mock = MagicMock(notifications=MagicMock())
    mock_load.return_value = cfg_mock
    results = [_make_result("pipe_a", True), _make_result("pipe_b", False)]
    mock_check.return_value = results

    captured_task = {}

    def capture_scheduler(interval, task, **kwargs):
        captured_task["fn"] = task

    mock_sched.side_effect = capture_scheduler

    runner = CliRunner()
    runner.invoke(schedule_cmd, ["--interval", "30s"])

    # Now manually invoke the captured tick function.
    captured_task["fn"]()

    assert mock_record.call_count == 2
    mock_notify.assert_called_once_with(results, cfg_mock.notifications)


@patch("pipewatch.cli_schedule.run_scheduler")
@patch("pipewatch.cli_schedule.dispatch_notifications")
@patch("pipewatch.cli_schedule.record_result")
@patch("pipewatch.cli_schedule.check_all_pipelines")
@patch("pipewatch.cli_schedule.load_config")
def test_schedule_cmd_no_notify_flag(mock_load, mock_check, mock_record, mock_notify, mock_sched):
    cfg_mock = MagicMock(notifications=MagicMock())
    mock_load.return_value = cfg_mock
    mock_check.return_value = [_make_result("pipe_a", False)]

    captured_task = {}

    def capture_scheduler(interval, task, **kwargs):
        captured_task["fn"] = task

    mock_sched.side_effect = capture_scheduler

    runner = CliRunner()
    runner.invoke(schedule_cmd, ["--no-notify"])
    captured_task["fn"]()

    mock_notify.assert_not_called()


@patch("pipewatch.cli_schedule.run_scheduler")
@patch("pipewatch.cli_schedule.dispatch_notifications")
@patch("pipewatch.cli_schedule.record_result")
@patch("pipewatch.cli_schedule.check_all_pipelines")
@patch("pipewatch.cli_schedule.load_config")
def test_schedule_cmd_stop_on_error_flag(mock_load, mock_check, mock_record, mock_notify, mock_sched):
    mock_load.return_value = MagicMock(notifications=MagicMock())
    mock_check.return_value = []

    runner = CliRunner()
    runner.invoke(schedule_cmd, ["--stop-on-error"])

    _, kwargs = mock_sched.call_args
    assert kwargs.get("stop_on_error") is True
