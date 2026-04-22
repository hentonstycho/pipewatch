"""Tests for pipewatch.cli_eventlog."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_eventlog import eventlog_cmd
from pipewatch.eventlog import record_event
from pipewatch.config import PipewatchConfig, PipelineConfig, NotificationConfig


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _make_cfg(names: list[str]) -> PipewatchConfig:
    pipelines = [
        PipelineConfig(name=n, source="db", query="SELECT 1", schedule="5m")
        for n in names
    ]
    notif = NotificationConfig(slack_webhook=None, email_to=None, email_from=None, smtp_host=None)
    return PipewatchConfig(pipelines=pipelines, notification=notif)


def test_show_cmd_prints_output(tmp_path: Path, runner: CliRunner) -> None:
    event_dir = str(tmp_path / "events")
    record_event("pipe_a", "check", "All good", base_dir=event_dir)
    record_event("pipe_a", "alert", "Threshold breached", base_dir=event_dir)

    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_eventlog.load_config", return_value=cfg), \
         patch("pipewatch.cli_eventlog.EVENT_DIR", event_dir):
        result = runner.invoke(eventlog_cmd, ["show", "pipe_a"])

    assert result.exit_code == 0
    assert "CHECK" in result.output
    assert "ALERT" in result.output


def test_show_cmd_filters_by_type(tmp_path: Path, runner: CliRunner) -> None:
    event_dir = str(tmp_path / "events")
    record_event("pipe_b", "check", "ok", base_dir=event_dir)
    record_event("pipe_b", "alert", "fail", base_dir=event_dir)

    cfg = _make_cfg(["pipe_b"])
    with patch("pipewatch.cli_eventlog.load_config", return_value=cfg), \
         patch("pipewatch.cli_eventlog.EVENT_DIR", event_dir):
        result = runner.invoke(eventlog_cmd, ["show", "pipe_b", "--type", "check"])

    assert result.exit_code == 0
    assert "CHECK" in result.output
    assert "ALERT" not in result.output


def test_show_cmd_unknown_pipeline_exits_2(runner: CliRunner) -> None:
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_eventlog.load_config", return_value=cfg):
        result = runner.invoke(eventlog_cmd, ["show", "no_such_pipe"])
    assert result.exit_code == 2


def test_show_cmd_no_events_prints_message(tmp_path: Path, runner: CliRunner) -> None:
    event_dir = str(tmp_path / "events")
    cfg = _make_cfg(["pipe_c"])
    with patch("pipewatch.cli_eventlog.load_config", return_value=cfg), \
         patch("pipewatch.cli_eventlog.EVENT_DIR", event_dir):
        result = runner.invoke(eventlog_cmd, ["show", "pipe_c"])
    assert result.exit_code == 0
    assert "No events" in result.output


def test_summary_cmd_prints_counts(tmp_path: Path, runner: CliRunner) -> None:
    event_dir = str(tmp_path / "events")
    for _ in range(3):
        record_event("pipe_d", "check", "ok", base_dir=event_dir)
    record_event("pipe_d", "alert", "fail", base_dir=event_dir)

    cfg = _make_cfg(["pipe_d"])
    with patch("pipewatch.cli_eventlog.load_config", return_value=cfg), \
         patch("pipewatch.cli_eventlog.EVENT_DIR", event_dir):
        result = runner.invoke(eventlog_cmd, ["summary", "pipe_d"])

    assert result.exit_code == 0
    assert "check" in result.output
    assert "alert" in result.output
    assert "3" in result.output


def test_summary_cmd_unknown_pipeline_exits_2(runner: CliRunner) -> None:
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_eventlog.load_config", return_value=cfg):
        result = runner.invoke(eventlog_cmd, ["summary", "ghost"])
    assert result.exit_code == 2
