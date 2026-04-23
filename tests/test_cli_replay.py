"""Tests for pipewatch/cli_replay.py."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pipewatch.cli_replay import replay_cmd
from pipewatch.config import PipelineConfig, PipewatchConfig, ThresholdConfig
from pipewatch.replayer import ReplayEvent
from pipewatch.checker import CheckResult
from datetime import datetime, timezone


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _make_cfg(names: list[str]) -> PipewatchConfig:
    pipelines = [
        PipelineConfig(name=n, source="test", thresholds=ThresholdConfig())
        for n in names
    ]
    return PipewatchConfig(pipelines=pipelines, notifications=MagicMock())


def _make_event(pipeline: str, healthy: bool) -> ReplayEvent:
    r = CheckResult(pipeline=pipeline, healthy=healthy, violations=[] if healthy else ["err"], metrics={})
    return ReplayEvent(pipeline=pipeline, result=r, original_ts=datetime(2024, 1, 1, tzinfo=timezone.utc))


def test_run_cmd_prints_events(runner: CliRunner) -> None:
    cfg = _make_cfg(["pipe_a"])
    events = [_make_event("pipe_a", True), _make_event("pipe_a", False)]
    with patch("pipewatch.cli_replay.PipewatchConfig.load", return_value=cfg), \
         patch("pipewatch.cli_replay.replay_all", return_value=events):
        result = runner.invoke(replay_cmd, ["run"])
    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "OK" in result.output
    assert "FAIL" in result.output


def test_run_cmd_no_events_prints_message(runner: CliRunner) -> None:
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_replay.PipewatchConfig.load", return_value=cfg), \
         patch("pipewatch.cli_replay.replay_all", return_value=[]):
        result = runner.invoke(replay_cmd, ["run"])
    assert result.exit_code == 0
    assert "No events" in result.output


def test_run_cmd_unknown_pipeline_exits_2(runner: CliRunner) -> None:
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_replay.PipewatchConfig.load", return_value=cfg):
        result = runner.invoke(replay_cmd, ["run", "--pipeline", "ghost"])
    assert result.exit_code == 2


def test_run_cmd_single_pipeline(runner: CliRunner) -> None:
    cfg = _make_cfg(["pipe_a"])
    events = [_make_event("pipe_a", True)]
    with patch("pipewatch.cli_replay.PipewatchConfig.load", return_value=cfg), \
         patch("pipewatch.cli_replay.replay_pipeline", return_value=events) as mock_rp:
        result = runner.invoke(replay_cmd, ["run", "--pipeline", "pipe_a"])
    assert result.exit_code == 0
    mock_rp.assert_called_once()


def test_run_cmd_invalid_since_exits(runner: CliRunner) -> None:
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_replay.PipewatchConfig.load", return_value=cfg):
        result = runner.invoke(replay_cmd, ["run", "--since", "not-a-date"])
    assert result.exit_code != 0
