"""Tests for pipewatch.cli_reaper."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pipewatch.cli_reaper import reaper_cmd

UTC = timezone.utc
NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _make_cfg(hist_dir: Path, names: list[str]) -> MagicMock:
    cfg = MagicMock()
    cfg.history_dir = str(hist_dir)
    pipelines = []
    for n in names:
        p = MagicMock()
        p.name = n
        pipelines.append(p)
    cfg.pipelines = pipelines
    return cfg


def _write_entry(hist_dir: Path, pipeline: str, ts: datetime) -> None:
    hist_dir.mkdir(parents=True, exist_ok=True)
    path = hist_dir / f"{pipeline}.jsonl"
    entry = {"pipeline": pipeline, "healthy": True, "checked_at": ts.isoformat()}
    with path.open("a") as fh:
        fh.write(json.dumps(entry) + "\n")


def test_check_cmd_prints_output(runner: CliRunner, tmp_path: Path) -> None:
    hist = tmp_path / "history"
    cfg = _make_cfg(hist, ["pipe_a"])
    _write_entry(hist, "pipe_a", NOW - timedelta(hours=1))
    with patch("pipewatch.cli_reaper.load_config", return_value=cfg):
        result = runner.invoke(reaper_cmd, ["check"])
    assert result.exit_code == 0
    assert "pipe_a" in result.output


def test_check_cmd_exits_1_when_dead_and_flag_set(runner: CliRunner, tmp_path: Path) -> None:
    hist = tmp_path / "history"
    cfg = _make_cfg(hist, ["dead_pipe"])
    # no history written → dead
    with patch("pipewatch.cli_reaper.load_config", return_value=cfg):
        result = runner.invoke(reaper_cmd, ["check", "--fail-on-dead"])
    assert result.exit_code == 1
    assert "dead_pipe" in result.output


def test_check_cmd_no_exit_1_when_alive_and_flag_set(runner: CliRunner, tmp_path: Path) -> None:
    hist = tmp_path / "history"
    cfg = _make_cfg(hist, ["live_pipe"])
    _write_entry(hist, "live_pipe", datetime.now(UTC) - timedelta(minutes=5))
    with patch("pipewatch.cli_reaper.load_config", return_value=cfg):
        result = runner.invoke(reaper_cmd, ["check", "--fail-on-dead"])
    assert result.exit_code == 0


def test_check_cmd_unknown_pipeline_exits_2(runner: CliRunner, tmp_path: Path) -> None:
    hist = tmp_path / "history"
    cfg = _make_cfg(hist, ["real_pipe"])
    with patch("pipewatch.cli_reaper.load_config", return_value=cfg):
        result = runner.invoke(reaper_cmd, ["check", "--pipeline", "ghost_pipe"])
    assert result.exit_code == 2


def test_check_cmd_single_pipeline(runner: CliRunner, tmp_path: Path) -> None:
    hist = tmp_path / "history"
    cfg = _make_cfg(hist, ["solo"])
    _write_entry(hist, "solo", datetime.now(UTC) - timedelta(hours=2))
    with patch("pipewatch.cli_reaper.load_config", return_value=cfg):
        result = runner.invoke(reaper_cmd, ["check", "--pipeline", "solo"])
    assert result.exit_code == 0
    assert "solo" in result.output
