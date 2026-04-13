"""Tests for pipewatch.cli_silence."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.cli_silence import add_cmd, remove_cmd, status_cmd


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def silence_file(tmp_path: Path) -> Path:
    return tmp_path / "silences.json"


def test_add_cmd_creates_silence(runner: CliRunner, silence_file: Path) -> None:
    result = runner.invoke(
        add_cmd, ["my_pipe", "--minutes", "30", "--silence-file", str(silence_file)]
    )
    assert result.exit_code == 0
    assert "Silenced 'my_pipe'" in result.output
    assert silence_file.exists()


def test_add_cmd_default_minutes(runner: CliRunner, silence_file: Path) -> None:
    result = runner.invoke(
        add_cmd, ["pipe_a", "--silence-file", str(silence_file)]
    )
    assert result.exit_code == 0
    data = json.loads(silence_file.read_text())
    assert "pipe_a" in data


def test_remove_cmd_removes_existing_silence(
    runner: CliRunner, silence_file: Path
) -> None:
    runner.invoke(add_cmd, ["pipe_b", "--silence-file", str(silence_file)])
    result = runner.invoke(
        remove_cmd, ["pipe_b", "--silence-file", str(silence_file)]
    )
    assert result.exit_code == 0
    assert "Silence removed" in result.output


def test_remove_cmd_exits_1_when_not_found(
    runner: CliRunner, silence_file: Path
) -> None:
    result = runner.invoke(
        remove_cmd, ["nonexistent", "--silence-file", str(silence_file)]
    )
    assert result.exit_code == 1
    assert "No active silence" in result.output


def test_status_cmd_shows_silenced(runner: CliRunner, silence_file: Path) -> None:
    runner.invoke(add_cmd, ["pipe_c", "--minutes", "60", "--silence-file", str(silence_file)])
    result = runner.invoke(
        status_cmd, ["pipe_c", "--silence-file", str(silence_file)]
    )
    assert result.exit_code == 0
    assert "SILENCED" in result.output


def test_status_cmd_exits_1_when_not_silenced(
    runner: CliRunner, silence_file: Path
) -> None:
    result = runner.invoke(
        status_cmd, ["not_silenced", "--silence-file", str(silence_file)]
    )
    assert result.exit_code == 1
    assert "NOT silenced" in result.output


def test_status_cmd_expired_silence_exits_1(
    runner: CliRunner, silence_file: Path
) -> None:
    past = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    silence_file.parent.mkdir(parents=True, exist_ok=True)
    silence_file.write_text(json.dumps({"old_pipe": past}))
    result = runner.invoke(
        status_cmd, ["old_pipe", "--silence-file", str(silence_file)]
    )
    assert result.exit_code == 1
