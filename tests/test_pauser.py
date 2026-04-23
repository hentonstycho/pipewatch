"""Tests for pipewatch.pauser and pipewatch.cli_pause."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.pauser import (
    is_paused,
    list_pauses,
    pause_pipeline,
    unpause_pipeline,
)
from pipewatch.cli_pause import pause_cmd


@pytest.fixture()
def pause_dir(tmp_path: Path) -> Path:
    return tmp_path


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# pauser unit tests
# ---------------------------------------------------------------------------

def test_pause_pipeline_creates_file(pause_dir: Path) -> None:
    pause_pipeline("pipe_a", data_dir=pause_dir)
    assert (pause_dir / "pauses.json").exists()


def test_pause_indefinite_returns_none(pause_dir: Path) -> None:
    expiry = pause_pipeline("pipe_a", data_dir=pause_dir)
    assert expiry is None


def test_pause_with_minutes_returns_expiry(pause_dir: Path) -> None:
    expiry = pause_pipeline("pipe_a", minutes=30, data_dir=pause_dir)
    assert expiry is not None
    dt = datetime.fromisoformat(expiry)
    assert dt > _now()


def test_is_paused_true_after_indefinite_pause(pause_dir: Path) -> None:
    pause_pipeline("pipe_a", data_dir=pause_dir)
    assert is_paused("pipe_a", data_dir=pause_dir) is True


def test_is_paused_false_for_unknown_pipeline(pause_dir: Path) -> None:
    assert is_paused("no_such_pipe", data_dir=pause_dir) is False


def test_is_paused_false_after_expiry(pause_dir: Path) -> None:
    past = (_now() - timedelta(minutes=5)).isoformat()
    import json
    pause_dir.mkdir(parents=True, exist_ok=True)
    (pause_dir / "pauses.json").write_text(json.dumps({"pipe_a": past}))
    assert is_paused("pipe_a", data_dir=pause_dir) is False


def test_unpause_removes_entry(pause_dir: Path) -> None:
    pause_pipeline("pipe_a", data_dir=pause_dir)
    removed = unpause_pipeline("pipe_a", data_dir=pause_dir)
    assert removed is True
    assert is_paused("pipe_a", data_dir=pause_dir) is False


def test_unpause_unknown_returns_false(pause_dir: Path) -> None:
    assert unpause_pipeline("no_such", data_dir=pause_dir) is False


def test_list_pauses_excludes_expired(pause_dir: Path) -> None:
    import json
    past = (_now() - timedelta(minutes=1)).isoformat()
    future = (_now() + timedelta(minutes=10)).isoformat()
    pause_dir.mkdir(parents=True, exist_ok=True)
    (pause_dir / "pauses.json").write_text(
        json.dumps({"expired": past, "active": future})
    )
    result = list_pauses(data_dir=pause_dir)
    assert "active" in result
    assert "expired" not in result


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_add_cmd_creates_pause(runner: CliRunner, pause_dir: Path) -> None:
    with patch("pipewatch.cli_pause._DATA_DIR", pause_dir):
        result = runner.invoke(pause_cmd, ["add", "pipe_x"])
    assert result.exit_code == 0
    assert "paused" in result.output.lower()


def test_add_cmd_with_minutes(runner: CliRunner, pause_dir: Path) -> None:
    with patch("pipewatch.cli_pause._DATA_DIR", pause_dir):
        result = runner.invoke(pause_cmd, ["add", "pipe_x", "--minutes", "60"])
    assert result.exit_code == 0
    assert "until" in result.output


def test_remove_cmd_unpauses(runner: CliRunner, pause_dir: Path) -> None:
    pause_pipeline("pipe_x", data_dir=pause_dir)
    with patch("pipewatch.cli_pause._DATA_DIR", pause_dir):
        result = runner.invoke(pause_cmd, ["remove", "pipe_x"])
    assert result.exit_code == 0
    assert "resumed" in result.output.lower()


def test_remove_cmd_exits_1_when_not_paused(runner: CliRunner, pause_dir: Path) -> None:
    with patch("pipewatch.cli_pause._DATA_DIR", pause_dir):
        result = runner.invoke(pause_cmd, ["remove", "no_such"])
    assert result.exit_code == 1


def test_status_cmd_shows_paused(runner: CliRunner, pause_dir: Path) -> None:
    pause_pipeline("pipe_x", data_dir=pause_dir)
    with patch("pipewatch.cli_pause._DATA_DIR", pause_dir):
        result = runner.invoke(pause_cmd, ["status", "pipe_x"])
    assert "paused" in result.output


def test_status_cmd_shows_active(runner: CliRunner, pause_dir: Path) -> None:
    with patch("pipewatch.cli_pause._DATA_DIR", pause_dir):
        result = runner.invoke(pause_cmd, ["status", "pipe_x"])
    assert "active" in result.output
