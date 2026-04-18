"""Tests for pipewatch.suppressor."""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.suppressor import (
    suppress,
    unsuppress,
    is_suppressed,
    get_suppression,
    list_suppressions,
)
from pipewatch.cli_suppress import suppress_cmd


@pytest.fixture()
def sup_dir(tmp_path: Path) -> Path:
    return tmp_path / "suppressor"


def test_suppress_creates_file(sup_dir: Path) -> None:
    suppress("pipe_a", "known issue", base_dir=sup_dir)
    assert (sup_dir / "pipe_a.json").exists()


def test_is_suppressed_true_after_suppress(sup_dir: Path) -> None:
    suppress("pipe_a", "test", base_dir=sup_dir)
    assert is_suppressed("pipe_a", base_dir=sup_dir)


def test_is_suppressed_false_for_unknown(sup_dir: Path) -> None:
    assert not is_suppressed("no_such_pipe", base_dir=sup_dir)


def test_unsuppress_removes_file(sup_dir: Path) -> None:
    suppress("pipe_a", "test", base_dir=sup_dir)
    result = unsuppress("pipe_a", base_dir=sup_dir)
    assert result is True
    assert not is_suppressed("pipe_a", base_dir=sup_dir)


def test_unsuppress_returns_false_when_missing(sup_dir: Path) -> None:
    assert unsuppress("ghost", base_dir=sup_dir) is False


def test_get_suppression_returns_dict(sup_dir: Path) -> None:
    suppress("pipe_a", "reason x", base_dir=sup_dir)
    state = get_suppression("pipe_a", base_dir=sup_dir)
    assert state is not None
    assert state["pipeline"] == "pipe_a"
    assert state["reason"] == "reason x"
    assert "suppressed_at" in state


def test_get_suppression_none_for_unknown(sup_dir: Path) -> None:
    assert get_suppression("unknown", base_dir=sup_dir) is None


def test_list_suppressions_empty(sup_dir: Path) -> None:
    assert list_suppressions(base_dir=sup_dir) == []


def test_list_suppressions_returns_all(sup_dir: Path) -> None:
    suppress("pipe_a", "r1", base_dir=sup_dir)
    suppress("pipe_b", "r2", base_dir=sup_dir)
    entries = list_suppressions(base_dir=sup_dir)
    names = {e["pipeline"] for e in entries}
    assert names == {"pipe_a", "pipe_b"}


# --- CLI ---

@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_add_cmd_suppresses(runner: CliRunner, sup_dir: Path) -> None:
    result = runner.invoke(suppress_cmd, ["add", "pipe_x", "--reason", "testing", "--dir", str(sup_dir)])
    assert result.exit_code == 0
    assert "Suppressed" in result.output
    assert is_suppressed("pipe_x", base_dir=sup_dir)


def test_remove_cmd_removes(runner: CliRunner, sup_dir: Path) -> None:
    suppress("pipe_x", "test", base_dir=sup_dir)
    result = runner.invoke(suppress_cmd, ["remove", "pipe_x", "--dir", str(sup_dir)])
    assert result.exit_code == 0
    assert not is_suppressed("pipe_x", base_dir=sup_dir)


def test_remove_cmd_exits_1_when_missing(runner: CliRunner, sup_dir: Path) -> None:
    result = runner.invoke(suppress_cmd, ["remove", "ghost", "--dir", str(sup_dir)])
    assert result.exit_code == 1


def test_status_cmd_lists(runner: CliRunner, sup_dir: Path) -> None:
    suppress("pipe_a", "some reason", base_dir=sup_dir)
    result = runner.invoke(suppress_cmd, ["status", "--dir", str(sup_dir)])
    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "some reason" in result.output
