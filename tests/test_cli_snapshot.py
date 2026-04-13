"""Tests for pipewatch.cli_snapshot."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pipewatch.checker import CheckResult
from pipewatch.cli_snapshot import snapshot_cmd
from pipewatch.snapshotter import save_snapshot


def _result(name: str, healthy: bool) -> CheckResult:
    return CheckResult(
        pipeline=name,
        healthy=healthy,
        violations=[] if healthy else ["row_count"],
        checked_at="2024-01-01T00:00:00+00:00",
    )


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def _mock_config():
    cfg = MagicMock()
    cfg.pipelines = []
    with patch("pipewatch.cli_snapshot.PipewatchConfig.load", return_value=cfg), \
         patch("pipewatch.cli_snapshot.check_all_pipelines", return_value=[_result("p", True)]):
        yield


def test_take_cmd_prints_snapshot_path(runner: CliRunner, tmp_path: Path, _mock_config) -> None:
    result = runner.invoke(
        snapshot_cmd,
        ["take", "--config", "pipewatch.yaml", "--snapshot-dir", str(tmp_path)],
    )
    assert result.exit_code == 0
    assert "Snapshot saved" in result.output


def test_take_cmd_with_label(runner: CliRunner, tmp_path: Path, _mock_config) -> None:
    result = runner.invoke(
        snapshot_cmd,
        ["take", "--label", "ci", "--snapshot-dir", str(tmp_path)],
    )
    assert result.exit_code == 0
    assert "ci" in result.output


def test_list_cmd_no_snapshots(runner: CliRunner, tmp_path: Path) -> None:
    result = runner.invoke(snapshot_cmd, ["list", "--snapshot-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "No snapshots found" in result.output


def test_list_cmd_shows_snapshots(runner: CliRunner, tmp_path: Path) -> None:
    save_snapshot([_result("pipe_a", True)], label="nightly", snapshot_dir=tmp_path)
    result = runner.invoke(snapshot_cmd, ["list", "--snapshot-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "nightly" in result.output
    assert "pipe_a" not in result.output  # list shows file metadata, not pipeline names


def test_diff_cmd_no_changes(runner: CliRunner, tmp_path: Path) -> None:
    path = save_snapshot([_result("p", True)], snapshot_dir=tmp_path)
    result = runner.invoke(snapshot_cmd, ["diff", str(path), str(path)])
    assert result.exit_code == 0
    assert "No changes" in result.output


def test_diff_cmd_shows_degraded(runner: CliRunner, tmp_path: Path) -> None:
    old_path = save_snapshot([_result("p", True)], label="old", snapshot_dir=tmp_path)
    new_path = save_snapshot([_result("p", False)], label="new", snapshot_dir=tmp_path)
    result = runner.invoke(snapshot_cmd, ["diff", str(old_path), str(new_path)])
    assert result.exit_code == 0
    assert "DEGRADED" in result.output
