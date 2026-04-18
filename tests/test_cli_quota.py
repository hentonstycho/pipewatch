"""Tests for pipewatch.cli_quota."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_quota import quota_cmd
from pipewatch.quota import QuotaResult

TODAY = "2024-06-01"


@pytest.fixture()
def runner():
    return CliRunner()


def _make_cfg(names=("pipe_a", "pipe_b")):
    cfg = MagicMock()
    cfg.pipelines = [MagicMock(name=n) for n in names]
    for p, n in zip(cfg.pipelines, names):
        p.name = n
    return cfg


def _make_quota(pipeline, count, limit, exhausted=False):
    return QuotaResult(pipeline=pipeline, date=TODAY, count=count, limit=limit, exhausted=exhausted)


def test_status_cmd_prints_output(runner):
    cfg = _make_cfg()
    with patch("pipewatch.cli_quota.load_config", return_value=cfg), \
         patch("pipewatch.cli_quota.get_quota", side_effect=[
             _make_quota("pipe_a", 3, 10),
             _make_quota("pipe_b", 0, 10),
         ]):
        result = runner.invoke(quota_cmd, ["status", "--config", "x.yaml"])
    assert result.exit_code == 0
    assert "pipe_a: 3/10" in result.output
    assert "pipe_b: 0/10" in result.output


def test_status_cmd_marks_exhausted(runner):
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_quota.load_config", return_value=cfg), \
         patch("pipewatch.cli_quota.get_quota", return_value=_make_quota("pipe_a", 11, 10, exhausted=True)):
        result = runner.invoke(quota_cmd, ["status", "--config", "x.yaml"])
    assert "EXHAUSTED" in result.output


def test_status_cmd_unknown_pipeline_exits_2(runner):
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_quota.load_config", return_value=cfg):
        result = runner.invoke(quota_cmd, ["status", "--pipeline", "ghost", "--config", "x.yaml"])
    assert result.exit_code == 2


def test_reset_cmd_removes_file(runner, tmp_path):
    quota_file = tmp_path / "pipe_a.json"
    quota_file.write_text('{"date": "2024-06-01", "count": 5}')
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_quota.load_config", return_value=cfg), \
         patch("pipewatch.cli_quota._DEFAULT_DIR", tmp_path):
        result = runner.invoke(quota_cmd, ["reset", "pipe_a", "--config", "x.yaml"])
    assert result.exit_code == 0
    assert not quota_file.exists()


def test_reset_cmd_unknown_pipeline_exits_2(runner):
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_quota.load_config", return_value=cfg):
        result = runner.invoke(quota_cmd, ["reset", "ghost", "--config", "x.yaml"])
    assert result.exit_code == 2
