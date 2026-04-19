"""Tests for pipewatch.cli_trend"""
from __future__ import annotations

import json
import os
import pytest
from click.testing import CliRunner

from pipewatch.cli_trend import trend_cmd
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


@pytest.fixture()
def runner():
    return CliRunner()


def _make_cfg(names):
    return PipewatchConfig(
        pipelines=[PipelineConfig(name=n, query="SELECT 1", thresholds=ThresholdConfig()) for n in names],
        notifications=NotificationConfig(),
    )


def _write_history(hist_dir, pipeline, entries):
    os.makedirs(hist_dir, exist_ok=True)
    path = os.path.join(hist_dir, f"{pipeline}.jsonl")
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


def test_show_cmd_prints_output(tmp_path, runner, monkeypatch):
    hist = str(tmp_path / "history")
    _write_history(hist, "alpha", [{"healthy": True}] * 10)
    cfg = _make_cfg(["alpha"])
    monkeypatch.setattr("pipewatch.cli_trend.load_config", lambda _: cfg)
    result = runner.invoke(trend_cmd, ["show", "--history-dir", hist])
    assert result.exit_code == 0
    assert "alpha" in result.output


def test_show_cmd_unknown_pipeline_exits_2(tmp_path, runner, monkeypatch):
    cfg = _make_cfg(["alpha"])
    monkeypatch.setattr("pipewatch.cli_trend.load_config", lambda _: cfg)
    result = runner.invoke(trend_cmd, ["show", "--pipeline", "unknown", "--history-dir", str(tmp_path)])
    assert result.exit_code == 2


def test_show_cmd_exits_1_when_degrading_and_flag_set(tmp_path, runner, monkeypatch):
    hist = str(tmp_path / "history")
    _write_history(hist, "beta", [{"healthy": True}] * 5 + [{"healthy": False}] * 15)
    cfg = _make_cfg(["beta"])
    monkeypatch.setattr("pipewatch.cli_trend.load_config", lambda _: cfg)
    result = runner.invoke(trend_cmd, ["show", "--history-dir", hist, "--fail-on-degrading"])
    assert result.exit_code == 1


def test_show_cmd_no_exit_1_when_stable_and_flag_set(tmp_path, runner, monkeypatch):
    hist = str(tmp_path / "history")
    _write_history(hist, "gamma", [{"healthy": True}] * 10)
    cfg = _make_cfg(["gamma"])
    monkeypatch.setattr("pipewatch.cli_trend.load_config", lambda _: cfg)
    result = runner.invoke(trend_cmd, ["show", "--history-dir", hist, "--fail-on-degrading"])
    assert result.exit_code == 0
