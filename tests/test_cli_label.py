"""Tests for pipewatch/cli_label.py"""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.cli_label import label_cmd
from pipewatch.checker import CheckResult
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig
from pipewatch.labeler import LabeledResult, SEVERITY_OK, SEVERITY_CRITICAL


@pytest.fixture()
def runner():
    return CliRunner()


def _make_cfg(names: list[str] = None) -> PipewatchConfig:
    names = names or ["alpha", "beta"]
    pipelines = [
        PipelineConfig(
            name=n,
            thresholds=ThresholdConfig(),
            schedule=None,
        )
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(),
    )


def _lr(name: str, severity: str) -> LabeledResult:
    result = CheckResult(pipeline=name, healthy=(severity == SEVERITY_OK), violations=[])
    return LabeledResult(result=result, severity=severity, reason=None)


def test_show_cmd_prints_pipeline_names(runner, tmp_path):
    cfg = _make_cfg(["alpha", "beta"])
    labeled = [_lr("alpha", SEVERITY_OK), _lr("beta", SEVERITY_OK)]

    with patch("pipewatch.cli_label.check_all_pipelines", return_value=[]) as _chk, \
         patch("pipewatch.cli_label.label_all", return_value=labeled):
        result = runner.invoke(
            label_cmd, ["show", "--history-dir", str(tmp_path)], obj=cfg
        )

    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "beta" in result.output


def test_show_cmd_filters_by_pipeline(runner, tmp_path):
    cfg = _make_cfg(["alpha", "beta"])
    labeled = [_lr("alpha", SEVERITY_OK)]

    with patch("pipewatch.cli_label.check_all_pipelines", return_value=[]), \
         patch("pipewatch.cli_label.label_all", return_value=labeled):
        result = runner.invoke(
            label_cmd,
            ["show", "--pipeline", "alpha", "--history-dir", str(tmp_path)],
            obj=cfg,
        )

    assert result.exit_code == 0
    assert "alpha" in result.output


def test_show_cmd_unknown_pipeline_exits_2(runner, tmp_path):
    cfg = _make_cfg(["alpha"])
    result = runner.invoke(
        label_cmd,
        ["show", "--pipeline", "unknown", "--history-dir", str(tmp_path)],
        obj=cfg,
    )
    assert result.exit_code == 2


def test_show_cmd_exits_1_on_critical_when_flag_set(runner, tmp_path):
    cfg = _make_cfg(["alpha"])
    labeled = [_lr("alpha", SEVERITY_CRITICAL)]

    with patch("pipewatch.cli_label.check_all_pipelines", return_value=[]), \
         patch("pipewatch.cli_label.label_all", return_value=labeled):
        result = runner.invoke(
            label_cmd,
            ["show", "--fail-on-critical", "--history-dir", str(tmp_path)],
            obj=cfg,
        )

    assert result.exit_code == 1


def test_show_cmd_no_exit_1_when_healthy_and_flag_set(runner, tmp_path):
    cfg = _make_cfg(["alpha"])
    labeled = [_lr("alpha", SEVERITY_OK)]

    with patch("pipewatch.cli_label.check_all_pipelines", return_value=[]), \
         patch("pipewatch.cli_label.label_all", return_value=labeled):
        result = runner.invoke(
            label_cmd,
            ["show", "--fail-on-critical", "--history-dir", str(tmp_path)],
            obj=cfg,
        )

    assert result.exit_code == 0
