"""Tests for pipewatch.cli_heatmap"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_heatmap import heatmap_cmd
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig
from pipewatch.heatmap import HeatmapRow


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def _mock_config() -> PipewatchConfig:
    return PipewatchConfig(
        pipelines=[
            PipelineConfig(name="alpha", thresholds=ThresholdConfig()),
            PipelineConfig(name="beta", thresholds=ThresholdConfig()),
        ],
        notifications=NotificationConfig(),
    )


def _make_rows(*names: str) -> list[HeatmapRow]:
    rows = []
    for name in names:
        row = HeatmapRow(pipeline=name)
        row.buckets[6] = 2
        rows.append(row)
    return rows


def test_show_cmd_prints_output(runner, _mock_config):
    with patch("pipewatch.cli_heatmap.load_config", return_value=_mock_config), \
         patch("pipewatch.cli_heatmap.build_heatmap", return_value=_make_rows("alpha", "beta")):
        result = runner.invoke(heatmap_cmd, ["show"])
    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "beta" in result.output


def test_show_cmd_unknown_pipeline_exits_2(runner, _mock_config):
    with patch("pipewatch.cli_heatmap.load_config", return_value=_mock_config):
        result = runner.invoke(heatmap_cmd, ["show", "--pipeline", "ghost"])
    assert result.exit_code == 2


def test_show_cmd_single_pipeline_filters(runner, _mock_config):
    rows = _make_rows("alpha", "beta")
    with patch("pipewatch.cli_heatmap.load_config", return_value=_mock_config), \
         patch("pipewatch.cli_heatmap.build_heatmap", return_value=rows):
        result = runner.invoke(heatmap_cmd, ["show", "--pipeline", "alpha"])
    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "beta" not in result.output


def test_show_cmd_fail_if_peak_exits_1_when_threshold_met(runner, _mock_config):
    rows = _make_rows("alpha")
    rows[0].buckets[6] = 5
    with patch("pipewatch.cli_heatmap.load_config", return_value=_mock_config), \
         patch("pipewatch.cli_heatmap.build_heatmap", return_value=rows):
        result = runner.invoke(heatmap_cmd, ["show", "--fail-if-peak", "5"])
    assert result.exit_code == 1


def test_show_cmd_fail_if_peak_no_exit_when_below_threshold(runner, _mock_config):
    rows = _make_rows("alpha")
    rows[0].buckets[6] = 2
    with patch("pipewatch.cli_heatmap.load_config", return_value=_mock_config), \
         patch("pipewatch.cli_heatmap.build_heatmap", return_value=rows):
        result = runner.invoke(heatmap_cmd, ["show", "--fail-if-peak", "10"])
    assert result.exit_code == 0
