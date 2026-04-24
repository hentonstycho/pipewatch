"""Tests for pipewatch.cli_graph."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_graph import graph_cmd
from pipewatch.grapher import GraphResult


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _make_cfg(names: list[str]):
    from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig
    return PipewatchConfig(
        pipelines=[PipelineConfig(name=n, thresholds=ThresholdConfig()) for n in names],
        notifications=NotificationConfig(),
    )


def _make_graph(name: str, ok: int = 5, fail: int = 0) -> GraphResult:
    total = ok + fail
    spark = "█" * ok + " " * fail
    return GraphResult(pipeline=name, sparkline=spark, window=30, total=total, failures=fail)


def test_show_cmd_prints_output(runner: CliRunner):
    cfg = _make_cfg(["pipe_a"])
    graphs = [_make_graph("pipe_a")]
    with patch("pipewatch.cli_graph.load_config", return_value=cfg), \
         patch("pipewatch.cli_graph.build_all_graphs", return_value=graphs):
        result = runner.invoke(graph_cmd, ["show"])
    assert result.exit_code == 0
    assert "pipe_a" in result.output


def test_show_cmd_single_pipeline(runner: CliRunner):
    cfg = _make_cfg(["pipe_a", "pipe_b"])
    graph = _make_graph("pipe_a")
    with patch("pipewatch.cli_graph.load_config", return_value=cfg), \
         patch("pipewatch.cli_graph.build_graph", return_value=graph):
        result = runner.invoke(graph_cmd, ["show", "--pipeline", "pipe_a"])
    assert result.exit_code == 0
    assert "pipe_a" in result.output


def test_show_cmd_unknown_pipeline_exits_2(runner: CliRunner):
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_graph.load_config", return_value=cfg):
        result = runner.invoke(graph_cmd, ["show", "--pipeline", "ghost"])
    assert result.exit_code == 2


def test_show_cmd_no_history_prints_message(runner: CliRunner):
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_graph.load_config", return_value=cfg), \
         patch("pipewatch.cli_graph.build_graph", return_value=None):
        result = runner.invoke(graph_cmd, ["show", "--pipeline", "pipe_a"])
    assert result.exit_code == 0
    assert "no history" in result.output


def test_show_cmd_no_results_prints_message(runner: CliRunner):
    cfg = _make_cfg(["pipe_a"])
    with patch("pipewatch.cli_graph.load_config", return_value=cfg), \
         patch("pipewatch.cli_graph.build_all_graphs", return_value=[]):
        result = runner.invoke(graph_cmd, ["show"])
    assert result.exit_code == 0
    assert "No history" in result.output
