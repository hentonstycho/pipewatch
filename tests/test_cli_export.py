"""Tests for pipewatch.cli_export."""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pipewatch.cli_export import export_cmd
from pipewatch.metrics import PipelineMetrics


def _make_metrics(name: str = "pipe_a") -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name=name,
        success_rate=1.0,
        avg_latency=10.0,
        avg_error_rate=0.0,
        total_checks=5,
        consecutive_failures=0,
    )


def _mock_config(pipeline_names: list[str]):
    cfg = MagicMock()
    cfg.pipelines = [MagicMock(name=n) for n in pipeline_names]
    for p, n in zip(cfg.pipelines, pipeline_names):
        p.name = n
    return cfg


@pytest.fixture()
def runner():
    return CliRunner()


def test_export_cmd_prints_prometheus_output(runner, tmp_path):
    cfg = _mock_config(["orders"])
    metrics = [_make_metrics("orders")]
    with patch("pipewatch.cli_export.compute_all_metrics", return_value=metrics):
        result = runner.invoke(export_cmd, [], obj={"config": cfg})
    assert result.exit_code == 0
    assert "pipewatch_success_rate" in result.output


def test_export_cmd_unknown_pipeline_exits_2(runner):
    cfg = _mock_config(["orders"])
    result = runner.invoke(export_cmd, ["--pipeline", "nonexistent"], obj={"config": cfg})
    assert result.exit_code == 2


def test_export_cmd_single_pipeline_filter(runner):
    cfg = _mock_config(["orders", "users"])
    metrics = [_make_metrics("orders")]
    with patch("pipewatch.cli_export.compute_all_metrics", return_value=metrics) as mock_compute:
        result = runner.invoke(export_cmd, ["--pipeline", "orders"], obj={"config": cfg})
    assert result.exit_code == 0
    called_pipelines = mock_compute.call_args[0][0]
    assert len(called_pipelines) == 1
    assert called_pipelines[0].name == "orders"


def test_export_cmd_writes_file(runner, tmp_path):
    cfg = _mock_config(["pipe_a"])
    out_file = tmp_path / "metrics.txt"
    metrics = [_make_metrics("pipe_a")]
    with patch("pipewatch.cli_export.compute_all_metrics", return_value=metrics):
        result = runner.invoke(export_cmd, ["--output", str(out_file)], obj={"config": cfg})
    assert result.exit_code == 0
    assert out_file.exists()
    content = out_file.read_text()
    assert "pipewatch_success_rate" in content


def test_export_cmd_eof_in_output(runner):
    cfg = _mock_config(["pipe_a"])
    metrics = [_make_metrics("pipe_a")]
    with patch("pipewatch.cli_export.compute_all_metrics", return_value=metrics):
        result = runner.invoke(export_cmd, [], obj={"config": cfg})
    assert "# EOF" in result.output
