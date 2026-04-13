"""Tests for pipewatch.cli_metrics."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_metrics import metrics_cmd
from pipewatch.checker import CheckResult
from pipewatch.metrics import PipelineMetrics


def _make_metrics(name, uptime=100.0, checks=10, failures=0):
    return PipelineMetrics(
        pipeline_name=name,
        total_checks=checks,
        total_failures=failures,
        avg_row_count=500.0,
        avg_error_rate=0.01,
        avg_latency_seconds=1.2,
        uptime_pct=uptime,
    )


@pytest.fixture()
def _mock_config():
    pipeline = MagicMock()
    pipeline.name = "orders"
    cfg = MagicMock()
    cfg.pipelines = [pipeline]
    with patch("pipewatch.cli_metrics.load_config", return_value=cfg):
        yield cfg


def test_metrics_cmd_prints_output(_mock_config):
    with patch(
        "pipewatch.cli_metrics.compute_all_metrics",
        return_value=[_make_metrics("orders")],
    ):
        runner = CliRunner()
        result = runner.invoke(metrics_cmd, ["--config", "pipewatch.yaml"])
    assert result.exit_code == 0
    assert "orders" in result.output
    assert "Uptime" in result.output
    assert "100.00%" in result.output


def test_metrics_cmd_unknown_pipeline_exits_2(_mock_config):
    runner = CliRunner()
    result = runner.invoke(
        metrics_cmd, ["--config", "pipewatch.yaml", "--pipeline", "ghost"]
    )
    assert result.exit_code == 2


def test_metrics_cmd_fail_below_exits_1_when_degraded(_mock_config):
    with patch(
        "pipewatch.cli_metrics.compute_all_metrics",
        return_value=[_make_metrics("orders", uptime=80.0, failures=2)],
    ):
        runner = CliRunner()
        result = runner.invoke(
            metrics_cmd, ["--config", "pipewatch.yaml", "--fail-below", "90"]
        )
    assert result.exit_code == 1
    assert "DEGRADED" in result.output + (result.stderr if hasattr(result, 'stderr') else "")


def test_metrics_cmd_fail_below_ok_when_healthy(_mock_config):
    with patch(
        "pipewatch.cli_metrics.compute_all_metrics",
        return_value=[_make_metrics("orders", uptime=99.5)],
    ):
        runner = CliRunner()
        result = runner.invoke(
            metrics_cmd, ["--config", "pipewatch.yaml", "--fail-below", "90"]
        )
    assert result.exit_code == 0


def test_metrics_cmd_single_pipeline(_mock_config):
    with patch(
        "pipewatch.cli_metrics.compute_all_metrics",
        return_value=[_make_metrics("orders")],
    ) as mock_compute:
        runner = CliRunner()
        result = runner.invoke(
            metrics_cmd, ["--config", "pipewatch.yaml", "--pipeline", "orders"]
        )
    mock_compute.assert_called_once_with(["orders"], history_dir=".pipewatch")
    assert result.exit_code == 0
