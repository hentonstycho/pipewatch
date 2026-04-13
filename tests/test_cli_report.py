"""Tests for the `pipewatch report` CLI sub-command."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pipewatch.cli_report import report_cmd
from pipewatch.reporter import Report, PipelineSummary
from datetime import datetime, timezone


HEALTHY_REPORT = Report(
    generated_at=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
    summaries=[
        PipelineSummary(
            pipeline_name="orders",
            total_checks=10,
            failures=0,
            success_rate=100.0,
            last_checked=datetime(2024, 1, 15, 9, 55, tzinfo=timezone.utc),
            last_status="OK",
        )
    ],
)

DEGRADED_REPORT = Report(
    generated_at=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
    summaries=[
        PipelineSummary(
            pipeline_name="orders",
            total_checks=5,
            failures=3,
            success_rate=40.0,
            last_checked=datetime(2024, 1, 15, 9, 55, tzinfo=timezone.utc),
            last_status="FAIL",
        )
    ],
)


def _mock_config(pipeline_names=("orders",)):
    cfg = MagicMock()
    cfg.pipelines = [MagicMock(name=n) for n in pipeline_names]
    for p, n in zip(cfg.pipelines, pipeline_names):
        p.name = n
    return cfg


def test_report_cmd_prints_output():
    runner = CliRunner()
    with patch("pipewatch.cli_report.load_config", return_value=_mock_config()), \
         patch("pipewatch.cli_report.build_report", return_value=HEALTHY_REPORT):
        result = runner.invoke(report_cmd, ["--config", "pipewatch.yaml"])
    assert result.exit_code == 0
    assert "orders" in result.output
    assert "HEALTHY" in result.output


def test_report_cmd_exits_1_when_degraded_and_flag_set():
    runner = CliRunner()
    with patch("pipewatch.cli_report.load_config", return_value=_mock_config()), \
         patch("pipewatch.cli_report.build_report", return_value=DEGRADED_REPORT):
        result = runner.invoke(report_cmd, ["--fail-on-degraded"])
    assert result.exit_code == 1


def test_report_cmd_no_exit_1_when_healthy_and_flag_set():
    runner = CliRunner()
    with patch("pipewatch.cli_report.load_config", return_value=_mock_config()), \
         patch("pipewatch.cli_report.build_report", return_value=HEALTHY_REPORT):
        result = runner.invoke(report_cmd, ["--fail-on-degraded"])
    assert result.exit_code == 0


def test_report_cmd_unknown_pipeline_exits_2():
    runner = CliRunner()
    with patch("pipewatch.cli_report.load_config", return_value=_mock_config()):
        result = runner.invoke(report_cmd, ["--pipeline", "nonexistent"])
    assert result.exit_code == 2


def test_report_cmd_missing_config_exits_2():
    runner = CliRunner()
    with patch("pipewatch.cli_report.load_config", side_effect=FileNotFoundError):
        result = runner.invoke(report_cmd, [])
    assert result.exit_code == 2
