"""Tests for pipewatch.cli_digest."""

from __future__ import annotations

import datetime
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_digest import digest_cmd
from pipewatch.digest import Digest, DigestEntry
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig


@pytest.fixture()
def _mock_config():
    return PipewatchConfig(
        pipelines=[
            PipelineConfig(
                name="sales",
                thresholds=ThresholdConfig(min_row_count=1),
            )
        ],
        notifications=None,
    )


@pytest.fixture()
def _mock_digest():
    return Digest(
        generated_at=datetime.datetime(2024, 6, 1, 8, 0, 0),
        overall_healthy=True,
        entries=[
            DigestEntry(
                pipeline="sales",
                success_rate=1.0,
                avg_latency_seconds=3.2,
                avg_row_count=200.0,
                total_checks=5,
                consecutive_failures=0,
            )
        ],
    )


def test_digest_cmd_prints_output(_mock_config, _mock_digest):
    runner = CliRunner()
    with patch("pipewatch.cli_digest.load_config", return_value=_mock_config), \
         patch("pipewatch.cli_digest.build_digest", return_value=_mock_digest):
        result = runner.invoke(digest_cmd, ["--config", "pipewatch.yaml"])
    assert result.exit_code == 0
    assert "sales" in result.output
    assert "OK" in result.output


def test_digest_cmd_exits_1_when_degraded_and_flag_set(_mock_config):
    degraded = Digest(
        generated_at=datetime.datetime(2024, 6, 1, 8, 0, 0),
        overall_healthy=False,
        entries=[],
    )
    runner = CliRunner()
    with patch("pipewatch.cli_digest.load_config", return_value=_mock_config), \
         patch("pipewatch.cli_digest.build_digest", return_value=degraded):
        result = runner.invoke(digest_cmd, ["--fail-degraded"])
    assert result.exit_code == 1


def test_digest_cmd_no_exit_1_when_healthy_and_flag_set(_mock_config, _mock_digest):
    runner = CliRunner()
    with patch("pipewatch.cli_digest.load_config", return_value=_mock_config), \
         patch("pipewatch.cli_digest.build_digest", return_value=_mock_digest):
        result = runner.invoke(digest_cmd, ["--fail-degraded"])
    assert result.exit_code == 0


def test_digest_cmd_missing_config_exits_2():
    runner = CliRunner()
    result = runner.invoke(digest_cmd, ["--config", "nonexistent.yaml"])
    assert result.exit_code == 2
