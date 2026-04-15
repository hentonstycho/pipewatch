"""Tests for pipewatch.cli_score."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_score import score_cmd
from pipewatch.config import PipelineConfig, ThresholdConfig, PipewatchConfig, NotificationConfig
from pipewatch.scorer import PipelineScore


@pytest.fixture()
def runner():
    return CliRunner()


def _make_cfg(*names: str) -> PipewatchConfig:
    pipelines = [
        PipelineConfig(
            name=n,
            source="db",
            thresholds=ThresholdConfig(min_row_count=None, max_error_rate=None, max_latency_seconds=None),
        )
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(slack_webhook=None, email_to=None, email_from=None, smtp_host=None),
    )


def _make_score(name: str, score: float = 85.0, grade: str = "B") -> PipelineScore:
    return PipelineScore(pipeline=name, score=score, grade=grade, reasons=["all recent checks healthy"])


# ---------------------------------------------------------------------------

def test_show_cmd_prints_pipeline_name(runner):
    cfg = _make_cfg("orders")
    scores = [_make_score("orders")]
    with patch("pipewatch.cli_score.load_config", return_value=cfg), \
         patch("pipewatch.cli_score.score_all", return_value=scores):
        result = runner.invoke(score_cmd, ["show", "--config", "pipewatch.yaml"])
    assert result.exit_code == 0
    assert "orders" in result.output


def test_show_cmd_single_pipeline(runner):
    cfg = _make_cfg("orders", "users")
    score = _make_score("orders")
    with patch("pipewatch.cli_score.load_config", return_value=cfg), \
         patch("pipewatch.cli_score.score_pipeline", return_value=score):
        result = runner.invoke(score_cmd, ["show", "--pipeline", "orders"])
    assert result.exit_code == 0
    assert "orders" in result.output


def test_show_cmd_unknown_pipeline_exits_2(runner):
    cfg = _make_cfg("orders")
    with patch("pipewatch.cli_score.load_config", return_value=cfg):
        result = runner.invoke(score_cmd, ["show", "--pipeline", "nonexistent"])
    assert result.exit_code == 2


def test_show_cmd_fail_below_exits_1_when_degraded(runner):
    cfg = _make_cfg("orders")
    scores = [_make_score("orders", score=30.0, grade="F")]
    with patch("pipewatch.cli_score.load_config", return_value=cfg), \
         patch("pipewatch.cli_score.score_all", return_value=scores):
        result = runner.invoke(score_cmd, ["show", "--fail-below", "50"])
    assert result.exit_code == 1


def test_show_cmd_no_exit_1_when_healthy(runner):
    cfg = _make_cfg("orders")
    scores = [_make_score("orders", score=95.0, grade="A")]
    with patch("pipewatch.cli_score.load_config", return_value=cfg), \
         patch("pipewatch.cli_score.score_all", return_value=scores):
        result = runner.invoke(score_cmd, ["show", "--fail-below", "50"])
    assert result.exit_code == 0


def test_show_cmd_prints_score_value(runner):
    cfg = _make_cfg("pipe")
    scores = [_make_score("pipe", score=72.5, grade="C")]
    with patch("pipewatch.cli_score.load_config", return_value=cfg), \
         patch("pipewatch.cli_score.score_all", return_value=scores):
        result = runner.invoke(score_cmd, ["show"])
    assert "72.5" in result.output
