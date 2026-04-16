"""Tests for pipewatch.anomaly and cli_anomaly."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.anomaly import detect_anomaly, detect_all_anomalies, _failure_rate
from pipewatch.cli_anomaly import anomaly_cmd


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_history(hist_dir: Path, pipeline: str, entries: list[dict]):
    p = hist_dir / f"{pipeline}.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


@pytest.fixture
def hist_dir(tmp_path):
    return tmp_path / "history"


# ---------------------------------------------------------------------------
# unit tests
# ---------------------------------------------------------------------------

def test_failure_rate_all_healthy():
    entries = [{"healthy": True}] * 10
    assert _failure_rate(entries) == 0.0


def test_failure_rate_all_failing():
    entries = [{"healthy": False}] * 4
    assert _failure_rate(entries) == 1.0


def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_no_anomaly_when_stable(hist_dir):
    entries = [{"healthy": i % 5 != 0} for i in range(25)]  # ~20% failure rate throughout
    _write_history(hist_dir, "pipe_a", entries)
    result = detect_anomaly("pipe_a", history_dir=str(hist_dir))
    assert not result.is_anomaly


def test_anomaly_detected_on_spike(hist_dir):
    # baseline: 1 failure in 20, recent: 4 failures in 5
    baseline = [{"healthy": i != 0} for i in range(20)]
    recent = [{"healthy": False}] * 4 + [{"healthy": True}]
    _write_history(hist_dir, "pipe_b", baseline + recent)
    result = detect_anomaly("pipe_b", history_dir=str(hist_dir), baseline_window=20, recent_window=5)
    assert result.is_anomaly
    assert result.spike_ratio > 2.0


def test_no_history_returns_no_anomaly(hist_dir):
    result = detect_anomaly("missing", history_dir=str(hist_dir))
    assert not result.is_anomaly
    assert result.recent_failure_rate == 0.0


def test_zero_baseline_with_recent_failures_is_anomaly(hist_dir):
    entries = [{"healthy": True}] * 20 + [{"healthy": False}] * 5
    _write_history(hist_dir, "pipe_c", entries)
    result = detect_anomaly("pipe_c", history_dir=str(hist_dir))
    assert result.is_anomaly
    assert result.spike_ratio == float("inf")


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

@pytest.fixture
def _mock_config(tmp_path, monkeypatch):
    from unittest.mock import MagicMock
    cfg = MagicMock()
    cfg.pipelines = [MagicMock(name="pipe_a"), MagicMock(name="pipe_b")]
    for p in cfg.pipelines:
        p.name = p.name  # already set above
    monkeypatch.setattr("pipewatch.cli_anomaly.load_config", lambda _: cfg)
    return cfg


def test_check_cmd_prints_output(_mock_config, hist_dir, monkeypatch):
    monkeypatch.setattr(
        "pipewatch.cli_anomaly.detect_all_anomalies",
        lambda cfg, **kw: [],
    )
    runner = CliRunner()
    result = runner.invoke(anomaly_cmd, ["check"])
    assert result.exit_code == 0


def test_check_cmd_exits_1_on_anomaly(_mock_config, monkeypatch):
    from pipewatch.anomaly import AnomalyResult
    anomaly = AnomalyResult("pipe_a", 0.05, 0.8, 16.0, True)
    monkeypatch.setattr(
        "pipewatch.cli_anomaly.detect_all_anomalies",
        lambda cfg, **kw: [anomaly],
    )
    runner = CliRunner()
    result = runner.invoke(anomaly_cmd, ["check", "--fail-on-anomaly"])
    assert result.exit_code == 1


def test_check_cmd_unknown_pipeline_exits_2(_mock_config):
    runner = CliRunner()
    result = runner.invoke(anomaly_cmd, ["check", "--pipeline", "nonexistent"])
    assert result.exit_code == 2
