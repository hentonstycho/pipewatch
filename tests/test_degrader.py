"""Tests for pipewatch.degrader."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.degrader import (
    DegradationResult,
    _failure_rate,
    detect_degradation,
    detect_all_degradations,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_history(hist_dir: Path, name: str, entries: list[dict]) -> None:
    pipeline_dir = hist_dir / name
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    path = pipeline_dir / "history.jsonl"
    with path.open("w") as fh:
        for entry in entries:
            fh.write(json.dumps(entry) + "\n")


def _make_entry(healthy: bool) -> dict:
    return {"pipeline": "p", "healthy": healthy, "checked_at": "2024-01-01T00:00:00"}


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path / "history"


# ---------------------------------------------------------------------------
# _failure_rate
# ---------------------------------------------------------------------------

def test_failure_rate_all_healthy():
    entries = [{"healthy": True}] * 5
    assert _failure_rate(entries) == 0.0


def test_failure_rate_all_failing():
    entries = [{"healthy": False}] * 4
    assert _failure_rate(entries) == 1.0


def test_failure_rate_mixed():
    entries = [{"healthy": True}, {"healthy": False}, {"healthy": False}, {"healthy": True}]
    assert _failure_rate(entries) == pytest.approx(0.5)


def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


# ---------------------------------------------------------------------------
# detect_degradation
# ---------------------------------------------------------------------------

def test_returns_none_when_insufficient_history(hist_dir: Path):
    entries = [_make_entry(True)] * 5
    _write_history(hist_dir, "pipe1", entries)
    result = detect_degradation("pipe1", history_dir=str(hist_dir), window=10)
    assert result is None


def test_not_degraded_when_stable(hist_dir: Path):
    # 20 healthy runs -> no degradation
    entries = [_make_entry(True)] * 20
    _write_history(hist_dir, "pipe1", entries)
    result = detect_degradation("pipe1", history_dir=str(hist_dir), window=10, threshold=0.20)
    assert result is not None
    assert result.degraded is False
    assert result.delta == pytest.approx(0.0)


def test_degraded_when_recent_failures_spike(hist_dir: Path):
    # baseline window: all healthy; recent window: all failing
    baseline = [_make_entry(True)] * 10
    recent = [_make_entry(False)] * 10
    _write_history(hist_dir, "pipe2", baseline + recent)
    result = detect_degradation("pipe2", history_dir=str(hist_dir), window=10, threshold=0.20)
    assert result is not None
    assert result.degraded is True
    assert result.recent_failure_rate == pytest.approx(1.0)
    assert result.baseline_failure_rate == pytest.approx(0.0)
    assert result.delta == pytest.approx(1.0)


def test_not_degraded_below_threshold(hist_dir: Path):
    # baseline: 0 failures, recent: 1/10 failures -> delta=0.10 < threshold=0.20
    baseline = [_make_entry(True)] * 10
    recent = [_make_entry(False)] + [_make_entry(True)] * 9
    _write_history(hist_dir, "pipe3", baseline + recent)
    result = detect_degradation("pipe3", history_dir=str(hist_dir), window=10, threshold=0.20)
    assert result is not None
    assert result.degraded is False


def test_str_representation(hist_dir: Path):
    baseline = [_make_entry(True)] * 10
    recent = [_make_entry(False)] * 10
    _write_history(hist_dir, "pipe4", baseline + recent)
    result = detect_degradation("pipe4", history_dir=str(hist_dir), window=10)
    assert "pipe4" in str(result)
    assert "degraded" in str(result)


# ---------------------------------------------------------------------------
# detect_all_degradations
# ---------------------------------------------------------------------------

def test_detect_all_skips_pipelines_with_no_history(hist_dir: Path):
    from pipewatch.config import PipewatchConfig, PipelineConfig, NotificationConfig

    cfg = PipewatchConfig(
        pipelines=[PipelineConfig(name="empty_pipe", source="", thresholds=None)],
        notifications=NotificationConfig(),
    )
    results = detect_all_degradations(cfg, history_dir=str(hist_dir), window=10)
    assert results == []


def test_detect_all_returns_result_for_each_pipeline_with_enough_history(hist_dir: Path):
    from pipewatch.config import PipewatchConfig, PipelineConfig, NotificationConfig

    for name in ("a", "b"):
        entries = [_make_entry(True)] * 20
        _write_history(hist_dir, name, entries)

    cfg = PipewatchConfig(
        pipelines=[
            PipelineConfig(name="a", source="", thresholds=None),
            PipelineConfig(name="b", source="", thresholds=None),
        ],
        notifications=NotificationConfig(),
    )
    results = detect_all_degradations(cfg, history_dir=str(hist_dir), window=10)
    assert len(results) == 2
    assert all(isinstance(r, DegradationResult) for r in results)
