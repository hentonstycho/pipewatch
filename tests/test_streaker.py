"""Tests for pipewatch.streaker."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.streaker import _compute_streak, compute_all_streaks, StreakResult
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_history(hist_dir: Path, pipeline: str, entries: list[dict]) -> None:
    path = hist_dir / f"{pipeline}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path / "history"


def _make_config(names: list[str]) -> PipewatchConfig:
    pipelines = [
        PipelineConfig(
            name=n,
            query="SELECT 1",
            thresholds=ThresholdConfig(),
        )
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(),
    )


# ---------------------------------------------------------------------------
# _compute_streak
# ---------------------------------------------------------------------------

def test_no_history_returns_zero_streak(hist_dir: Path) -> None:
    hist_dir.mkdir(parents=True, exist_ok=True)
    result = _compute_streak("pipe_a", history_dir=str(hist_dir))
    assert result.current_streak == 0
    assert result.best_streak == 0
    assert result.last_status is None


def test_all_healthy_streak(hist_dir: Path) -> None:
    _write_history(hist_dir, "pipe_a", [{"healthy": True}] * 5)
    result = _compute_streak("pipe_a", history_dir=str(hist_dir))
    assert result.current_streak == 5
    assert result.best_streak == 5
    assert result.last_status == "ok"


def test_streak_resets_after_failure(hist_dir: Path) -> None:
    entries = [{"healthy": True}] * 3 + [{"healthy": False}] + [{"healthy": True}] * 2
    _write_history(hist_dir, "pipe_b", entries)
    result = _compute_streak("pipe_b", history_dir=str(hist_dir))
    assert result.current_streak == 2
    assert result.best_streak == 3


def test_last_run_failed_gives_zero_current(hist_dir: Path) -> None:
    entries = [{"healthy": True}] * 4 + [{"healthy": False}]
    _write_history(hist_dir, "pipe_c", entries)
    result = _compute_streak("pipe_c", history_dir=str(hist_dir))
    assert result.current_streak == 0
    assert result.best_streak == 4
    assert result.last_status == "fail"


def test_single_failure_entry(hist_dir: Path) -> None:
    _write_history(hist_dir, "pipe_d", [{"healthy": False}])
    result = _compute_streak("pipe_d", history_dir=str(hist_dir))
    assert result.current_streak == 0
    assert result.best_streak == 0
    assert result.last_status == "fail"


# ---------------------------------------------------------------------------
# compute_all_streaks
# ---------------------------------------------------------------------------

def test_compute_all_streaks_returns_one_per_pipeline(hist_dir: Path) -> None:
    hist_dir.mkdir(parents=True, exist_ok=True)
    cfg = _make_config(["alpha", "beta", "gamma"])
    results = compute_all_streaks(cfg, history_dir=str(hist_dir))
    assert len(results) == 3
    assert {r.pipeline for r in results} == {"alpha", "beta", "gamma"}


def test_compute_all_streaks_mixed(hist_dir: Path) -> None:
    _write_history(hist_dir, "alpha", [{"healthy": True}] * 3)
    _write_history(hist_dir, "beta", [{"healthy": False}])
    cfg = _make_config(["alpha", "beta"])
    results = {r.pipeline: r for r in compute_all_streaks(cfg, history_dir=str(hist_dir))}
    assert results["alpha"].current_streak == 3
    assert results["beta"].current_streak == 0
