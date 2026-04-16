"""Tests for pipewatch.flapper."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.flapper import FlapResult, _count_transitions, detect_flap, detect_all_flaps
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_history(hist_dir: Path, pipeline: str, statuses: list[bool]) -> None:
    path = hist_dir / f"{pipeline}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for s in statuses:
            fh.write(json.dumps({"healthy": s, "pipeline": pipeline}) + "\n")


def _make_config(names: list[str]) -> PipewatchConfig:
    pipelines = [
        PipelineConfig(name=n, thresholds=ThresholdConfig())
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(),
    )


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path / "history"


# ---------------------------------------------------------------------------
# unit tests
# ---------------------------------------------------------------------------

def test_count_transitions_stable():
    assert _count_transitions([True, True, True]) == 0


def test_count_transitions_alternating():
    assert _count_transitions([True, False, True, False]) == 3


def test_count_transitions_single():
    assert _count_transitions([True]) == 0


def test_no_history_returns_not_flapping(hist_dir: Path):
    result = detect_flap("pipe", hist_dir)
    assert isinstance(result, FlapResult)
    assert result.is_flapping is False
    assert result.transitions == 0
    assert result.recent_statuses == []


def test_stable_healthy_not_flapping(hist_dir: Path):
    _write_history(hist_dir, "pipe", [True] * 10)
    result = detect_flap("pipe", hist_dir)
    assert result.is_flapping is False
    assert result.transitions == 0


def test_alternating_is_flapping(hist_dir: Path):
    statuses = [True, False] * 5  # 9 transitions
    _write_history(hist_dir, "pipe", statuses)
    result = detect_flap("pipe", hist_dir, window=10, threshold=4)
    assert result.is_flapping is True
    assert result.transitions >= 4


def test_window_limits_history(hist_dir: Path):
    # 20 entries but only last 5 are stable
    _write_history(hist_dir, "pipe", [True, False] * 10 + [True] * 5)
    result = detect_flap("pipe", hist_dir, window=5, threshold=4)
    assert result.is_flapping is False
    assert len(result.recent_statuses) == 5


def test_detect_all_flaps_returns_one_per_pipeline(hist_dir: Path):
    cfg = _make_config(["a", "b", "c"])
    for name in ["a", "b", "c"]:
        _write_history(hist_dir, name, [True, False, True, False, True])
    results = detect_all_flaps(cfg, hist_dir)
    assert len(results) == 3
    assert {r.pipeline for r in results} == {"a", "b", "c"}


def test_detect_all_flaps_identifies_flapping_pipeline(hist_dir: Path):
    cfg = _make_config(["stable", "flappy"])
    _write_history(hist_dir, "stable", [True] * 10)
    _write_history(hist_dir, "flappy", [True, False] * 5)
    results = {r.pipeline: r for r in detect_all_flaps(cfg, hist_dir)}
    assert results["stable"].is_flapping is False
    assert results["flappy"].is_flapping is True
