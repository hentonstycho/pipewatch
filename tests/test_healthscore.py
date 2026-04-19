"""Tests for pipewatch.healthscore."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.healthscore import (
    HealthScore,
    _grade,
    compute_health_score,
    compute_all_health_scores,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_history(hist_dir: Path, name: str, entries: list[dict]) -> None:
    p = hist_dir / f"{name}.jsonl"
    with p.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    d = tmp_path / "history"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# _grade
# ---------------------------------------------------------------------------

def test_grade_a():
    assert _grade(95) == "A"
    assert _grade(90) == "A"

def test_grade_b():
    assert _grade(80) == "B"

def test_grade_c():
    assert _grade(65) == "C"

def test_grade_d():
    assert _grade(50) == "D"

def test_grade_f():
    assert _grade(30) == "F"


# ---------------------------------------------------------------------------
# compute_health_score
# ---------------------------------------------------------------------------

def test_no_history_returns_perfect_score(hist_dir: Path):
    result = compute_health_score("pipe_a", history_dir=str(hist_dir))
    assert result.score == 100
    assert result.grade == "A"
    assert result.total == 0
    assert result.avg_latency is None


def test_all_healthy(hist_dir: Path):
    entries = [{"healthy": True, "latency_seconds": 1.0}] * 10
    _write_history(hist_dir, "pipe_a", entries)
    result = compute_health_score("pipe_a", history_dir=str(hist_dir))
    assert result.score == 100
    assert result.failures == 0
    assert result.avg_latency == pytest.approx(1.0)


def test_half_failing(hist_dir: Path):
    entries = ([{"healthy": True}] * 5) + ([{"healthy": False}] * 5)
    _write_history(hist_dir, "pipe_b", entries)
    result = compute_health_score("pipe_b", history_dir=str(hist_dir))
    assert result.score == 50
    assert result.grade == "D"
    assert result.failures == 5


def test_window_limits_entries(hist_dir: Path):
    # 60 entries: first 10 fail, last 50 healthy → within window=50 all healthy
    entries = ([{"healthy": False}] * 10) + ([{"healthy": True}] * 50)
    _write_history(hist_dir, "pipe_c", entries)
    result = compute_health_score("pipe_c", history_dir=str(hist_dir), window=50)
    assert result.score == 100


# ---------------------------------------------------------------------------
# compute_all_health_scores
# ---------------------------------------------------------------------------

def test_compute_all_health_scores(hist_dir: Path):
    from unittest.mock import MagicMock
    cfg = MagicMock()
    cfg.pipelines = [MagicMock(name="p1"), MagicMock(name="p2")]
    cfg.pipelines[0].name = "p1"
    cfg.pipelines[1].name = "p2"

    results = compute_all_health_scores(cfg, history_dir=str(hist_dir))
    assert len(results) == 2
    assert all(isinstance(r, HealthScore) for r in results)
