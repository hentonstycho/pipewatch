"""Tests for pipewatch.scorer."""

from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import datetime, timezone

import pytest

from pipewatch.config import PipelineConfig, ThresholdConfig
from pipewatch.scorer import score_pipeline, score_all, _grade, PipelineScore


@pytest.fixture()
def hist_dir(tmp_path: Path) -> str:
    return str(tmp_path / "history")


def _write_result(hist_dir: str, name: str, healthy: bool) -> None:
    os.makedirs(hist_dir, exist_ok=True)
    path = os.path.join(hist_dir, f"{name}.jsonl")
    entry = {
        "pipeline": name,
        "healthy": healthy,
        "violations": [],
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(path, "a") as fh:
        fh.write(json.dumps(entry) + "\n")


def _cfg(name: str = "pipe") -> PipelineConfig:
    return PipelineConfig(
        name=name,
        source="db",
        thresholds=ThresholdConfig(min_row_count=None, max_error_rate=None, max_latency_seconds=None),
    )


# ---------------------------------------------------------------------------
# _grade
# ---------------------------------------------------------------------------

def test_grade_a():
    assert _grade(95) == "A"


def test_grade_b():
    assert _grade(80) == "B"


def test_grade_f():
    assert _grade(30) == "F"


# ---------------------------------------------------------------------------
# score_pipeline
# ---------------------------------------------------------------------------

def test_score_no_history_returns_50(hist_dir):
    ps = score_pipeline(_cfg(), history_dir=hist_dir)
    assert ps.score == 50.0
    assert ps.grade == "C"
    assert any("no history" in r for r in ps.reasons)


def test_score_all_healthy_is_high(hist_dir):
    for _ in range(10):
        _write_result(hist_dir, "pipe", healthy=True)
    ps = score_pipeline(_cfg(), history_dir=hist_dir)
    assert ps.score >= 90.0
    assert ps.grade == "A"


def test_score_all_failing_is_low(hist_dir):
    for _ in range(10):
        _write_result(hist_dir, "pipe", healthy=False)
    ps = score_pipeline(_cfg(), history_dir=hist_dir)
    assert ps.score <= 20.0
    assert ps.grade == "F"


def test_score_mixed_history(hist_dir):
    for _ in range(7):
        _write_result(hist_dir, "pipe", healthy=True)
    for _ in range(3):
        _write_result(hist_dir, "pipe", healthy=False)
    ps = score_pipeline(_cfg(), history_dir=hist_dir)
    assert 40.0 < ps.score < 100.0


def test_score_pipeline_returns_pipeline_name(hist_dir):
    _write_result(hist_dir, "my_pipe", healthy=True)
    ps = score_pipeline(_cfg("my_pipe"), history_dir=hist_dir)
    assert ps.pipeline == "my_pipe"


# ---------------------------------------------------------------------------
# score_all
# ---------------------------------------------------------------------------

def test_score_all_sorted_worst_first(hist_dir):
    for _ in range(10):
        _write_result(hist_dir, "good", healthy=True)
    for _ in range(10):
        _write_result(hist_dir, "bad", healthy=False)
    cfgs = [_cfg("good"), _cfg("bad")]
    scores = score_all(cfgs, history_dir=hist_dir)
    assert scores[0].pipeline == "bad"
    assert scores[1].pipeline == "good"


def test_score_all_returns_all_pipelines(hist_dir):
    cfgs = [_cfg("a"), _cfg("b"), _cfg("c")]
    scores = score_all(cfgs, history_dir=hist_dir)
    assert len(scores) == 3
