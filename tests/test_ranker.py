"""Tests for pipewatch.ranker."""
from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import datetime, timezone

import pytest

from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig
from pipewatch.ranker import rank_pipelines, _score, PipelineRank
from pipewatch.metrics import PipelineMetrics


def _make_config(*names: str, max_latency: float = 60.0) -> PipewatchConfig:
    pipelines = [
        PipelineConfig(
            name=n,
            thresholds=ThresholdConfig(max_latency_seconds=max_latency),
        )
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(),
    )


def _write_result(hist_dir: Path, pipeline: str, healthy: bool, ts: str) -> None:
    path = hist_dir / f"{pipeline}.jsonl"
    entry = {"pipeline": pipeline, "healthy": healthy, "checked_at": ts, "violations": []}
    with open(path, "a") as fh:
        fh.write(json.dumps(entry) + "\n")


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    d = tmp_path / "history"
    d.mkdir()
    return d


def test_rank_returns_all_pipelines(hist_dir: Path) -> None:
    cfg = _make_config("alpha", "beta", "gamma")
    ranks = rank_pipelines(cfg, history_dir=str(hist_dir))
    assert {r.pipeline for r in ranks} == {"alpha", "beta", "gamma"}


def test_rank_worst_first(hist_dir: Path) -> None:
    cfg = _make_config("good", "bad")
    ts = datetime.now(timezone.utc).isoformat()
    # "bad" has 3 failures, "good" has none
    for _ in range(3):
        _write_result(hist_dir, "bad", False, ts)
    _write_result(hist_dir, "good", True, ts)

    ranks = rank_pipelines(cfg, history_dir=str(hist_dir))
    assert ranks[0].pipeline == "bad"
    assert ranks[-1].pipeline == "good"


def test_rank_scores_non_negative(hist_dir: Path) -> None:
    cfg = _make_config("pipe1", "pipe2")
    ts = datetime.now(timezone.utc).isoformat()
    _write_result(hist_dir, "pipe1", True, ts)
    _write_result(hist_dir, "pipe2", False, ts)

    ranks = rank_pipelines(cfg, history_dir=str(hist_dir))
    for r in ranks:
        assert r.score >= 0.0


def test_rank_empty_history_scores_zero(hist_dir: Path) -> None:
    cfg = _make_config("lonely")
    ranks = rank_pipelines(cfg, history_dir=str(hist_dir))
    assert ranks[0].score == 0.0
    assert ranks[0].failure_rate == 0.0
    assert ranks[0].consecutive_failures == 0


def test_score_all_bad_capped_below_one() -> None:
    m = PipelineMetrics(pipeline="x", failure_rate=1.0, avg_latency=200.0)
    s = _score(m, consec=10, latency_threshold=100.0)
    # max: 0.5 + 0.6 + 0.2 = 1.3 (latency ratio capped at 2.0 → 0.6)
    assert 0.0 <= s <= 1.5


def test_score_perfect_pipeline_is_zero() -> None:
    m = PipelineMetrics(pipeline="ok", failure_rate=0.0, avg_latency=0.0)
    s = _score(m, consec=0, latency_threshold=60.0)
    assert s == 0.0


def test_rank_pipeline_rank_has_latency_threshold(hist_dir: Path) -> None:
    cfg = _make_config("p", max_latency=120.0)
    ranks = rank_pipelines(cfg, history_dir=str(hist_dir))
    assert ranks[0].latency_threshold == 120.0
