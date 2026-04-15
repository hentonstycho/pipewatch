"""Tests for pipewatch.summarizer."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.config import PipelineConfig, ThresholdConfig
from pipewatch.summarizer import (
    PipelineSummaryLine,
    format_summary_line,
    summarise_all,
    summarise_pipeline,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_result(hist_dir: Path, name: str, healthy: bool, message: str = "") -> None:
    hist_dir.mkdir(parents=True, exist_ok=True)
    entry = {
        "pipeline": name,
        "healthy": healthy,
        "message": message,
        "checked_at": _ts(),
        "row_count": 100,
        "error_rate": 0.01,
        "latency_seconds": 5.0,
    }
    path = hist_dir / f"{name}.jsonl"
    with open(path, "a") as fh:
        fh.write(json.dumps(entry) + "\n")


def _pipeline(name: str) -> PipelineConfig:
    return PipelineConfig(
        name=name,
        thresholds=ThresholdConfig(min_row_count=10, max_error_rate=0.1, max_latency_seconds=60),
        schedule=None,
    )


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path / "history"


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_summarise_pipeline_no_data(hist_dir: Path) -> None:
    line = summarise_pipeline(_pipeline("alpha"), history_dir=str(hist_dir))
    assert line.status == "NO DATA"
    assert line.consecutive_fails == 0
    assert line.avg_row_count is None


def test_summarise_pipeline_healthy(hist_dir: Path) -> None:
    _write_result(hist_dir, "alpha", healthy=True)
    line = summarise_pipeline(_pipeline("alpha"), history_dir=str(hist_dir))
    assert line.status == "OK"
    assert line.consecutive_fails == 0


def test_summarise_pipeline_failing(hist_dir: Path) -> None:
    _write_result(hist_dir, "beta", healthy=False, message="row count too low")
    _write_result(hist_dir, "beta", healthy=False, message="row count too low")
    line = summarise_pipeline(_pipeline("beta"), history_dir=str(hist_dir))
    assert line.status == "FAIL"
    assert line.consecutive_fails == 2
    assert line.last_message == "row count too low"


def test_format_summary_line_ok(hist_dir: Path) -> None:
    _write_result(hist_dir, "gamma", healthy=True)
    line = summarise_pipeline(_pipeline("gamma"), history_dir=str(hist_dir))
    text = format_summary_line(line)
    assert "gamma" in text
    assert "OK" in text
    assert "✅" in text


def test_format_summary_line_fail_contains_consec(hist_dir: Path) -> None:
    for _ in range(3):
        _write_result(hist_dir, "delta", healthy=False)
    line = summarise_pipeline(_pipeline("delta"), history_dir=str(hist_dir))
    text = format_summary_line(line)
    assert "consec_fails=3" in text
    assert "❌" in text


def test_summarise_all_returns_one_per_pipeline(hist_dir: Path) -> None:
    pipelines = [_pipeline("p1"), _pipeline("p2"), _pipeline("p3")]
    lines = summarise_all(pipelines, history_dir=str(hist_dir))
    assert len(lines) == 3
    names = {l.pipeline for l in lines}
    assert names == {"p1", "p2", "p3"}
