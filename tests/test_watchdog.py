"""Tests for pipewatch.watchdog."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.watchdog import check_staleness, check_all_staleness, StaleResult
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def hist_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _write_history(hist_dir: str, pipeline: str, checked_at: datetime) -> None:
    path = Path(hist_dir) / f"{pipeline}.jsonl"
    with path.open("a") as fh:
        fh.write(json.dumps({"pipeline": pipeline, "healthy": True, "checked_at": checked_at.isoformat()}) + "\n")


def _make_config(names: list[str]) -> PipewatchConfig:
    pipelines = [
        PipelineConfig(
            name=n,
            thresholds=ThresholdConfig(max_row_count=None, min_row_count=None, max_error_rate=None, max_latency_seconds=None),
        )
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(slack_webhook=None, email_to=None, email_from=None, smtp_host=None),
    )


def test_stale_when_no_history(hist_dir: str) -> None:
    result = check_staleness("pipe_a", hist_dir, threshold_seconds=600, now=FIXED_NOW)
    assert result.is_stale is True
    assert result.last_checked is None
    assert result.age_seconds is None


def test_not_stale_when_recent(hist_dir: str) -> None:
    recent = FIXED_NOW - timedelta(seconds=300)
    _write_history(hist_dir, "pipe_b", recent)
    result = check_staleness("pipe_b", hist_dir, threshold_seconds=600, now=FIXED_NOW)
    assert result.is_stale is False
    assert result.age_seconds == pytest.approx(300.0)


def test_stale_when_old(hist_dir: str) -> None:
    old = FIXED_NOW - timedelta(seconds=7200)
    _write_history(hist_dir, "pipe_c", old)
    result = check_staleness("pipe_c", hist_dir, threshold_seconds=3600, now=FIXED_NOW)
    assert result.is_stale is True
    assert result.age_seconds == pytest.approx(7200.0)


def test_uses_most_recent_entry(hist_dir: str) -> None:
    _write_history(hist_dir, "pipe_d", FIXED_NOW - timedelta(seconds=5000))
    _write_history(hist_dir, "pipe_d", FIXED_NOW - timedelta(seconds=100))
    result = check_staleness("pipe_d", hist_dir, threshold_seconds=600, now=FIXED_NOW)
    assert result.is_stale is False
    assert result.age_seconds == pytest.approx(100.0)


def test_check_all_staleness_returns_one_per_pipeline(hist_dir: str) -> None:
    cfg = _make_config(["alpha", "beta"])
    _write_history(hist_dir, "alpha", FIXED_NOW - timedelta(seconds=60))
    results = check_all_staleness(cfg, hist_dir, default_threshold_seconds=600, now=FIXED_NOW)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"alpha", "beta"}


def test_check_all_staleness_stale_flag_correct(hist_dir: str) -> None:
    cfg = _make_config(["gamma"])
    results = check_all_staleness(cfg, hist_dir, default_threshold_seconds=600, now=FIXED_NOW)
    assert results[0].is_stale is True
