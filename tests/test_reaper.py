"""Tests for pipewatch.reaper."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.reaper import ReaperResult, reap_all, reap_pipeline


UTC = timezone.utc
NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path / "history"


def _write_entry(hist_dir: Path, pipeline: str, ts: datetime) -> None:
    hist_dir.mkdir(parents=True, exist_ok=True)
    path = hist_dir / f"{pipeline}.jsonl"
    entry = {"pipeline": pipeline, "healthy": True, "checked_at": ts.isoformat()}
    with path.open("a") as fh:
        fh.write(json.dumps(entry) + "\n")


def test_no_history_is_dead(hist_dir: Path) -> None:
    result = reap_pipeline("p1", history_dir=str(hist_dir), threshold_hours=24.0, now=NOW)
    assert result.dead is True
    assert result.last_seen is None
    assert result.age_hours is None


def test_recent_entry_is_alive(hist_dir: Path) -> None:
    recent = NOW - timedelta(hours=1)
    _write_entry(hist_dir, "p1", recent)
    result = reap_pipeline("p1", history_dir=str(hist_dir), threshold_hours=24.0, now=NOW)
    assert result.dead is False
    assert result.age_hours == pytest.approx(1.0, abs=0.01)


def test_old_entry_is_dead(hist_dir: Path) -> None:
    old = NOW - timedelta(hours=48)
    _write_entry(hist_dir, "p1", old)
    result = reap_pipeline("p1", history_dir=str(hist_dir), threshold_hours=24.0, now=NOW)
    assert result.dead is True
    assert result.age_hours == pytest.approx(48.0, abs=0.01)


def test_uses_most_recent_entry(hist_dir: Path) -> None:
    _write_entry(hist_dir, "p1", NOW - timedelta(hours=50))
    _write_entry(hist_dir, "p1", NOW - timedelta(hours=2))
    result = reap_pipeline("p1", history_dir=str(hist_dir), threshold_hours=24.0, now=NOW)
    assert result.dead is False
    assert result.age_hours == pytest.approx(2.0, abs=0.01)


def test_custom_threshold(hist_dir: Path) -> None:
    _write_entry(hist_dir, "p1", NOW - timedelta(hours=3))
    result = reap_pipeline("p1", history_dir=str(hist_dir), threshold_hours=2.0, now=NOW)
    assert result.dead is True


def test_reap_all_returns_all_pipelines(hist_dir: Path) -> None:
    from unittest.mock import MagicMock

    cfg = MagicMock()
    cfg.history_dir = str(hist_dir)
    p1, p2 = MagicMock(), MagicMock()
    p1.name = "alpha"
    p2.name = "beta"
    cfg.pipelines = [p1, p2]

    _write_entry(hist_dir, "alpha", NOW - timedelta(hours=1))
    # beta has no history → dead

    results = reap_all(cfg, threshold_hours=24.0, now=NOW)
    assert len(results) == 2
    by_name = {r.pipeline: r for r in results}
    assert by_name["alpha"].dead is False
    assert by_name["beta"].dead is True


def test_reaper_result_str_dead() -> None:
    r = ReaperResult(pipeline="p", last_seen=None, age_hours=None, dead=True)
    assert "DEAD" in str(r)


def test_reaper_result_str_alive() -> None:
    ts = NOW - timedelta(hours=1)
    r = ReaperResult(pipeline="p", last_seen=ts, age_hours=1.0, dead=False)
    assert "ALIVE" in str(r)
