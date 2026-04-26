"""Tests for pipewatch.cadence."""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipewatch.cadence import (
    CadenceResult,
    check_all_cadences,
    check_cadence,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _ts(dt: datetime) -> str:
    return dt.isoformat()


def _write_history(hist_dir: Path, name: str, entries: list[dict]) -> None:
    path = hist_dir / f"{name}.jsonl"
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path


_BASE = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# check_cadence
# ---------------------------------------------------------------------------

def test_no_history_returns_not_off_cadence(hist_dir: Path) -> None:
    result = check_cadence("pipe_a", 30.0, str(hist_dir))
    assert isinstance(result, CadenceResult)
    assert result.off_cadence is False
    assert result.observed_gap_minutes is None
    assert "insufficient" in result.reason


def test_single_entry_returns_not_off_cadence(hist_dir: Path) -> None:
    _write_history(hist_dir, "pipe_a", [{"checked_at": _ts(_BASE), "healthy": True}])
    result = check_cadence("pipe_a", 30.0, str(hist_dir))
    assert result.off_cadence is False
    assert result.observed_gap_minutes is None


def test_gap_within_tolerance_is_ok(hist_dir: Path) -> None:
    entries = [
        {"checked_at": _ts(_BASE), "healthy": True},
        {"checked_at": _ts(_BASE + timedelta(minutes=28)), "healthy": True},
    ]
    _write_history(hist_dir, "pipe_a", entries)
    result = check_cadence("pipe_a", 30.0, str(hist_dir))
    assert result.off_cadence is False
    assert result.observed_gap_minutes == pytest.approx(28.0, abs=0.1)


def test_gap_exceeds_tolerance_is_off_cadence(hist_dir: Path) -> None:
    # expected=30, tolerance=1.5 → threshold=45 min; gap=60 min → off-cadence
    entries = [
        {"checked_at": _ts(_BASE), "healthy": True},
        {"checked_at": _ts(_BASE + timedelta(minutes=60)), "healthy": True},
    ]
    _write_history(hist_dir, "pipe_a", entries)
    result = check_cadence("pipe_a", 30.0, str(hist_dir))
    assert result.off_cadence is True
    assert result.observed_gap_minutes == pytest.approx(60.0, abs=0.1)
    assert "exceeds" in result.reason


def test_custom_tolerance_factor(hist_dir: Path) -> None:
    # gap=50 min, expected=30, tolerance=2.0 → threshold=60 min → OK
    entries = [
        {"checked_at": _ts(_BASE), "healthy": True},
        {"checked_at": _ts(_BASE + timedelta(minutes=50)), "healthy": True},
    ]
    _write_history(hist_dir, "pipe_a", entries)
    result = check_cadence("pipe_a", 30.0, str(hist_dir), tolerance_factor=2.0)
    assert result.off_cadence is False


def test_uses_two_most_recent_entries(hist_dir: Path) -> None:
    # Three entries; gap between last two is 10 min (within threshold)
    entries = [
        {"checked_at": _ts(_BASE), "healthy": True},
        {"checked_at": _ts(_BASE + timedelta(minutes=120)), "healthy": True},
        {"checked_at": _ts(_BASE + timedelta(minutes=130)), "healthy": True},
    ]
    _write_history(hist_dir, "pipe_a", entries)
    result = check_cadence("pipe_a", 30.0, str(hist_dir))
    assert result.off_cadence is False
    assert result.observed_gap_minutes == pytest.approx(10.0, abs=0.1)


# ---------------------------------------------------------------------------
# check_all_cadences
# ---------------------------------------------------------------------------

def _make_config(pipelines: list) -> MagicMock:
    cfg = MagicMock()
    cfg.pipelines = pipelines
    return cfg


def _make_pipeline(name: str, interval: float | None) -> MagicMock:
    p = MagicMock()
    p.name = name
    p.expected_interval_minutes = interval
    return p


def test_check_all_skips_pipelines_without_interval(hist_dir: Path) -> None:
    cfg = _make_config([
        _make_pipeline("pipe_no_interval", None),
        _make_pipeline("pipe_with_interval", 30.0),
    ])
    results = check_all_cadences(cfg, str(hist_dir))
    assert len(results) == 1
    assert results[0].pipeline == "pipe_with_interval"


def test_check_all_returns_result_per_pipeline(hist_dir: Path) -> None:
    for name in ("p1", "p2"):
        entries = [
            {"checked_at": _ts(_BASE), "healthy": True},
            {"checked_at": _ts(_BASE + timedelta(minutes=20)), "healthy": True},
        ]
        _write_history(hist_dir, name, entries)
    cfg = _make_config([
        _make_pipeline("p1", 30.0),
        _make_pipeline("p2", 30.0),
    ])
    results = check_all_cadences(cfg, str(hist_dir))
    assert len(results) == 2
    assert all(not r.off_cadence for r in results)
