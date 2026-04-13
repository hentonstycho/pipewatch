"""Tests for pipewatch.pruner."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.pruner import prune_all, prune_pipeline


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _ts(offset_days: int = 0) -> str:
    """ISO-8601 UTC timestamp offset by *offset_days* from now."""
    dt = datetime.now(timezone.utc) - timedelta(days=offset_days)
    return dt.isoformat()


def _write_history(directory: Path, pipeline: str, entries: list[dict]) -> Path:
    path = directory / f"{pipeline}.jsonl"
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))
    return path


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path


# ---------------------------------------------------------------------------
# prune_pipeline – missing file
# ---------------------------------------------------------------------------

def test_prune_pipeline_missing_file_returns_zero(hist_dir: Path) -> None:
    removed = prune_pipeline("ghost", history_dir=hist_dir, max_entries=5)
    assert removed == 0


# ---------------------------------------------------------------------------
# max_entries
# ---------------------------------------------------------------------------

def test_prune_pipeline_max_entries_trims_oldest(hist_dir: Path) -> None:
    entries = [{"checked_at": _ts(i), "healthy": True} for i in range(10, 0, -1)]
    _write_history(hist_dir, "pipe", entries)

    removed = prune_pipeline("pipe", history_dir=hist_dir, max_entries=4)

    assert removed == 6
    path = hist_dir / "pipe.jsonl"
    kept = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    assert len(kept) == 4


def test_prune_pipeline_max_entries_no_trim_when_within_limit(hist_dir: Path) -> None:
    entries = [{"checked_at": _ts(i), "healthy": True} for i in range(3, 0, -1)]
    _write_history(hist_dir, "pipe", entries)

    removed = prune_pipeline("pipe", history_dir=hist_dir, max_entries=10)
    assert removed == 0


# ---------------------------------------------------------------------------
# max_age_days
# ---------------------------------------------------------------------------

def test_prune_pipeline_max_age_removes_stale_entries(hist_dir: Path) -> None:
    entries = [
        {"checked_at": _ts(1), "healthy": True},   # recent – keep
        {"checked_at": _ts(5), "healthy": False},  # old – drop
        {"checked_at": _ts(10), "healthy": False}, # old – drop
    ]
    _write_history(hist_dir, "pipe", entries)

    removed = prune_pipeline("pipe", history_dir=hist_dir, max_age_days=3)

    assert removed == 2
    path = hist_dir / "pipe.jsonl"
    kept = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    assert len(kept) == 1
    assert kept[0]["healthy"] is True


def test_prune_pipeline_no_age_limit_keeps_all(hist_dir: Path) -> None:
    entries = [{"checked_at": _ts(100), "healthy": True}]
    _write_history(hist_dir, "pipe", entries)

    removed = prune_pipeline("pipe", history_dir=hist_dir)
    assert removed == 0


# ---------------------------------------------------------------------------
# prune_all
# ---------------------------------------------------------------------------

def test_prune_all_returns_per_pipeline_counts(hist_dir: Path) -> None:
    for name, count in [("a", 8), ("b", 3)]:
        entries = [{"checked_at": _ts(i), "healthy": True} for i in range(count, 0, -1)]
        _write_history(hist_dir, name, entries)

    result = prune_all(["a", "b"], history_dir=hist_dir, max_entries=2)

    assert result["a"] == 6
    assert result["b"] == 1


def test_prune_all_missing_pipeline_returns_zero(hist_dir: Path) -> None:
    result = prune_all(["nonexistent"], history_dir=hist_dir, max_entries=5)
    assert result["nonexistent"] == 0
