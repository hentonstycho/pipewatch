"""Tests for pipewatch.snapshotter."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult
from pipewatch.snapshotter import (
    diff_snapshots,
    list_snapshots,
    load_snapshot,
    save_snapshot,
)


def _result(name: str, healthy: bool, violations: list[str] | None = None) -> CheckResult:
    return CheckResult(
        pipeline=name,
        healthy=healthy,
        violations=violations or [],
        checked_at="2024-01-01T00:00:00+00:00",
    )


@pytest.fixture()
def snap_dir(tmp_path: Path) -> Path:
    return tmp_path / "snapshots"


def test_save_snapshot_creates_file(snap_dir: Path) -> None:
    results = [_result("pipe_a", True)]
    path = save_snapshot(results, snapshot_dir=snap_dir)
    assert path.exists()
    assert path.suffix == ".json"


def test_save_snapshot_with_label(snap_dir: Path) -> None:
    results = [_result("pipe_a", True)]
    path = save_snapshot(results, label="nightly", snapshot_dir=snap_dir)
    assert "nightly" in path.name


def test_load_snapshot_round_trips(snap_dir: Path) -> None:
    results = [_result("pipe_a", False, ["row_count"])]
    path = save_snapshot(results, snapshot_dir=snap_dir)
    data = load_snapshot(path)
    assert data["results"][0]["pipeline"] == "pipe_a"
    assert data["results"][0]["healthy"] is False
    assert "row_count" in data["results"][0]["violations"]


def test_load_snapshot_contains_results_key(snap_dir: Path) -> None:
    """Ensure the top-level 'results' key is always present in saved snapshots."""
    results = [_result("pipe_b", True)]
    path = save_snapshot(results, snapshot_dir=snap_dir)
    data = load_snapshot(path)
    assert "results" in data


def test_list_snapshots_returns_sorted(snap_dir: Path) -> None:
    save_snapshot([_result("a", True)], label="first", snapshot_dir=snap_dir)
    save_snapshot([_result("b", True)], label="second", snapshot_dir=snap_dir)
    snaps = list_snapshots(snap_dir)
    assert len(snaps) == 2
    assert snaps[0].stat().st_mtime <= snaps[1].stat().st_mtime


def test_list_snapshots_empty_dir(snap_dir: Path) -> None:
    assert list_snapshots(snap_dir) == []


def test_diff_snapshots_no_changes() -> None:
    snap = {"results": [{"pipeline": "p", "healthy": True, "violations": []}]}
    lines = diff_snapshots(snap, snap)
    assert lines == ["No changes detected between snapshots."]


def test_diff_snapshots_detects_degraded() -> None:
    old = {"results": [{"pipeline": "p", "healthy": True, "violations": []}]}
    new = {"results": [{"pipeline": "p", "healthy": False, "violations": ["latency"]}]}
    lines = diff_snapshots(old, new)
    assert any("DEGRADED" in l for l in lines)


def test_diff_snapshots_detects_recovered() -> None:
    old = {"results": [{"pipeline": "p", "healthy": False, "violations": ["latency"]}]}
    new = {"results": [{"pipeline": "p", "healthy": True, "violations": []}]}
    lines = diff_snapshots(old, new)
    assert any("RECOVERED" in l for l in lines)


def test_diff_snapshots_detects_new_pipeline() -> None:
    old: dict = {"results": []}
    new = {"results": [{"pipeline": "fresh", "healthy": True, "violations": []}]}
    lines = diff_snapshots(old, new)
    assert any("NEW" in l and "fresh" in l for l in lines)


def test_diff_snapshots_detects_removed_pipeline() -> None:
    """A pipeline present in the old snapshot but absent in the new one should be flagged."""
    old = {"results": [{"pipeline": "gone", "healthy": True, "violations": []}]}
    new: dict = {"results": []}
    lines = diff_snapshots(old, new)
    assert any("REMOVED" in l and "gone" in l for l in lines)
