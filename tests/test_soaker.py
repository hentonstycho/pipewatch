"""Tests for pipewatch.soaker."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.soaker import SoakResult, evaluate_soak


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    d = tmp_path / "history"
    d.mkdir()
    return d


@pytest.fixture()
def soak_dir(tmp_path: Path) -> Path:
    d = tmp_path / "soak"
    d.mkdir()
    return d


def _write_history(hist_dir: Path, pipeline: str, entries: list[dict]) -> None:
    p = hist_dir / f"{pipeline}.jsonl"
    p.write_text("\n".join(json.dumps(e) for e in entries))


# ---------------------------------------------------------------------------
# No history
# ---------------------------------------------------------------------------

def test_no_history_is_not_soaking(hist_dir, soak_dir):
    result = evaluate_soak("pipe_a", required=3, history_dir=hist_dir, soak_dir=soak_dir)
    assert isinstance(result, SoakResult)
    assert result.soaking is False
    assert result.healthy_streak == 0


# ---------------------------------------------------------------------------
# All healthy – never soaking
# ---------------------------------------------------------------------------

def test_all_healthy_not_soaking(hist_dir, soak_dir):
    _write_history(hist_dir, "pipe_a", [{"healthy": True}] * 5)
    result = evaluate_soak("pipe_a", required=3, history_dir=hist_dir, soak_dir=soak_dir)
    assert result.soaking is False
    assert result.healthy_streak == 5


# ---------------------------------------------------------------------------
# Transition: was failing, now partially recovered
# ---------------------------------------------------------------------------

def test_soaking_after_partial_recovery(hist_dir, soak_dir):
    # Two failures then two healthy runs — not enough to exit soak (required=3)
    _write_history(
        hist_dir, "pipe_a",
        [{"healthy": False}, {"healthy": False}, {"healthy": True}, {"healthy": True}],
    )
    # Prime the soak state to know it was previously failing
    soak_file = soak_dir / "pipe_a.json"
    soak_file.write_text(json.dumps({"previously_failing": True, "started_at": None}))

    result = evaluate_soak("pipe_a", required=3, history_dir=hist_dir, soak_dir=soak_dir)
    assert result.soaking is True
    assert result.healthy_streak == 2
    assert result.required == 3
    assert result.started_at is not None


def test_soak_exits_when_streak_meets_required(hist_dir, soak_dir):
    _write_history(
        hist_dir, "pipe_a",
        [{"healthy": False}, {"healthy": True}, {"healthy": True}, {"healthy": True}],
    )
    soak_file = soak_dir / "pipe_a.json"
    soak_file.write_text(json.dumps({"previously_failing": True, "started_at": "2024-01-01T00:00:00+00:00"}))

    result = evaluate_soak("pipe_a", required=3, history_dir=hist_dir, soak_dir=soak_dir)
    assert result.soaking is False
    assert result.healthy_streak == 3


# ---------------------------------------------------------------------------
# Failing pipeline resets soak
# ---------------------------------------------------------------------------

def test_currently_failing_not_soaking(hist_dir, soak_dir):
    _write_history(
        hist_dir, "pipe_a",
        [{"healthy": True}, {"healthy": True}, {"healthy": False}],
    )
    result = evaluate_soak("pipe_a", required=3, history_dir=hist_dir, soak_dir=soak_dir)
    assert result.soaking is False
    assert result.healthy_streak == 0


# ---------------------------------------------------------------------------
# __str__ representations
# ---------------------------------------------------------------------------

def test_str_soaking():
    r = SoakResult("my_pipe", True, 1, 3, "2024-01-01T00:00:00+00:00")
    assert "SOAKING" in str(r)
    assert "1/3" in str(r)


def test_str_stable():
    r = SoakResult("my_pipe", False, 5, 3, None)
    assert "STABLE" in str(r)
