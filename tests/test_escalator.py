"""Tests for pipewatch.escalator."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult
from pipewatch.escalator import EscalationResult, escalate, escalate_all, _level


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_history(hist_dir: Path, pipeline: str, entries: list[dict]) -> None:
    p = hist_dir / f"{pipeline}.jsonl"
    with p.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _entry(healthy: bool) -> dict:
    return {
        "pipeline": "p",
        "healthy": healthy,
        "violations": [] if healthy else ["row_count"],
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path


# ---------------------------------------------------------------------------
# _level
# ---------------------------------------------------------------------------

def test_level_ok():
    assert _level(0, 2, 5) == "ok"
    assert _level(1, 2, 5) == "ok"


def test_level_warn():
    assert _level(2, 2, 5) == "warn"
    assert _level(4, 2, 5) == "warn"


def test_level_critical():
    assert _level(5, 2, 5) == "critical"
    assert _level(10, 2, 5) == "critical"


# ---------------------------------------------------------------------------
# escalate
# ---------------------------------------------------------------------------

def test_healthy_result_returns_ok(hist_dir):
    result = CheckResult(pipeline="pipe", healthy=True, violations=[], checked_at="now")
    esc = escalate(result, str(hist_dir))
    assert esc.level == "ok"
    assert esc.consecutive == 0


def test_single_failure_is_ok_level(hist_dir):
    _write_history(hist_dir, "pipe", [_entry(False)])
    result = CheckResult(pipeline="pipe", healthy=False, violations=["row_count"], checked_at="now")
    esc = escalate(result, str(hist_dir), warn_after=2, critical_after=5)
    assert esc.level == "ok"
    assert esc.consecutive == 1


def test_two_failures_is_warn_level(hist_dir):
    _write_history(hist_dir, "pipe", [_entry(False), _entry(False)])
    result = CheckResult(pipeline="pipe", healthy=False, violations=["row_count"], checked_at="now")
    esc = escalate(result, str(hist_dir), warn_after=2, critical_after=5)
    assert esc.level == "warn"


def test_five_failures_is_critical(hist_dir):
    _write_history(hist_dir, "pipe", [_entry(False)] * 5)
    result = CheckResult(pipeline="pipe", healthy=False, violations=["error_rate"], checked_at="now")
    esc = escalate(result, str(hist_dir), warn_after=2, critical_after=5)
    assert esc.level == "critical"
    assert "error_rate" in esc.message


def test_escalate_all_returns_one_per_result(hist_dir):
    results = [
        CheckResult(pipeline="a", healthy=True, violations=[], checked_at="now"),
        CheckResult(pipeline="b", healthy=False, violations=["latency"], checked_at="now"),
    ]
    out = escalate_all(results, str(hist_dir))
    assert len(out) == 2
    assert out[0].pipeline == "a"
    assert out[1].pipeline == "b"


def test_str_representation(hist_dir):
    result = CheckResult(pipeline="mypipe", healthy=False, violations=["row_count"], checked_at="now")
    esc = escalate(result, str(hist_dir))
    s = str(esc)
    assert "mypipe" in s
    assert esc.level.upper() in s
