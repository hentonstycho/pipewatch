"""Tests for pipewatch.scheduler."""

from __future__ import annotations

import pytest

from pipewatch.scheduler import parse_interval, run_scheduler


# ---------------------------------------------------------------------------
# parse_interval
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("30s", 30),
    ("5m", 300),
    ("2h", 7200),
    ("120", 120),
    ("1s", 1),
    ("60m", 3600),
])
def test_parse_interval_valid(value, expected):
    assert parse_interval(value) == expected


def test_parse_interval_strips_whitespace():
    assert parse_interval("  10m  ") == 600


def test_parse_interval_invalid_raises():
    with pytest.raises(ValueError):
        parse_interval("abc")


# ---------------------------------------------------------------------------
# run_scheduler
# ---------------------------------------------------------------------------

def test_run_scheduler_calls_task_expected_times():
    calls = []

    def task():
        calls.append(1)

    run_scheduler("0s", task, max_iterations=3)
    assert len(calls) == 3


def test_run_scheduler_stops_at_max_iterations():
    calls = []

    def task():
        calls.append(1)

    run_scheduler("0s", task, max_iterations=5)
    assert len(calls) == 5


def test_run_scheduler_continues_on_error_by_default():
    calls = []

    def task():
        calls.append(1)
        raise RuntimeError("boom")

    # Should not raise; should still run all iterations.
    run_scheduler("0s", task, max_iterations=3)
    assert len(calls) == 3


def test_run_scheduler_stop_on_error_raises():
    def task():
        raise ValueError("fatal")

    with pytest.raises(ValueError, match="fatal"):
        run_scheduler("0s", task, max_iterations=10, stop_on_error=True)


def test_run_scheduler_single_iteration():
    called = []

    def task():
        called.append(True)

    run_scheduler("0s", task, max_iterations=1)
    assert called == [True]
