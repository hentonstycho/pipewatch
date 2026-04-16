"""Tests for pipewatch.ratelimiter."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipewatch.ratelimiter import (
    clear_state,
    is_rate_limited,
    record_alert,
)


@pytest.fixture()
def rl_dir(tmp_path: Path) -> Path:
    return tmp_path / "ratelimits"


def test_not_limited_when_no_history(rl_dir):
    assert is_rate_limited("pipe_a", max_alerts=3, window_seconds=60, base_dir=rl_dir) is False


def test_not_limited_below_threshold(rl_dir):
    record_alert("pipe_a", base_dir=rl_dir)
    record_alert("pipe_a", base_dir=rl_dir)
    assert is_rate_limited("pipe_a", max_alerts=3, window_seconds=60, base_dir=rl_dir) is False


def test_limited_at_threshold(rl_dir):
    for _ in range(3):
        record_alert("pipe_a", base_dir=rl_dir)
    assert is_rate_limited("pipe_a", max_alerts=3, window_seconds=60, base_dir=rl_dir) is True


def test_limited_above_threshold(rl_dir):
    for _ in range(5):
        record_alert("pipe_b", base_dir=rl_dir)
    assert is_rate_limited("pipe_b", max_alerts=3, window_seconds=60, base_dir=rl_dir) is True


def test_old_timestamps_outside_window_ignored(rl_dir, monkeypatch):
    import pipewatch.ratelimiter as rl_mod

    # Record two alerts "in the past"
    fake_past = time.time() - 120
    monkeypatch.setattr(rl_mod, "_now", lambda: fake_past)
    record_alert("pipe_c", base_dir=rl_dir)
    record_alert("pipe_c", base_dir=rl_dir)

    # Restore real time and check — those old alerts should not count
    monkeypatch.setattr(rl_mod, "_now", time.time)
    assert is_rate_limited("pipe_c", max_alerts=2, window_seconds=60, base_dir=rl_dir) is False


def test_clear_state_removes_file(rl_dir):
    record_alert("pipe_d", base_dir=rl_dir)
    assert is_rate_limited("pipe_d", max_alerts=1, window_seconds=60, base_dir=rl_dir) is True
    clear_state("pipe_d", base_dir=rl_dir)
    assert is_rate_limited("pipe_d", max_alerts=1, window_seconds=60, base_dir=rl_dir) is False


def test_clear_state_noop_when_missing(rl_dir):
    # Should not raise
    clear_state("nonexistent", base_dir=rl_dir)


def test_independent_pipelines(rl_dir):
    for _ in range(3):
        record_alert("pipe_x", base_dir=rl_dir)
    assert is_rate_limited("pipe_x", max_alerts=3, window_seconds=60, base_dir=rl_dir) is True
    assert is_rate_limited("pipe_y", max_alerts=3, window_seconds=60, base_dir=rl_dir) is False
