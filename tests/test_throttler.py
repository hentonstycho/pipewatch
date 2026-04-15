"""Tests for pipewatch.throttler."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.checker import CheckResult
from pipewatch.throttler import is_throttled, mark_notified, clear_throttle


@pytest.fixture()
def throttle_dir(tmp_path: Path) -> Path:
    return tmp_path / "throttle"


def _fail(name: str = "pipe_a") -> CheckResult:
    return CheckResult(pipeline=name, healthy=False, violations=["row_count"], error=None)


def _ok(name: str = "pipe_a") -> CheckResult:
    return CheckResult(pipeline=name, healthy=True, violations=[], error=None)


# ---------------------------------------------------------------------------
# is_throttled
# ---------------------------------------------------------------------------

def test_is_throttled_false_when_no_state(throttle_dir):
    assert is_throttled(_fail(), window_minutes=60, data_dir=throttle_dir) is False


def test_is_throttled_false_for_unknown_pipeline(throttle_dir):
    mark_notified(_fail("pipe_a"), data_dir=throttle_dir)
    assert is_throttled(_fail("pipe_b"), window_minutes=60, data_dir=throttle_dir) is False


def test_is_throttled_true_within_window(throttle_dir):
    mark_notified(_fail(), data_dir=throttle_dir)
    assert is_throttled(_fail(), window_minutes=60, data_dir=throttle_dir) is True


def test_is_throttled_false_after_window_expires(throttle_dir):
    past = datetime.now(timezone.utc) - timedelta(minutes=90)
    mark_notified(_fail(), data_dir=throttle_dir)
    # Overwrite with an old timestamp
    import json
    state_file = throttle_dir / "throttle_state.json"
    state_file.write_text(json.dumps({"pipe_a": past.isoformat()}))
    assert is_throttled(_fail(), window_minutes=60, data_dir=throttle_dir) is False


def test_is_throttled_respects_custom_window(throttle_dir):
    mark_notified(_fail(), data_dir=throttle_dir)
    # 0-minute window → always expired
    assert is_throttled(_fail(), window_minutes=0, data_dir=throttle_dir) is False


# ---------------------------------------------------------------------------
# mark_notified
# ---------------------------------------------------------------------------

def test_mark_notified_creates_state_file(throttle_dir):
    mark_notified(_fail(), data_dir=throttle_dir)
    assert (throttle_dir / "throttle_state.json").exists()


def test_mark_notified_records_pipeline(throttle_dir):
    mark_notified(_fail("my_pipe"), data_dir=throttle_dir)
    import json
    state = json.loads((throttle_dir / "throttle_state.json").read_text())
    assert "my_pipe" in state


def test_mark_notified_updates_existing_entry(throttle_dir):
    import json, time
    mark_notified(_fail(), data_dir=throttle_dir)
    first = json.loads((throttle_dir / "throttle_state.json").read_text())["pipe_a"]
    time.sleep(0.01)
    mark_notified(_fail(), data_dir=throttle_dir)
    second = json.loads((throttle_dir / "throttle_state.json").read_text())["pipe_a"]
    assert second >= first


# ---------------------------------------------------------------------------
# clear_throttle
# ---------------------------------------------------------------------------

def test_clear_throttle_returns_false_when_missing(throttle_dir):
    assert clear_throttle("no_such_pipe", data_dir=throttle_dir) is False


def test_clear_throttle_removes_entry(throttle_dir):
    mark_notified(_fail(), data_dir=throttle_dir)
    assert clear_throttle("pipe_a", data_dir=throttle_dir) is True
    assert is_throttled(_fail(), window_minutes=60, data_dir=throttle_dir) is False


def test_clear_throttle_leaves_other_pipelines(throttle_dir):
    mark_notified(_fail("pipe_a"), data_dir=throttle_dir)
    mark_notified(_fail("pipe_b"), data_dir=throttle_dir)
    clear_throttle("pipe_a", data_dir=throttle_dir)
    assert is_throttled(_fail("pipe_b"), window_minutes=60, data_dir=throttle_dir) is True
