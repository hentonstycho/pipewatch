"""Tests for pipewatch.debouncer."""
from __future__ import annotations

import pytest
from pathlib import Path

from pipewatch.checker import CheckResult
from pipewatch.debouncer import evaluate, get_state


@pytest.fixture()
def deb_dir(tmp_path: Path) -> Path:
    return tmp_path / "debounce"


def _fail(name: str = "pipe") -> CheckResult:
    return CheckResult(pipeline_name=name, healthy=False, violations=["row_count"], checked_at="2024-01-01T00:00:00Z")


def _ok(name: str = "pipe") -> CheckResult:
    return CheckResult(pipeline_name=name, healthy=True, violations=[], checked_at="2024-01-01T00:00:00Z")


def test_no_notification_below_threshold(deb_dir):
    assert evaluate(_fail(), threshold=3, base_dir=deb_dir) is False
    assert evaluate(_fail(), threshold=3, base_dir=deb_dir) is False


def test_notification_at_threshold(deb_dir):
    evaluate(_fail(), threshold=2, base_dir=deb_dir)
    result = evaluate(_fail(), threshold=2, base_dir=deb_dir)
    assert result is True


def test_notification_fires_only_once_per_run(deb_dir):
    for _ in range(4):
        evaluate(_fail(), threshold=2, base_dir=deb_dir)
    # By now threshold was crossed; collect all fire events
    fires = [evaluate(_fail(), threshold=2, base_dir=deb_dir) for _ in range(3)]
    assert all(f is False for f in fires)


def test_recovery_resets_state(deb_dir):
    evaluate(_fail(), threshold=1, base_dir=deb_dir)
    evaluate(_fail(), threshold=1, base_dir=deb_dir)  # notified=True
    evaluate(_ok(), threshold=1, base_dir=deb_dir)   # reset
    # Next failure should fire again
    assert evaluate(_fail(), threshold=1, base_dir=deb_dir) is True


def test_healthy_result_never_notifies(deb_dir):
    for _ in range(5):
        assert evaluate(_ok(), threshold=1, base_dir=deb_dir) is False


def test_get_state_unknown_returns_none(deb_dir):
    assert get_state("missing", deb_dir) is None


def test_get_state_reflects_failures(deb_dir):
    evaluate(_fail("p1"), threshold=5, base_dir=deb_dir)
    evaluate(_fail("p1"), threshold=5, base_dir=deb_dir)
    s = get_state("p1", deb_dir)
    assert s is not None
    assert s.consecutive_failures == 2
    assert s.notified is False


def test_get_state_notified_flag(deb_dir):
    evaluate(_fail("p2"), threshold=1, base_dir=deb_dir)
    s = get_state("p2", deb_dir)
    assert s.notified is True


def test_multiple_pipelines_independent(deb_dir):
    evaluate(_fail("a"), threshold=2, base_dir=deb_dir)
    evaluate(_fail("b"), threshold=2, base_dir=deb_dir)
    evaluate(_fail("b"), threshold=2, base_dir=deb_dir)
    assert get_state("a", deb_dir).consecutive_failures == 1
    assert get_state("b", deb_dir).consecutive_failures == 2
