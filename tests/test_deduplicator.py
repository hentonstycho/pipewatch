"""Tests for pipewatch.deduplicator."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult
from pipewatch.deduplicator import (
    _dedup_path,
    clear_pipeline,
    is_duplicate,
    mark_notified,
    should_notify,
)


@pytest.fixture()
def dedup_dir(tmp_path: Path) -> Path:
    return tmp_path / "dedup"


def _fail(name: str = "orders", violation: str = "row_count below threshold") -> CheckResult:
    return CheckResult(pipeline_name=name, healthy=False, violation=violation)


def _ok(name: str = "orders") -> CheckResult:
    return CheckResult(pipeline_name=name, healthy=True, violation=None)


# ---------------------------------------------------------------------------
# is_duplicate / mark_notified
# ---------------------------------------------------------------------------

def test_is_duplicate_returns_false_for_unknown(dedup_dir):
    assert is_duplicate(_fail(), base_dir=dedup_dir) is False


def test_mark_notified_creates_file(dedup_dir):
    mark_notified(_fail(), base_dir=dedup_dir)
    assert _dedup_path(dedup_dir).exists()


def test_is_duplicate_returns_true_after_mark(dedup_dir):
    result = _fail()
    mark_notified(result, base_dir=dedup_dir)
    assert is_duplicate(result, base_dir=dedup_dir) is True


def test_different_violation_not_duplicate(dedup_dir):
    mark_notified(_fail(violation="row_count below threshold"), base_dir=dedup_dir)
    assert is_duplicate(_fail(violation="error_rate above threshold"), base_dir=dedup_dir) is False


def test_different_pipeline_not_duplicate(dedup_dir):
    mark_notified(_fail(name="orders"), base_dir=dedup_dir)
    assert is_duplicate(_fail(name="inventory"), base_dir=dedup_dir) is False


# ---------------------------------------------------------------------------
# clear_pipeline
# ---------------------------------------------------------------------------

def test_clear_pipeline_removes_entries(dedup_dir):
    mark_notified(_fail(name="orders", violation="row_count below threshold"), base_dir=dedup_dir)
    mark_notified(_fail(name="orders", violation="error_rate above threshold"), base_dir=dedup_dir)
    mark_notified(_fail(name="inventory"), base_dir=dedup_dir)
    removed = clear_pipeline("orders", base_dir=dedup_dir)
    assert removed == 2
    assert not is_duplicate(_fail(name="orders"), base_dir=dedup_dir)
    assert is_duplicate(_fail(name="inventory"), base_dir=dedup_dir)


def test_clear_pipeline_missing_returns_zero(dedup_dir):
    assert clear_pipeline("nonexistent", base_dir=dedup_dir) == 0


# ---------------------------------------------------------------------------
# should_notify
# ---------------------------------------------------------------------------

def test_should_notify_true_on_first_failure(dedup_dir):
    assert should_notify(_fail(), base_dir=dedup_dir) is True


def test_should_notify_false_on_second_identical_failure(dedup_dir):
    should_notify(_fail(), base_dir=dedup_dir)  # first call marks it
    assert should_notify(_fail(), base_dir=dedup_dir) is False


def test_should_notify_false_for_healthy_result(dedup_dir):
    assert should_notify(_ok(), base_dir=dedup_dir) is False


def test_should_notify_true_after_recovery(dedup_dir):
    should_notify(_fail(), base_dir=dedup_dir)   # notified
    should_notify(_ok(), base_dir=dedup_dir)     # recovered → clears state
    assert should_notify(_fail(), base_dir=dedup_dir) is True  # new failure


def test_state_file_is_valid_json(dedup_dir):
    should_notify(_fail(), base_dir=dedup_dir)
    data = json.loads(_dedup_path(dedup_dir).read_text())
    assert isinstance(data, dict)
    key = next(iter(data))
    assert "notified_at" in data[key]
