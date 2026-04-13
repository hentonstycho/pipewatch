"""Tests for pipewatch.history persistence helpers."""

import json
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult
from pipewatch.history import (
    consecutive_failures,
    load_history,
    record_result,
)


@pytest.fixture()
def tmp_history(tmp_path: Path) -> Path:
    return tmp_path / "history"


@pytest.fixture()
def ok_result() -> CheckResult:
    return CheckResult(pipeline_name="orders", healthy=True, violations=[])


@pytest.fixture()
def fail_result() -> CheckResult:
    return CheckResult(
        pipeline_name="orders",
        healthy=False,
        violations=["row_count below threshold"],
    )


def test_record_result_creates_file(tmp_history, ok_result):
    record_result(ok_result, history_dir=tmp_history)
    history_file = tmp_history / "orders.jsonl"
    assert history_file.exists()


def test_record_result_appends_json_line(tmp_history, ok_result, fail_result):
    record_result(ok_result, history_dir=tmp_history)
    record_result(fail_result, history_dir=tmp_history)

    lines = (tmp_history / "orders.jsonl").read_text().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["healthy"] is True
    assert first["pipeline_name"] == "orders"


def test_load_history_returns_empty_for_unknown_pipeline(tmp_history):
    result = load_history("nonexistent", history_dir=tmp_history)
    assert result == []


def test_load_history_respects_limit(tmp_history, ok_result):
    for _ in range(10):
        record_result(ok_result, history_dir=tmp_history)

    records = load_history("orders", limit=3, history_dir=tmp_history)
    assert len(records) == 3


def test_load_history_returns_all_when_under_limit(tmp_history, ok_result):
    for _ in range(4):
        record_result(ok_result, history_dir=tmp_history)

    records = load_history("orders", limit=50, history_dir=tmp_history)
    assert len(records) == 4


def test_consecutive_failures_zero_when_last_ok(tmp_history, ok_result, fail_result):
    record_result(fail_result, history_dir=tmp_history)
    record_result(fail_result, history_dir=tmp_history)
    record_result(ok_result, history_dir=tmp_history)

    assert consecutive_failures("orders", history_dir=tmp_history) == 0


def test_consecutive_failures_counts_tail(tmp_history, ok_result, fail_result):
    record_result(ok_result, history_dir=tmp_history)
    record_result(fail_result, history_dir=tmp_history)
    record_result(fail_result, history_dir=tmp_history)
    record_result(fail_result, history_dir=tmp_history)

    assert consecutive_failures("orders", history_dir=tmp_history) == 3


def test_consecutive_failures_zero_for_unknown_pipeline(tmp_history):
    assert consecutive_failures("ghost_pipeline", history_dir=tmp_history) == 0
