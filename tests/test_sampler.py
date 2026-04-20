"""Tests for pipewatch.sampler."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult
from pipewatch.sampler import (
    _DEFAULT_RESERVOIR,
    clear_sample,
    load_sample,
    reservoir_sample,
)


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path / "history"


@pytest.fixture()
def _result() -> CheckResult:
    return CheckResult(
        pipeline="orders",
        healthy=True,
        timestamp="2024-01-01T00:00:00Z",
        violations=[],
    )


@pytest.fixture()
def _fail_result() -> CheckResult:
    return CheckResult(
        pipeline="orders",
        healthy=False,
        timestamp="2024-01-02T00:00:00Z",
        violations=["row_count below threshold"],
    )


def test_reservoir_sample_creates_file(hist_dir, _result):
    reservoir_sample(hist_dir, "orders", _result)
    sample_file = hist_dir / "orders.sample.jsonl"
    assert sample_file.exists()


def test_reservoir_sample_returns_size(hist_dir, _result):
    size = reservoir_sample(hist_dir, "orders", _result)
    assert size == 1


def test_reservoir_sample_appends_up_to_limit(hist_dir, _result):
    for _ in range(5):
        size = reservoir_sample(hist_dir, "orders", _result, reservoir_size=10)
    assert size == 5


def test_reservoir_sample_does_not_exceed_reservoir_size(hist_dir, _result):
    for _ in range(20):
        size = reservoir_sample(hist_dir, "orders", _result, reservoir_size=10)
    assert size == 10


def test_load_sample_returns_empty_for_unknown(hist_dir):
    entries = load_sample(hist_dir, "nonexistent")
    assert entries == []


def test_load_sample_returns_entries(hist_dir, _result):
    reservoir_sample(hist_dir, "orders", _result)
    entries = load_sample(hist_dir, "orders")
    assert len(entries) == 1
    assert entries[0]["pipeline"] == "orders"
    assert entries[0]["healthy"] is True


def test_load_sample_stores_violations(hist_dir, _fail_result):
    reservoir_sample(hist_dir, "orders", _fail_result)
    entries = load_sample(hist_dir, "orders")
    assert entries[0]["violations"] == ["row_count below threshold"]


def test_clear_sample_removes_file(hist_dir, _result):
    reservoir_sample(hist_dir, "orders", _result)
    clear_sample(hist_dir, "orders")
    assert not (hist_dir / "orders.sample.jsonl").exists()


def test_clear_sample_noop_when_missing(hist_dir):
    # Should not raise even if file does not exist.
    clear_sample(hist_dir, "ghost")


def test_sample_file_is_valid_jsonl(hist_dir, _result, _fail_result):
    reservoir_sample(hist_dir, "orders", _result)
    reservoir_sample(hist_dir, "orders", _fail_result)
    raw = (hist_dir / "orders.sample.jsonl").read_text().strip().splitlines()
    for line in raw:
        obj = json.loads(line)  # must not raise
        assert "pipeline" in obj
