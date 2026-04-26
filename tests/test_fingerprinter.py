"""Tests for pipewatch.fingerprinter."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.config import PipelineConfig, ThresholdConfig
from pipewatch.checker import CheckResult
from pipewatch.fingerprinter import (
    Fingerprint,
    _violation_keys,
    _hash_violations,
    fingerprint_pipeline,
    fingerprint_all,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_config(name: str = "pipe_a") -> PipelineConfig:
    return PipelineConfig(
        name=name,
        source="db",
        thresholds=ThresholdConfig(min_row_count=10, max_error_rate=0.05, max_latency_seconds=30),
        schedule=None,
    )


def _write_history(hist_dir: Path, name: str, entries: list[dict]) -> None:
    path = hist_dir / f"{name}.jsonl"
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _healthy_entry() -> dict:
    return {"pipeline": "pipe_a", "healthy": True, "row_count": 100,
            "error_rate": 0.0, "latency_seconds": 5.0, "checked_at": "2024-01-01T00:00:00"}


def _failing_entry(error_rate: float = 0.1) -> dict:
    return {"pipeline": "pipe_a", "healthy": False, "row_count": 100,
            "error_rate": error_rate, "latency_seconds": 5.0, "checked_at": "2024-01-01T01:00:00"}


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path


# ---------------------------------------------------------------------------
# unit tests for helpers
# ---------------------------------------------------------------------------

def test_violation_keys_healthy_result_returns_empty():
    result = CheckResult(pipeline="p", healthy=True, row_count=50,
                         error_rate=0.0, latency_seconds=1.0)
    assert _violation_keys(result) == []


def test_violation_keys_failing_with_error_rate():
    result = CheckResult(pipeline="p", healthy=False, row_count=50,
                         error_rate=0.2, latency_seconds=1.0)
    assert "error_rate" in _violation_keys(result)


def test_violation_keys_falling_with_zero_row_count():
    result = CheckResult(pipeline="p", healthy=False, row_count=0,
                         error_rate=0.0, latency_seconds=1.0)
    assert "row_count" in _violation_keys(result)


def test_violation_keys_multiple_violations_returns_all():
    """A result that violates both error_rate and latency should list both keys."""
    result = CheckResult(pipeline="p", healthy=False, row_count=50,
                         error_rate=0.9, latency_seconds=999.0)
    keys = _violation_keys(result)
    assert "error_rate" in keys
    assert "latency_seconds" in keys


def test_hash_violations_is_8_chars():
    h = _hash_violations("pipe", ["error_rate"])
    assert len(h) == 8


def test_hash_violations_same_input_same_output():
    assert _hash_violations("pipe", ["error_rate"]) == _hash_violations("pipe", ["error_rate"])


def test_hash_violations_different_pipeline_different_hash():
    assert _hash_violations("pipe_a", ["error_rate"]) != _hash_violations("pipe_b", ["error_rate"])


def test_hash_violations_different_keys_different_hash():
    """Different violation keys for the same pipeline must produce different hashes."""
    assert _hash_violations("pipe", ["error_rate"]) != _hash_violations("pipe", ["latency_seconds"])


# ---------------------------------------------------------------------------
# fingerprint_pipeline
# ---------------------------------------------------------------------------
