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


def test_hash_violations_is_8_chars():
    h = _hash_violations("pipe", ["error_rate"])
    assert len(h) == 8


def test_hash_violations_same_input_same_output():
    assert _hash_violations("pipe", ["error_rate"]) == _hash_violations("pipe", ["error_rate"])


def test_hash_violations_different_pipeline_different_hash():
    assert _hash_violations("pipe_a", ["error_rate"]) != _hash_violations("pipe_b", ["error_rate"])


# ---------------------------------------------------------------------------
# fingerprint_pipeline
# ---------------------------------------------------------------------------

def test_fingerprint_pipeline_no_history_returns_none(hist_dir: Path):
    cfg = _make_config()
    assert fingerprint_pipeline(cfg, history_dir=hist_dir) is None


def test_fingerprint_pipeline_all_healthy_no_violations(hist_dir: Path):
    cfg = _make_config()
    _write_history(hist_dir, "pipe_a", [_healthy_entry()] * 5)
    fp = fingerprint_pipeline(cfg, history_dir=hist_dir)
    assert fp is not None
    assert fp.violations == []


def test_fingerprint_pipeline_majority_failing_reports_violation(hist_dir: Path):
    cfg = _make_config()
    entries = [_failing_entry()] * 4 + [_healthy_entry()]
    _write_history(hist_dir, "pipe_a", entries)
    fp = fingerprint_pipeline(cfg, history_dir=hist_dir)
    assert fp is not None
    assert "error_rate" in fp.violations


def test_fingerprint_pipeline_returns_fingerprint_dataclass(hist_dir: Path):
    cfg = _make_config()
    _write_history(hist_dir, "pipe_a", [_healthy_entry()] * 3)
    fp = fingerprint_pipeline(cfg, history_dir=hist_dir)
    assert isinstance(fp, Fingerprint)
    assert fp.pipeline == "pipe_a"


def test_fingerprint_pipeline_str_contains_pipeline_name(hist_dir: Path):
    cfg = _make_config()
    _write_history(hist_dir, "pipe_a", [_healthy_entry()] * 3)
    fp = fingerprint_pipeline(cfg, history_dir=hist_dir)
    assert "pipe_a" in str(fp)


# ---------------------------------------------------------------------------
# fingerprint_all
# ---------------------------------------------------------------------------

def test_fingerprint_all_skips_missing_history(hist_dir: Path):
    cfgs = [_make_config("pipe_a"), _make_config("pipe_b")]
    _write_history(hist_dir, "pipe_a", [_healthy_entry()] * 3)
    # pipe_b has no history
    results = fingerprint_all(cfgs, history_dir=hist_dir)
    assert len(results) == 1
    assert results[0].pipeline == "pipe_a"


def test_fingerprint_all_returns_all_when_history_present(hist_dir: Path):
    cfgs = [_make_config("pipe_a"), _make_config("pipe_b")]
    for name in ("pipe_a", "pipe_b"):
        entry = _healthy_entry()
        entry["pipeline"] = name
        _write_history(hist_dir, name, [entry] * 3)
    results = fingerprint_all(cfgs, history_dir=hist_dir)
    assert len(results) == 2
