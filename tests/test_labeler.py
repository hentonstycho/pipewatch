"""Tests for pipewatch/labeler.py"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.checker import CheckResult
from pipewatch.labeler import (
    label_result,
    label_all,
    SEVERITY_OK,
    SEVERITY_WARNING,
    SEVERITY_CRITICAL,
)


@pytest.fixture()
def hist_dir(tmp_path: Path) -> str:
    return str(tmp_path / "history")


def _write_history(hist_dir: str, pipeline: str, entries: list[dict]) -> None:
    os.makedirs(hist_dir, exist_ok=True)
    path = Path(hist_dir) / f"{pipeline}.jsonl"
    with path.open("w") as fh:
        for entry in entries:
            fh.write(json.dumps(entry) + "\n")


def _fail(pipeline: str = "pipe") -> CheckResult:
    return CheckResult(pipeline=pipeline, healthy=False, violations=["row_count"])


def _ok(pipeline: str = "pipe") -> CheckResult:
    return CheckResult(pipeline=pipeline, healthy=True, violations=[])


def test_healthy_result_is_labelled_ok(hist_dir):
    lr = label_result(_ok(), hist_dir)
    assert lr.severity == SEVERITY_OK
    assert lr.reason is None


def test_single_failure_is_info_below_warning_threshold(hist_dir):
    _write_history(hist_dir, "pipe", [{"healthy": False}])
    lr = label_result(_fail(), hist_dir, warning_after=2, critical_after=5)
    # 1 consecutive failure < warning_after=2 → ok severity
    assert lr.severity == SEVERITY_OK


def test_two_consecutive_failures_is_warning(hist_dir):
    _write_history(
        hist_dir,
        "pipe",
        [{"healthy": False}, {"healthy": False}],
    )
    lr = label_result(_fail(), hist_dir, warning_after=2, critical_after=5)
    assert lr.severity == SEVERITY_WARNING


def test_five_consecutive_failures_is_critical(hist_dir):
    _write_history(
        hist_dir,
        "pipe",
        [{"healthy": False}] * 5,
    )
    lr = label_result(_fail(), hist_dir, warning_after=2, critical_after=5)
    assert lr.severity == SEVERITY_CRITICAL


def test_reason_contains_pipeline_name(hist_dir):
    _write_history(hist_dir, "pipe", [{"healthy": False}] * 3)
    lr = label_result(_fail(), hist_dir, warning_after=2, critical_after=5)
    assert "pipe" in (lr.reason or "")


def test_reason_contains_violations(hist_dir):
    _write_history(hist_dir, "pipe", [{"healthy": False}] * 3)
    lr = label_result(_fail(), hist_dir, warning_after=2, critical_after=5)
    assert "row_count" in (lr.reason or "")


def test_label_all_returns_one_per_result(hist_dir):
    results = [_ok("a"), _fail("b"), _ok("c")]
    labeled = label_all(results, hist_dir)
    assert len(labeled) == 3


def test_label_all_ok_pipelines_are_ok(hist_dir):
    results = [_ok("a"), _ok("b")]
    labeled = label_all(results, hist_dir)
    assert all(lr.severity == SEVERITY_OK for lr in labeled)
