"""Tests for pipewatch.auditor."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.auditor import audit_summary, load_audit_log, record_audit
from pipewatch.checker import CheckResult


@pytest.fixture()
def audit_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _result(name: str, healthy: bool, violations: list[str] | None = None) -> CheckResult:
    return CheckResult(
        pipeline_name=name,
        healthy=healthy,
        violations=violations or [],
        last_checked="2024-01-01T00:00:00+00:00",
    )


def test_record_audit_creates_file(audit_dir: str) -> None:
    record_audit(_result("pipe_a", True), data_dir=audit_dir)
    audit_file = Path(audit_dir) / "audit.jsonl"
    assert audit_file.exists()


def test_record_audit_appends_json_line(audit_dir: str) -> None:
    record_audit(_result("pipe_a", True), data_dir=audit_dir)
    record_audit(_result("pipe_a", False, ["row_count"]), data_dir=audit_dir)
    lines = (Path(audit_dir) / "audit.jsonl").read_text().splitlines()
    assert len(lines) == 2
    entry = json.loads(lines[1])
    assert entry["healthy"] is False
    assert "row_count" in entry["violations"]


def test_record_audit_stores_action(audit_dir: str) -> None:
    record_audit(_result("pipe_a", True), action="scheduled", data_dir=audit_dir)
    entries = load_audit_log(audit_dir)
    assert entries[0]["action"] == "scheduled"


def test_load_audit_log_empty_when_missing(audit_dir: str) -> None:
    entries = load_audit_log(audit_dir)
    assert entries == []


def test_load_audit_log_returns_all_entries(audit_dir: str) -> None:
    for i in range(5):
        record_audit(_result(f"pipe_{i}", i % 2 == 0), data_dir=audit_dir)
    entries = load_audit_log(audit_dir)
    assert len(entries) == 5


def test_audit_summary_counts(audit_dir: str) -> None:
    record_audit(_result("pipe_a", True), data_dir=audit_dir)
    record_audit(_result("pipe_a", False, ["latency"]), data_dir=audit_dir)
    record_audit(_result("pipe_b", True), data_dir=audit_dir)
    summary = audit_summary(audit_dir)
    assert summary["total_checks"] == 3
    assert summary["healthy_checks"] == 2
    assert summary["failed_checks"] == 1
    assert sorted(summary["pipelines"]) == ["pipe_a", "pipe_b"]


def test_audit_summary_empty(audit_dir: str) -> None:
    summary = audit_summary(audit_dir)
    assert summary["total_checks"] == 0
    assert summary["last_checked"] is None
