"""Tests for pipewatch.alerting."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.alerting import AlertPolicy, record_alert, should_alert
from pipewatch.checker import CheckResult


@pytest.fixture()
def tmp_dirs(tmp_path: Path):
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    cooldown_file = tmp_path / "cooldowns.json"
    return history_dir, cooldown_file


def _result(pipeline: str = "pipe", healthy: bool = False) -> CheckResult:
    return CheckResult(pipeline=pipeline, healthy=healthy, violations=[] if healthy else ["err"])


def _write_history(history_dir: Path, pipeline: str, entries: list[dict]) -> None:
    p = history_dir / f"{pipeline}.jsonl"
    with p.open("w") as fh:
        for entry in entries:
            fh.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------------------
# should_alert — basic cases
# ---------------------------------------------------------------------------

def test_healthy_result_never_alerts(tmp_dirs):
    history_dir, cooldown_file = tmp_dirs
    policy = AlertPolicy(min_consecutive_failures=1)
    assert should_alert(_result(healthy=True), policy, history_dir, cooldown_file) is False


def test_alerts_when_consecutive_failures_met(tmp_dirs):
    history_dir, cooldown_file = tmp_dirs
    _write_history(history_dir, "pipe", [{"healthy": False}])
    policy = AlertPolicy(min_consecutive_failures=1, cooldown_minutes=0)
    assert should_alert(_result(), policy, history_dir, cooldown_file) is True


def test_no_alert_when_below_min_consecutive(tmp_dirs):
    history_dir, cooldown_file = tmp_dirs
    # Only 1 failure in history but policy requires 3
    _write_history(history_dir, "pipe", [{"healthy": False}])
    policy = AlertPolicy(min_consecutive_failures=3, cooldown_minutes=0)
    assert should_alert(_result(), policy, history_dir, cooldown_file) is False


# ---------------------------------------------------------------------------
# should_alert — cooldown
# ---------------------------------------------------------------------------

def test_cooldown_suppresses_repeat_alert(tmp_dirs):
    history_dir, cooldown_file = tmp_dirs
    _write_history(history_dir, "pipe", [{"healthy": False}])
    policy = AlertPolicy(min_consecutive_failures=1, cooldown_minutes=60)

    # First alert — no prior cooldown entry
    assert should_alert(_result(), policy, history_dir, cooldown_file) is True

    # Simulate that we notified just now
    record_alert("pipe", cooldown_file)

    # Immediately after — should be suppressed
    assert should_alert(_result(), policy, history_dir, cooldown_file) is False


def test_cooldown_expired_allows_alert(tmp_dirs):
    history_dir, cooldown_file = tmp_dirs
    _write_history(history_dir, "pipe", [{"healthy": False}])
    policy = AlertPolicy(min_consecutive_failures=1, cooldown_minutes=30)

    past = datetime.now(tz=timezone.utc) - timedelta(minutes=60)
    cooldown_file.write_text(json.dumps({"pipe": past.isoformat()}))

    assert should_alert(_result(), policy, history_dir, cooldown_file) is True


# ---------------------------------------------------------------------------
# record_alert
# ---------------------------------------------------------------------------

def test_record_alert_writes_timestamp(tmp_dirs):
    _, cooldown_file = tmp_dirs
    record_alert("my_pipeline", cooldown_file)
    data = json.loads(cooldown_file.read_text())
    assert "my_pipeline" in data
    dt = datetime.fromisoformat(data["my_pipeline"])
    assert abs((datetime.now(tz=timezone.utc) - dt).total_seconds()) < 5


def test_record_alert_updates_existing(tmp_dirs):
    _, cooldown_file = tmp_dirs
    old_ts = (datetime.now(tz=timezone.utc) - timedelta(hours=2)).isoformat()
    cooldown_file.write_text(json.dumps({"my_pipeline": old_ts}))
    record_alert("my_pipeline", cooldown_file)
    data = json.loads(cooldown_file.read_text())
    new_dt = datetime.fromisoformat(data["my_pipeline"])
    assert new_dt > datetime.fromisoformat(old_ts)
