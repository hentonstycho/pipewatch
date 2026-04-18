"""Tests for pipewatch.quota."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch
from datetime import datetime, timezone

import pytest

from pipewatch.quota import record_alert, get_quota, is_quota_exhausted

TODAY = "2024-06-01"
_DT = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def quota_dir(tmp_path):
    return tmp_path / "quotas"


def _patch_now():
    return patch("pipewatch.quota._now_utc", return_value=_DT)


def test_get_quota_no_history_returns_zero(quota_dir):
    with _patch_now():
        result = get_quota("pipe_a", limit=5, base_dir=quota_dir)
    assert result.count == 0
    assert result.limit == 5
    assert not result.exhausted


def test_record_alert_increments(quota_dir):
    with _patch_now():
        r1 = record_alert("pipe_a", limit=5, base_dir=quota_dir)
        r2 = record_alert("pipe_a", limit=5, base_dir=quota_dir)
    assert r1.count == 1
    assert r2.count == 2


def test_record_alert_exhausted_when_over_limit(quota_dir):
    with _patch_now():
        for _ in range(3):
            record_alert("pipe_b", limit=2, base_dir=quota_dir)
        result = get_quota("pipe_b", limit=2, base_dir=quota_dir)
    assert result.exhausted


def test_is_quota_exhausted_false_below_limit(quota_dir):
    with _patch_now():
        record_alert("pipe_c", limit=10, base_dir=quota_dir)
    with _patch_now():
        assert not is_quota_exhausted("pipe_c", limit=10, base_dir=quota_dir)


def test_quota_resets_on_new_day(quota_dir):
    dt1 = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    dt2 = datetime(2024, 6, 2, 12, 0, tzinfo=timezone.utc)
    with patch("pipewatch.quota._now_utc", return_value=dt1):
        record_alert("pipe_d", limit=5, base_dir=quota_dir)
        record_alert("pipe_d", limit=5, base_dir=quota_dir)
    with patch("pipewatch.quota._now_utc", return_value=dt2):
        result = get_quota("pipe_d", limit=5, base_dir=quota_dir)
    assert result.count == 0
    assert result.date == "2024-06-02"


def test_str_representation(quota_dir):
    with _patch_now():
        result = get_quota("pipe_e", limit=5, base_dir=quota_dir)
    assert "pipe_e" in str(result)
    assert "0/5" in str(result)
