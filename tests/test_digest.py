"""Tests for pipewatch.digest."""

from __future__ import annotations

import json
import os
import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.digest import DigestEntry, Digest, build_digest
from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig


@pytest.fixture()
def simple_config(tmp_path):
    return PipewatchConfig(
        pipelines=[
            PipelineConfig(
                name="orders",
                thresholds=ThresholdConfig(min_row_count=10, max_error_rate=0.05, max_latency_seconds=60.0),
            )
        ],
        notifications=None,
    )


def test_digest_entry_fields():
    e = DigestEntry(
        pipeline="orders",
        success_rate=0.95,
        avg_latency_seconds=12.3,
        avg_row_count=500.0,
        total_checks=20,
        consecutive_failures=0,
    )
    assert e.pipeline == "orders"
    assert e.success_rate == pytest.approx(0.95)


def test_digest_to_text_contains_pipeline():
    d = Digest(
        generated_at=datetime.datetime(2024, 6, 1, 12, 0, 0),
        overall_healthy=True,
        entries=[
            DigestEntry(
                pipeline="orders",
                success_rate=1.0,
                avg_latency_seconds=5.0,
                avg_row_count=100.0,
                total_checks=10,
                consecutive_failures=0,
            )
        ],
    )
    text = d.to_text()
    assert "orders" in text
    assert "100%" in text
    assert "5.0s" in text
    assert "OK" in text


def test_digest_to_text_degraded():
    d = Digest(
        generated_at=datetime.datetime(2024, 6, 1, 12, 0, 0),
        overall_healthy=False,
        entries=[],
    )
    assert "DEGRADED" in d.to_text()


def test_digest_to_text_none_values():
    d = Digest(
        generated_at=datetime.datetime(2024, 6, 1, 12, 0, 0),
        overall_healthy=True,
        entries=[
            DigestEntry(
                pipeline="empty",
                success_rate=None,
                avg_latency_seconds=None,
                avg_row_count=None,
                total_checks=0,
                consecutive_failures=0,
            )
        ],
    )
    text = d.to_text()
    assert "n/a" in text


def test_build_digest_returns_digest(simple_config, tmp_path):
    with patch("pipewatch.digest.compute_all_metrics") as mock_metrics, \
         patch("pipewatch.digest.build_report") as mock_report:
        mock_metrics.return_value = {}
        mock_report.return_value = MagicMock(overall_healthy=True)
        result = build_digest(simple_config, str(tmp_path))
    assert isinstance(result, Digest)
    assert result.overall_healthy is True
    assert isinstance(result.generated_at, datetime.datetime)
