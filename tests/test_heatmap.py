"""Tests for pipewatch.heatmap"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig
from pipewatch.heatmap import HeatmapRow, build_heatmap, format_heatmap


@pytest.fixture()
def simple_config() -> PipewatchConfig:
    return PipewatchConfig(
        pipelines=[
            PipelineConfig(name="pipe_a", thresholds=ThresholdConfig()),
            PipelineConfig(name="pipe_b", thresholds=ThresholdConfig()),
        ],
        notifications=NotificationConfig(),
    )


@pytest.fixture()
def hist_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _write(hist_dir: str, pipeline: str, entries: list[dict]) -> None:
    path = Path(hist_dir) / f"{pipeline}.jsonl"
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def test_build_heatmap_empty_history(simple_config, hist_dir):
    rows = build_heatmap(simple_config, history_dir=hist_dir)
    assert len(rows) == 2
    assert all(r.total_failures == 0 for r in rows)


def test_build_heatmap_counts_failures(simple_config, hist_dir):
    _write(hist_dir, "pipe_a", [
        {"healthy": False, "checked_at": "2024-01-15T03:00:00"},
        {"healthy": False, "checked_at": "2024-01-15T03:30:00"},
        {"healthy": True, "checked_at": "2024-01-15T03:45:00"},
        {"healthy": False, "checked_at": "2024-01-15T14:00:00"},
    ])
    rows = build_heatmap(simple_config, history_dir=hist_dir)
    row_a = next(r for r in rows if r.pipeline == "pipe_a")
    assert row_a.buckets[3] == 2
    assert row_a.buckets[14] == 1
    assert row_a.total_failures == 3


def test_build_heatmap_ignores_healthy(simple_config, hist_dir):
    _write(hist_dir, "pipe_b", [
        {"healthy": True, "checked_at": "2024-01-15T10:00:00"},
    ])
    rows = build_heatmap(simple_config, history_dir=hist_dir)
    row_b = next(r for r in rows if r.pipeline == "pipe_b")
    assert row_b.total_failures == 0


def test_peak_hour_returns_busiest_hour():
    row = HeatmapRow(pipeline="x", buckets=[0] * 24)
    row.buckets[7] = 5
    row.buckets[22] = 3
    assert row.peak_hour == 7


def test_format_heatmap_empty_returns_no_data():
    assert format_heatmap([]) == "No data."


def test_format_heatmap_contains_pipeline_name(simple_config, hist_dir):
    rows = build_heatmap(simple_config, history_dir=hist_dir)
    output = format_heatmap(rows)
    assert "pipe_a" in output
    assert "pipe_b" in output


def test_format_heatmap_shows_peak_and_total(simple_config, hist_dir):
    _write(hist_dir, "pipe_a", [
        {"healthy": False, "checked_at": "2024-01-15T09:00:00"},
    ])
    rows = build_heatmap(simple_config, history_dir=hist_dir)
    output = format_heatmap(rows)
    assert "peak=09h" in output
    assert "total=1" in output
