"""Tests for pipewatch.pager."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.config import PipelineConfig, ThresholdConfig
from pipewatch.pager import evaluate_page, evaluate_all_pages, PageEvent


@pytest.fixture()
def hist_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _pipeline(name: str = "pipe_a") -> PipelineConfig:
    return PipelineConfig(name=name, source="db", thresholds=ThresholdConfig())


def _write_history(hist_dir: str, name: str, entries: list[dict]) -> None:
    path = Path(hist_dir) / f"{name}.jsonl"
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _entry(healthy: bool) -> dict:
    return {"pipeline": "pipe_a", "healthy": healthy, "checked_at": "2024-01-01T00:00:00"}


def test_no_history_does_not_page(hist_dir):
    event = evaluate_page(_pipeline(), history_dir=hist_dir, escalate_after=3)
    assert event.should_page is False
    assert event.consecutive == 0


def test_below_threshold_does_not_page(hist_dir):
    _write_history(hist_dir, "pipe_a", [_entry(False), _entry(False)])
    event = evaluate_page(_pipeline(), history_dir=hist_dir, escalate_after=3)
    assert event.should_page is False


def test_exactly_at_threshold_pages(hist_dir):
    _write_history(hist_dir, "pipe_a", [_entry(False)] * 3)
    event = evaluate_page(_pipeline(), history_dir=hist_dir, escalate_after=3)
    assert event.should_page is True
    assert event.consecutive == 3
    assert "pipe_a" in event.message
    assert "3" in event.message


def test_above_threshold_pages(hist_dir):
    _write_history(hist_dir, "pipe_a", [_entry(False)] * 5)
    event = evaluate_page(_pipeline(), history_dir=hist_dir, escalate_after=3)
    assert event.should_page is True
    assert event.consecutive == 5


def test_healthy_run_resets_streak(hist_dir):
    entries = [_entry(False), _entry(False), _entry(True)]
    _write_history(hist_dir, "pipe_a", entries)
    event = evaluate_page(_pipeline(), history_dir=hist_dir, escalate_after=3)
    assert event.should_page is False


def test_evaluate_all_pages_filters_non_paging(hist_dir):
    pipes = [_pipeline("pipe_a"), _pipeline("pipe_b")]
    _write_history(hist_dir, "pipe_a", [_entry(False)] * 4)
    # pipe_b has no history
    results = evaluate_all_pages(pipes, history_dir=hist_dir, escalate_after=3)
    assert len(results) == 1
    assert results[0].pipeline == "pipe_a"


def test_evaluate_all_pages_empty_when_all_healthy(hist_dir):
    pipes = [_pipeline("pipe_a")]
    _write_history(hist_dir, "pipe_a", [_entry(True)] * 5)
    results = evaluate_all_pages(pipes, history_dir=hist_dir, escalate_after=3)
    assert results == []
