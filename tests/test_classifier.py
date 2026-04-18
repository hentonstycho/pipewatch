"""Tests for pipewatch.classifier."""
import json
import os
from pathlib import Path

import pytest

from pipewatch.classifier import (
    TIER_CRITICAL,
    TIER_DEGRADED,
    TIER_HEALTHY,
    TIER_WARNING,
    ClassificationResult,
    classify_all,
    classify_pipeline,
)


def _write_history(hist_dir: Path, name: str, entries: list) -> None:
    hist_dir.mkdir(parents=True, exist_ok=True)
    path = hist_dir / f"{name}.jsonl"
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")


@pytest.fixture()
def hist_dir(tmp_path):
    return tmp_path / "history"


def test_no_history_returns_healthy(hist_dir):
    result = classify_pipeline("pipe", history_dir=str(hist_dir))
    assert result.tier == TIER_HEALTHY
    assert result.sample_size == 0
    assert result.failure_rate == 0.0


def test_all_healthy_is_healthy(hist_dir):
    entries = [{"healthy": True}] * 10
    _write_history(hist_dir, "pipe", entries)
    result = classify_pipeline("pipe", history_dir=str(hist_dir))
    assert result.tier == TIER_HEALTHY
    assert result.failure_rate == 0.0


def test_all_failing_is_critical(hist_dir):
    entries = [{"healthy": False}] * 10
    _write_history(hist_dir, "pipe", entries)
    result = classify_pipeline("pipe", history_dir=str(hist_dir))
    assert result.tier == TIER_CRITICAL
    assert result.failure_rate == 1.0


def test_half_failing_is_degraded(hist_dir):
    entries = [{"healthy": i % 2 == 0} for i in range(10)]
    _write_history(hist_dir, "pipe", entries)
    result = classify_pipeline("pipe", history_dir=str(hist_dir))
    assert result.tier == TIER_DEGRADED


def test_quarter_failing_is_warning(hist_dir):
    entries = [{"healthy": False}] * 3 + [{"healthy": True}] * 9
    _write_history(hist_dir, "pipe", entries)
    result = classify_pipeline("pipe", history_dir=str(hist_dir))
    assert result.tier == TIER_WARNING


def test_window_limits_sample(hist_dir):
    # 15 failures then 10 healthy — window=10 should see only healthy
    entries = [{"healthy": False}] * 15 + [{"healthy": True}] * 10
    _write_history(hist_dir, "pipe", entries)
    result = classify_pipeline("pipe", history_dir=str(hist_dir), window=10)
    assert result.tier == TIER_HEALTHY
    assert result.sample_size == 10


def test_str_representation(hist_dir):
    result = ClassificationResult(pipeline="p", tier=TIER_WARNING, failure_rate=0.3, sample_size=10)
    assert "p" in str(result)
    assert TIER_WARNING in str(result)


def test_classify_all(hist_dir):
    from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig
    _write_history(hist_dir, "a", [{"healthy": False}] * 5)
    _write_history(hist_dir, "b", [{"healthy": True}] * 5)
    pipelines = [
        PipelineConfig(name="a", source="s", thresholds=ThresholdConfig()),
        PipelineConfig(name="b", source="s", thresholds=ThresholdConfig()),
    ]
    cfg = PipewatchConfig(pipelines=pipelines, notifications=NotificationConfig())
    results = classify_all(cfg, history_dir=str(hist_dir))
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"a", "b"}
