"""Tests for pipewatch.grapher."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.grapher import (
    GraphResult,
    _normalise,
    _spark_char,
    build_graph,
    build_all_graphs,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_history(hist_dir: Path, name: str, entries: list[dict]) -> None:
    path = hist_dir / f"{name}.jsonl"
    with path.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    d = tmp_path / "history"
    d.mkdir()
    return d


def _make_config(names: list[str]):
    from pipewatch.config import PipewatchConfig, PipelineConfig, ThresholdConfig, NotificationConfig
    pipelines = [
        PipelineConfig(name=n, thresholds=ThresholdConfig())
        for n in names
    ]
    return PipewatchConfig(
        pipelines=pipelines,
        notifications=NotificationConfig(),
    )


# ---------------------------------------------------------------------------
# unit tests
# ---------------------------------------------------------------------------

def test_normalise_uniform_returns_half():
    result = _normalise([5.0, 5.0, 5.0])
    assert all(v == 0.5 for v in result)


def test_normalise_range():
    result = _normalise([0.0, 0.5, 1.0])
    assert result[0] == pytest.approx(0.0)
    assert result[-1] == pytest.approx(1.0)


def test_spark_char_zero_is_space():
    assert _spark_char(0.0) == " "


def test_spark_char_one_is_full_block():
    assert _spark_char(1.0) == "█"


def test_build_graph_no_history_returns_none(hist_dir: Path):
    result = build_graph("missing", str(hist_dir))
    assert result is None


def test_build_graph_all_healthy(hist_dir: Path):
    entries = [{"healthy": True} for _ in range(5)]
    _write_history(hist_dir, "pipe_a", entries)
    result = build_graph("pipe_a", str(hist_dir), window=10)
    assert result is not None
    assert result.failures == 0
    assert result.total == 5
    assert len(result.sparkline) == 5


def test_build_graph_all_failing(hist_dir: Path):
    entries = [{"healthy": False} for _ in range(4)]
    _write_history(hist_dir, "pipe_b", entries)
    result = build_graph("pipe_b", str(hist_dir), window=10)
    assert result is not None
    assert result.failures == 4


def test_build_graph_window_limits_entries(hist_dir: Path):
    entries = [{"healthy": True} for _ in range(50)]
    _write_history(hist_dir, "pipe_c", entries)
    result = build_graph("pipe_c", str(hist_dir), window=10)
    assert result is not None
    assert result.total == 10
    assert len(result.sparkline) == 10


def test_build_graph_str_contains_pipeline_name(hist_dir: Path):
    _write_history(hist_dir, "my_pipe", [{"healthy": True}])
    result = build_graph("my_pipe", str(hist_dir))
    assert result is not None
    assert "my_pipe" in str(result)


def test_build_all_graphs_skips_missing(hist_dir: Path):
    cfg = _make_config(["exists", "missing"])
    _write_history(hist_dir, "exists", [{"healthy": True}])
    results = build_all_graphs(cfg, str(hist_dir))
    assert len(results) == 1
    assert results[0].pipeline == "exists"
