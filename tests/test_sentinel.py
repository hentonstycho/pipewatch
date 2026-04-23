"""Tests for pipewatch.sentinel."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.sentinel import (
    SentinelResult,
    check_all_sentinels,
    check_sentinel,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_history(hist_dir: Path, pipeline: str, entries: list[dict]) -> None:
    path = hist_dir / f"{pipeline}.jsonl"
    path.write_text(
        "\n".join(json.dumps(e) for e in entries) + "\n"
    )


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    d = tmp_path / "history"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# unit tests
# ---------------------------------------------------------------------------

def test_no_history_does_not_trigger(hist_dir: Path) -> None:
    result = check_sentinel("pipe_a", history_dir=str(hist_dir))
    assert result.triggered is False
    assert result.total_runs == 0
    assert result.healthy_runs == 0


def test_all_healthy_does_not_trigger(hist_dir: Path) -> None:
    _write_history(
        hist_dir, "pipe_a",
        [{"healthy": True}, {"healthy": True}],
    )
    result = check_sentinel("pipe_a", history_dir=str(hist_dir))
    assert result.triggered is False
    assert result.healthy_runs == 2
    assert result.total_runs == 2


def test_all_failing_triggers(hist_dir: Path) -> None:
    _write_history(
        hist_dir, "pipe_b",
        [{"healthy": False}, {"healthy": False}, {"healthy": False}],
    )
    result = check_sentinel("pipe_b", history_dir=str(hist_dir))
    assert result.triggered is True
    assert result.healthy_runs == 0
    assert result.total_runs == 3


def test_mixed_history_does_not_trigger(hist_dir: Path) -> None:
    _write_history(
        hist_dir, "pipe_c",
        [{"healthy": False}, {"healthy": True}],
    )
    result = check_sentinel("pipe_c", history_dir=str(hist_dir))
    assert result.triggered is False
    assert result.healthy_runs == 1


def test_str_triggered(hist_dir: Path) -> None:
    _write_history(hist_dir, "pipe_d", [{"healthy": False}])
    result = check_sentinel("pipe_d", history_dir=str(hist_dir))
    text = str(result)
    assert "SENTINEL" in text
    assert "pipe_d" in text


def test_str_ok(hist_dir: Path) -> None:
    _write_history(hist_dir, "pipe_e", [{"healthy": True}])
    result = check_sentinel("pipe_e", history_dir=str(hist_dir))
    text = str(result)
    assert "ok" in text
    assert "SENTINEL" not in text


def test_check_all_sentinels(hist_dir: Path) -> None:
    """check_all_sentinels returns one result per pipeline."""
    from unittest.mock import MagicMock

    _write_history(hist_dir, "alpha", [{"healthy": False}])
    _write_history(hist_dir, "beta", [{"healthy": True}])

    p_alpha = MagicMock()
    p_alpha.name = "alpha"
    p_beta = MagicMock()
    p_beta.name = "beta"

    cfg = MagicMock()
    cfg.pipelines = [p_alpha, p_beta]

    results = check_all_sentinels(cfg, history_dir=str(hist_dir))
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"alpha", "beta"}
    triggered = {r.pipeline for r in results if r.triggered}
    assert triggered == {"alpha"}
