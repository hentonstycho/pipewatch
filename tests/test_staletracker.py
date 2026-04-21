"""Tests for pipewatch.staletracker."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.staletracker import StaleEntry, track_all, track_pipeline
from pipewatch.cli_staletrack import staletrack_cmd


UTC = timezone.utc


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    d = tmp_path / "history"
    d.mkdir()
    return d


def _write_entry(hist_dir: Path, pipeline: str, ts: datetime) -> None:
    line = json.dumps({"pipeline": pipeline, "checked_at": ts.isoformat(), "healthy": True})
    (hist_dir / f"{pipeline}.jsonl").write_text(line + "\n")


# ---------------------------------------------------------------------------
# Unit tests – track_pipeline
# ---------------------------------------------------------------------------

def test_no_history_is_stale(hist_dir: Path) -> None:
    entry = track_pipeline("pipe_a", max_age_minutes=30.0, history_dir=hist_dir)
    assert entry.is_stale is True
    assert entry.last_checked is None
    assert entry.age_minutes is None


def test_recent_entry_is_not_stale(hist_dir: Path) -> None:
    recent = datetime.now(UTC) - timedelta(minutes=5)
    _write_entry(hist_dir, "pipe_b", recent)
    entry = track_pipeline("pipe_b", max_age_minutes=30.0, history_dir=hist_dir)
    assert entry.is_stale is False
    assert entry.age_minutes is not None
    assert entry.age_minutes < 30.0


def test_old_entry_is_stale(hist_dir: Path) -> None:
    old = datetime.now(UTC) - timedelta(hours=3)
    _write_entry(hist_dir, "pipe_c", old)
    entry = track_pipeline("pipe_c", max_age_minutes=60.0, history_dir=hist_dir)
    assert entry.is_stale is True
    assert entry.age_minutes > 60.0


def test_str_no_history(hist_dir: Path) -> None:
    entry = track_pipeline("pipe_x", 30.0, hist_dir)
    assert "never checked" in str(entry)


def test_str_stale(hist_dir: Path) -> None:
    old = datetime.now(UTC) - timedelta(hours=2)
    _write_entry(hist_dir, "pipe_y", old)
    entry = track_pipeline("pipe_y", 30.0, hist_dir)
    assert "STALE" in str(entry)


def test_str_ok(hist_dir: Path) -> None:
    recent = datetime.now(UTC) - timedelta(minutes=1)
    _write_entry(hist_dir, "pipe_z", recent)
    entry = track_pipeline("pipe_z", 30.0, hist_dir)
    assert "ok" in str(entry)


# ---------------------------------------------------------------------------
# Unit tests – track_all
# ---------------------------------------------------------------------------

def _make_config(names):
    from unittest.mock import MagicMock
    cfg = MagicMock()
    cfg.pipelines = [MagicMock(name=n, max_age_minutes=None) for n in names]
    # MagicMock sets .name via constructor kwarg differently; fix it:
    for p, n in zip(cfg.pipelines, names):
        p.name = n
    return cfg


def test_track_all_returns_one_per_pipeline(hist_dir: Path) -> None:
    cfg = _make_config(["a", "b", "c"])
    results = track_all(cfg, hist_dir, default_max_age_minutes=30.0)
    assert len(results) == 3
    assert {r.pipeline for r in results} == {"a", "b", "c"}


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_check_cmd_prints_output(runner: CliRunner, tmp_path: Path, hist_dir: Path) -> None:
    cfg_file = tmp_path / "pipewatch.yaml"
    cfg_file.write_text("pipelines:\n  - name: alpha\n    source: dummy\n")
    recent = datetime.now(UTC) - timedelta(minutes=2)
    _write_entry(hist_dir, "alpha", recent)

    result = runner.invoke(
        staletrack_cmd,
        ["check", "--config", str(cfg_file), "--history-dir", str(hist_dir)],
    )
    assert result.exit_code == 0
    assert "alpha" in result.output


def test_check_cmd_unknown_pipeline_exits_2(runner: CliRunner, tmp_path: Path, hist_dir: Path) -> None:
    cfg_file = tmp_path / "pipewatch.yaml"
    cfg_file.write_text("pipelines:\n  - name: alpha\n    source: dummy\n")

    result = runner.invoke(
        staletrack_cmd,
        ["check", "--config", str(cfg_file), "--history-dir", str(hist_dir),
         "--pipeline", "nonexistent"],
    )
    assert result.exit_code == 2


def test_check_cmd_fail_stale_exits_1(runner: CliRunner, tmp_path: Path, hist_dir: Path) -> None:
    cfg_file = tmp_path / "pipewatch.yaml"
    cfg_file.write_text("pipelines:\n  - name: beta\n    source: dummy\n")
    # No history written → stale

    result = runner.invoke(
        staletrack_cmd,
        ["check", "--config", str(cfg_file), "--history-dir", str(hist_dir),
         "--fail-stale"],
    )
    assert result.exit_code == 1
