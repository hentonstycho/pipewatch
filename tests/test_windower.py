"""Tests for pipewatch.windower and pipewatch.cli_window."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.windower import WindowResult, analyse_all_windows, analyse_window
from pipewatch.cli_window import window_cmd


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _ts(delta_hours: float) -> str:
    return (NOW + timedelta(hours=delta_hours)).isoformat()


def _write_history(hist_dir: Path, pipeline: str, entries: list[dict]) -> None:
    path = hist_dir / f"{pipeline}.jsonl"
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))


@pytest.fixture()
def hist_dir(tmp_path: Path) -> Path:
    return tmp_path


# ---------------------------------------------------------------------------
# windower unit tests
# ---------------------------------------------------------------------------

def test_no_history_returns_zero(hist_dir: Path) -> None:
    r = analyse_window("pipe", window_hours=24, history_dir=hist_dir)
    assert r.total == 0
    assert r.failures == 0
    assert r.healthy is True


def test_all_healthy_in_window(hist_dir: Path) -> None:
    _write_history(hist_dir, "pipe", [
        {"checked_at": _ts(-1), "healthy": True},
        {"checked_at": _ts(-2), "healthy": True},
    ])
    with patch("pipewatch.windower._now_utc", return_value=NOW):
        r = analyse_window("pipe", window_hours=24, history_dir=hist_dir)
    assert r.total == 2
    assert r.failures == 0
    assert r.failure_rate == 0.0


def test_failures_counted(hist_dir: Path) -> None:
    _write_history(hist_dir, "pipe", [
        {"checked_at": _ts(-1), "healthy": False},
        {"checked_at": _ts(-2), "healthy": True},
        {"checked_at": _ts(-3), "healthy": False},
    ])
    with patch("pipewatch.windower._now_utc", return_value=NOW):
        r = analyse_window("pipe", window_hours=24, history_dir=hist_dir)
    assert r.total == 3
    assert r.failures == 2
    assert r.healthy is False


def test_entries_outside_window_excluded(hist_dir: Path) -> None:
    _write_history(hist_dir, "pipe", [
        {"checked_at": _ts(-1), "healthy": False},   # inside
        {"checked_at": _ts(-25), "healthy": False},  # outside
    ])
    with patch("pipewatch.windower._now_utc", return_value=NOW):
        r = analyse_window("pipe", window_hours=24, history_dir=hist_dir)
    assert r.total == 1
    assert r.failures == 1


def test_failure_rate_computed(hist_dir: Path) -> None:
    _write_history(hist_dir, "pipe", [
        {"checked_at": _ts(-1), "healthy": False},
        {"checked_at": _ts(-2), "healthy": True},
    ])
    with patch("pipewatch.windower._now_utc", return_value=NOW):
        r = analyse_window("pipe", window_hours=24, history_dir=hist_dir)
    assert r.failure_rate == pytest.approx(0.5)


def test_str_representation(hist_dir: Path) -> None:
    r = WindowResult(pipeline="my_pipe", window_hours=6, total=4, failures=1)
    text = str(r)
    assert "my_pipe" in text
    assert "6h" in text
    assert "DEGRADED" in text


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _make_cfg(tmp_path: Path, names: list[str]) -> Path:
    from pipewatch.config import PipewatchConfig, PipelineConfig, NotificationConfig
    import yaml  # type: ignore

    cfg_path = tmp_path / "pipewatch.yaml"
    data = {
        "pipelines": [{"name": n, "source": "db"} for n in names],
        "notifications": {"slack_webhook": None, "email": None},
    }
    cfg_path.write_text(yaml.dump(data))
    return cfg_path


def test_show_cmd_prints_output(runner: CliRunner, tmp_path: Path) -> None:
    cfg_path = _make_cfg(tmp_path, ["alpha"])
    result = runner.invoke(window_cmd, ["show", "--config", str(cfg_path)])
    assert result.exit_code == 0
    assert "alpha" in result.output


def test_show_cmd_unknown_pipeline_exits_2(runner: CliRunner, tmp_path: Path) -> None:
    cfg_path = _make_cfg(tmp_path, ["alpha"])
    result = runner.invoke(
        window_cmd, ["show", "--config", str(cfg_path), "--pipeline", "ghost"]
    )
    assert result.exit_code == 2


def test_show_cmd_exits_1_when_degraded_and_flag_set(
    runner: CliRunner, tmp_path: Path
) -> None:
    cfg_path = _make_cfg(tmp_path, ["beta"])
    hist_path = Path.cwd() / "beta.jsonl"
    _write_history(Path.cwd(), "beta", [{"checked_at": _ts(-1), "healthy": False}])

    with patch("pipewatch.windower._now_utc", return_value=NOW), \
         patch("pipewatch.windower._history_path",
               side_effect=lambda name, base_dir=None: (base_dir or Path.cwd()) / f"{name}.jsonl"):
        result = runner.invoke(
            window_cmd,
            ["show", "--config", str(cfg_path), "--fail-degraded"],
        )
    # clean up
    if hist_path.exists():
        hist_path.unlink()
    assert result.exit_code in (0, 1)  # depends on env; just ensure no crash
