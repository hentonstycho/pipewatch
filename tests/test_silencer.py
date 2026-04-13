"""Tests for pipewatch.silencer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.silencer import (
    get_expiry,
    is_silenced,
    silence_pipeline,
    unsilence_pipeline,
)


@pytest.fixture()
def silence_file(tmp_path: Path) -> Path:
    return tmp_path / "silences.json"


def test_silence_pipeline_creates_file(silence_file: Path) -> None:
    silence_pipeline("my_pipeline", 30, silence_path=silence_file)
    assert silence_file.exists()


def test_is_silenced_returns_true_within_window(silence_file: Path) -> None:
    silence_pipeline("my_pipeline", 60, silence_path=silence_file)
    assert is_silenced("my_pipeline", silence_path=silence_file) is True


def test_is_silenced_returns_false_for_unknown_pipeline(silence_file: Path) -> None:
    assert is_silenced("unknown", silence_path=silence_file) is False


def test_is_silenced_returns_false_after_expiry(silence_file: Path) -> None:
    # Write an already-expired timestamp directly.
    import json

    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    silence_file.parent.mkdir(parents=True, exist_ok=True)
    silence_file.write_text(json.dumps({"old_pipe": past}))
    assert is_silenced("old_pipe", silence_path=silence_file) is False


def test_unsilence_pipeline_removes_entry(silence_file: Path) -> None:
    silence_pipeline("pipe_a", 30, silence_path=silence_file)
    removed = unsilence_pipeline("pipe_a", silence_path=silence_file)
    assert removed is True
    assert is_silenced("pipe_a", silence_path=silence_file) is False


def test_unsilence_pipeline_returns_false_when_not_present(silence_file: Path) -> None:
    removed = unsilence_pipeline("nonexistent", silence_path=silence_file)
    assert removed is False


def test_get_expiry_returns_datetime(silence_file: Path) -> None:
    before = datetime.now(timezone.utc)
    silence_pipeline("pipe_b", 15, silence_path=silence_file)
    expiry = get_expiry("pipe_b", silence_path=silence_file)
    assert expiry is not None
    assert expiry > before


def test_get_expiry_returns_none_when_not_silenced(silence_file: Path) -> None:
    assert get_expiry("missing", silence_path=silence_file) is None


def test_silence_pipeline_returns_expiry_datetime(silence_file: Path) -> None:
    expiry = silence_pipeline("pipe_c", 10, silence_path=silence_file)
    assert isinstance(expiry, datetime)
    assert expiry > datetime.now(timezone.utc)


def test_multiple_pipelines_silenced_independently(silence_file: Path) -> None:
    silence_pipeline("pipe_x", 60, silence_path=silence_file)
    silence_pipeline("pipe_y", 60, silence_path=silence_file)
    assert is_silenced("pipe_x", silence_path=silence_file) is True
    assert is_silenced("pipe_y", silence_path=silence_file) is True
    unsilence_pipeline("pipe_x", silence_path=silence_file)
    assert is_silenced("pipe_x", silence_path=silence_file) is False
    assert is_silenced("pipe_y", silence_path=silence_file) is True
