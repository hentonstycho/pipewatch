"""Tests for pipewatch.annotator."""
import json
import pytest
from pathlib import Path

from pipewatch.annotator import (
    add_annotation,
    get_annotations,
    clear_annotations,
    delete_annotation,
)


@pytest.fixture()
def ann_dir(tmp_path: Path) -> str:
    return str(tmp_path / "annotations")


def test_add_annotation_creates_file(ann_dir):
    add_annotation(ann_dir, "my_pipeline", "first note")
    path = Path(ann_dir) / "my_pipeline.annotations.json"
    assert path.exists()


def test_add_annotation_returns_entry(ann_dir):
    entry = add_annotation(ann_dir, "pipe", "hello", author="alice")
    assert entry["note"] == "hello"
    assert entry["author"] == "alice"
    assert "timestamp" in entry


def test_get_annotations_empty_for_unknown(ann_dir):
    result = get_annotations(ann_dir, "nonexistent")
    assert result == []


def test_get_annotations_returns_all(ann_dir):
    add_annotation(ann_dir, "pipe", "note one")
    add_annotation(ann_dir, "pipe", "note two")
    entries = get_annotations(ann_dir, "pipe")
    assert len(entries) == 2
    assert entries[0]["note"] == "note one"
    assert entries[1]["note"] == "note two"


def test_annotations_persisted_as_valid_json(ann_dir):
    add_annotation(ann_dir, "pipe", "persisted")
    path = Path(ann_dir) / "pipe.annotations.json"
    data = json.loads(path.read_text())
    assert isinstance(data, list)
    assert data[0]["note"] == "persisted"


def test_clear_annotations_returns_count(ann_dir):
    add_annotation(ann_dir, "pipe", "a")
    add_annotation(ann_dir, "pipe", "b")
    count = clear_annotations(ann_dir, "pipe")
    assert count == 2
    assert get_annotations(ann_dir, "pipe") == []


def test_clear_annotations_on_empty_returns_zero(ann_dir):
    count = clear_annotations(ann_dir, "pipe")
    assert count == 0


def test_delete_annotation_removes_correct_entry(ann_dir):
    add_annotation(ann_dir, "pipe", "keep")
    add_annotation(ann_dir, "pipe", "remove me")
    result = delete_annotation(ann_dir, "pipe", 1)
    assert result is True
    entries = get_annotations(ann_dir, "pipe")
    assert len(entries) == 1
    assert entries[0]["note"] == "keep"


def test_delete_annotation_out_of_range_returns_false(ann_dir):
    add_annotation(ann_dir, "pipe", "only one")
    result = delete_annotation(ann_dir, "pipe", 5)
    assert result is False
    assert len(get_annotations(ann_dir, "pipe")) == 1


def test_annotations_isolated_per_pipeline(ann_dir):
    add_annotation(ann_dir, "pipe_a", "note for a")
    add_annotation(ann_dir, "pipe_b", "note for b")
    assert len(get_annotations(ann_dir, "pipe_a")) == 1
    assert len(get_annotations(ann_dir, "pipe_b")) == 1
    assert get_annotations(ann_dir, "pipe_a")[0]["note"] == "note for a"
