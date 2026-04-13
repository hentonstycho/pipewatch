"""Tests for pipewatch.tagger."""
from __future__ import annotations

import pytest

from pipewatch.tagger import (
    add_tag,
    filter_by_tags,
    get_tags,
    pipelines_with_tag,
    remove_tag,
    set_tags,
)


@pytest.fixture()
def tag_dir(tmp_path):
    return str(tmp_path)


def test_set_tags_creates_file(tag_dir):
    set_tags("orders", ["critical", "daily"], data_dir=tag_dir)
    assert get_tags("orders", data_dir=tag_dir) == ["critical", "daily"]


def test_set_tags_deduplicates(tag_dir):
    set_tags("orders", ["critical", "critical", "daily"], data_dir=tag_dir)
    assert get_tags("orders", data_dir=tag_dir) == ["critical", "daily"]


def test_set_tags_replaces_existing(tag_dir):
    set_tags("orders", ["critical"], data_dir=tag_dir)
    set_tags("orders", ["nightly"], data_dir=tag_dir)
    assert get_tags("orders", data_dir=tag_dir) == ["nightly"]


def test_get_tags_unknown_pipeline_returns_empty(tag_dir):
    assert get_tags("nonexistent", data_dir=tag_dir) == []


def test_add_tag_idempotent(tag_dir):
    add_tag("orders", "critical", data_dir=tag_dir)
    add_tag("orders", "critical", data_dir=tag_dir)
    assert get_tags("orders", data_dir=tag_dir) == ["critical"]


def test_add_tag_appends(tag_dir):
    add_tag("orders", "critical", data_dir=tag_dir)
    add_tag("orders", "daily", data_dir=tag_dir)
    assert "daily" in get_tags("orders", data_dir=tag_dir)
    assert "critical" in get_tags("orders", data_dir=tag_dir)


def test_remove_tag_removes(tag_dir):
    set_tags("orders", ["critical", "daily"], data_dir=tag_dir)
    remove_tag("orders", "daily", data_dir=tag_dir)
    assert get_tags("orders", data_dir=tag_dir) == ["critical"]


def test_remove_tag_noop_when_absent(tag_dir):
    set_tags("orders", ["critical"], data_dir=tag_dir)
    remove_tag("orders", "nonexistent", data_dir=tag_dir)  # should not raise
    assert get_tags("orders", data_dir=tag_dir) == ["critical"]


def test_pipelines_with_tag(tag_dir):
    set_tags("orders", ["critical", "daily"], data_dir=tag_dir)
    set_tags("users", ["daily"], data_dir=tag_dir)
    set_tags("events", ["critical"], data_dir=tag_dir)

    assert set(pipelines_with_tag("critical", data_dir=tag_dir)) == {"orders", "events"}
    assert set(pipelines_with_tag("daily", data_dir=tag_dir)) == {"orders", "users"}


def test_filter_by_tags_returns_all_when_no_filter(tag_dir):
    pipelines = ["orders", "users", "events"]
    assert filter_by_tags(pipelines, None, data_dir=tag_dir) == pipelines
    assert filter_by_tags(pipelines, [], data_dir=tag_dir) == pipelines


def test_filter_by_tags_requires_all_tags(tag_dir):
    set_tags("orders", ["critical", "daily"], data_dir=tag_dir)
    set_tags("users", ["daily"], data_dir=tag_dir)

    result = filter_by_tags(["orders", "users"], ["critical", "daily"], data_dir=tag_dir)
    assert result == ["orders"]


def test_filter_by_tags_excludes_untagged(tag_dir):
    set_tags("orders", ["critical"], data_dir=tag_dir)
    # users has no tags
    result = filter_by_tags(["orders", "users"], ["critical"], data_dir=tag_dir)
    assert result == ["orders"]
