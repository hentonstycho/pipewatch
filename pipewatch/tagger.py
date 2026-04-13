"""Pipeline tagging — attach arbitrary string tags to pipelines and filter by them."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

_TAGS_FILENAME = "pipeline_tags.json"


def _tags_path(data_dir: str = ".pipewatch") -> Path:
    return Path(data_dir) / _TAGS_FILENAME


def _load_tags(data_dir: str = ".pipewatch") -> Dict[str, List[str]]:
    path = _tags_path(data_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        return json.load(fh)


def _save_tags(tags: Dict[str, List[str]], data_dir: str = ".pipewatch") -> None:
    path = _tags_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(tags, fh, indent=2)


def set_tags(pipeline: str, tags: List[str], data_dir: str = ".pipewatch") -> None:
    """Replace the full tag list for *pipeline*."""
    all_tags = _load_tags(data_dir)
    all_tags[pipeline] = sorted(set(tags))
    _save_tags(all_tags, data_dir)


def add_tag(pipeline: str, tag: str, data_dir: str = ".pipewatch") -> None:
    """Add a single tag to *pipeline* (idempotent)."""
    all_tags = _load_tags(data_dir)
    existing = set(all_tags.get(pipeline, []))
    existing.add(tag)
    all_tags[pipeline] = sorted(existing)
    _save_tags(all_tags, data_dir)


def remove_tag(pipeline: str, tag: str, data_dir: str = ".pipewatch") -> None:
    """Remove *tag* from *pipeline*; no-op if not present."""
    all_tags = _load_tags(data_dir)
    existing = set(all_tags.get(pipeline, []))
    existing.discard(tag)
    all_tags[pipeline] = sorted(existing)
    _save_tags(all_tags, data_dir)


def get_tags(pipeline: str, data_dir: str = ".pipewatch") -> List[str]:
    """Return the tags for *pipeline*, or an empty list."""
    return _load_tags(data_dir).get(pipeline, [])


def pipelines_with_tag(tag: str, data_dir: str = ".pipewatch") -> List[str]:
    """Return all pipeline names that carry *tag*."""
    return [
        name
        for name, tags in _load_tags(data_dir).items()
        if tag in tags
    ]


def filter_by_tags(
    pipelines: List[str],
    required_tags: Optional[List[str]],
    data_dir: str = ".pipewatch",
) -> List[str]:
    """Return the subset of *pipelines* that carry ALL of *required_tags*.

    If *required_tags* is None or empty every pipeline is returned unchanged.
    """
    if not required_tags:
        return pipelines
    all_tags = _load_tags(data_dir)
    required = set(required_tags)
    return [p for p in pipelines if required.issubset(set(all_tags.get(p, [])))]
