"""sampler.py – reservoir-based history sampling for pipelines.

Keeps a fixed-size random sample of CheckResult history entries per pipeline
so that long-running pipelines don't accumulate unbounded history on disk.
"""
from __future__ import annotations

import json
import random
from pathlib import Path
from typing import List

from pipewatch.checker import CheckResult

_DEFAULT_RESERVOIR = 200


def _sample_path(history_dir: Path, pipeline: str) -> Path:
    return history_dir / f"{pipeline}.sample.jsonl"


def _load_sample(path: Path) -> List[dict]:
    if not path.exists():
        return []
    lines = []
    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if raw:
            try:
                lines.append(json.loads(raw))
            except json.JSONDecodeError:
                pass
    return lines


def _save_sample(path: Path, entries: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")


def reservoir_sample(
    history_dir: Path,
    pipeline: str,
    result: CheckResult,
    reservoir_size: int = _DEFAULT_RESERVOIR,
) -> int:
    """Add *result* to the reservoir sample for *pipeline*.

    Uses Algorithm R so every entry has an equal probability of being
    retained.  Returns the current sample size after the update.
    """
    path = _sample_path(history_dir, pipeline)
    entries = _load_sample(path)
    entry = {
        "pipeline": result.pipeline,
        "healthy": result.healthy,
        "timestamp": result.timestamp,
        "violations": result.violations,
    }
    if len(entries) < reservoir_size:
        entries.append(entry)
    else:
        # Replace a random existing entry with decreasing probability.
        idx = random.randint(0, len(entries))  # inclusive upper bound
        if idx < reservoir_size:
            entries[idx] = entry
    _save_sample(path, entries)
    return len(entries)


def load_sample(history_dir: Path, pipeline: str) -> List[dict]:
    """Return the current reservoir sample for *pipeline*."""
    return _load_sample(_sample_path(history_dir, pipeline))


def clear_sample(history_dir: Path, pipeline: str) -> None:
    """Delete the reservoir sample file for *pipeline* if it exists."""
    path = _sample_path(history_dir, pipeline)
    if path.exists():
        path.unlink()
