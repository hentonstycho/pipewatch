"""cadence.py – detect whether a pipeline is running on its expected schedule.

Compares the gap between the two most recent history entries against a
configured expected interval (in minutes).  If the observed gap exceeds
``tolerance_factor * expected_minutes`` the pipeline is considered off-cadence.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.config import PipewatchConfig

_DEFAULT_TOLERANCE = 1.5


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _history_path(history_dir: str, pipeline_name: str) -> Path:
    return Path(history_dir) / f"{pipeline_name}.jsonl"


def _load_timestamps(history_dir: str, pipeline_name: str) -> List[datetime]:
    path = _history_path(history_dir, pipeline_name)
    if not path.exists():
        return []
    timestamps: List[datetime] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
            ts = datetime.fromisoformat(entry["checked_at"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            timestamps.append(ts)
        except (KeyError, ValueError):
            continue
    return sorted(timestamps)


@dataclass
class CadenceResult:
    pipeline: str
    expected_minutes: Optional[float]
    observed_gap_minutes: Optional[float]
    off_cadence: bool
    reason: str

    def __str__(self) -> str:  # pragma: no cover
        status = "OFF-CADENCE" if self.off_cadence else "OK"
        return f"{self.pipeline}: {status} – {self.reason}"


def check_cadence(
    pipeline_name: str,
    expected_minutes: float,
    history_dir: str,
    tolerance_factor: float = _DEFAULT_TOLERANCE,
) -> CadenceResult:
    """Return a CadenceResult for a single pipeline."""
    timestamps = _load_timestamps(history_dir, pipeline_name)
    if len(timestamps) < 2:
        return CadenceResult(
            pipeline=pipeline_name,
            expected_minutes=expected_minutes,
            observed_gap_minutes=None,
            off_cadence=False,
            reason="insufficient history",
        )
    gap_seconds = (timestamps[-1] - timestamps[-2]).total_seconds()
    gap_minutes = gap_seconds / 60.0
    threshold = expected_minutes * tolerance_factor
    off = gap_minutes > threshold
    reason = (
        f"gap {gap_minutes:.1f}m exceeds {threshold:.1f}m"
        if off
        else f"gap {gap_minutes:.1f}m within {threshold:.1f}m"
    )
    return CadenceResult(
        pipeline=pipeline_name,
        expected_minutes=expected_minutes,
        observed_gap_minutes=round(gap_minutes, 2),
        off_cadence=off,
        reason=reason,
    )


def check_all_cadences(
    config: PipewatchConfig,
    history_dir: str,
    tolerance_factor: float = _DEFAULT_TOLERANCE,
) -> List[CadenceResult]:
    """Check cadence for every pipeline that declares expected_interval_minutes."""
    results: List[CadenceResult] = []
    for pipeline in config.pipelines:
        expected = getattr(pipeline, "expected_interval_minutes", None)
        if expected is None:
            continue
        results.append(
            check_cadence(pipeline.name, expected, history_dir, tolerance_factor)
        )
    return results
