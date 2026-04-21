"""rollup.py – Periodic rollup of pipeline check results into summarised time buckets.

Supports hourly and daily rollups, storing aggregated stats so that long-term
trends can be queried without scanning every raw history entry.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional

from pipewatch.history import load_history
from pipewatch.config import PipewatchConfig

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


class RollupBucket:
    """Aggregated statistics for a single time bucket."""

    def __init__(
        self,
        pipeline: str,
        bucket: str,          # ISO-format truncated timestamp, e.g. "2024-06-01T14"
        granularity: str,     # "hourly" | "daily"
        total: int,
        failures: int,
        avg_latency_seconds: Optional[float],
    ) -> None:
        self.pipeline = pipeline
        self.bucket = bucket
        self.granularity = granularity
        self.total = total
        self.failures = failures
        self.avg_latency_seconds = avg_latency_seconds

    @property
    def success_rate(self) -> Optional[float]:
        if self.total == 0:
            return None
        return (self.total - self.failures) / self.total

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "bucket": self.bucket,
            "granularity": self.granularity,
            "total": self.total,
            "failures": self.failures,
            "avg_latency_seconds": self.avg_latency_seconds,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "RollupBucket":
        return cls(
            pipeline=d["pipeline"],
            bucket=d["bucket"],
            granularity=d["granularity"],
            total=d["total"],
            failures=d["failures"],
            avg_latency_seconds=d.get("avg_latency_seconds"),
        )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"RollupBucket({self.pipeline!r}, {self.bucket!r}, "
            f"total={self.total}, failures={self.failures})"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _rollup_path(history_dir: str, pipeline: str, granularity: str) -> Path:
    return Path(history_dir) / f"{pipeline}.rollup_{granularity}.jsonl"


def _bucket_key(ts: datetime, granularity: str) -> str:
    """Truncate *ts* to the appropriate bucket boundary."""
    if granularity == "hourly":
        return ts.strftime("%Y-%m-%dT%H")
    return ts.strftime("%Y-%m-%d")


def _parse_ts(ts_str: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(ts_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_rollup(
    pipeline: str,
    history_dir: str,
    granularity: str = "hourly",
    lookback_hours: int = 48,
) -> List[RollupBucket]:
    """Compute rollup buckets for *pipeline* over the last *lookback_hours* hours.

    Args:
        pipeline: Pipeline name matching a ``PipelineConfig`` key.
        history_dir: Directory where ``.jsonl`` history files live.
        granularity: ``"hourly"`` or ``"daily"``.
        lookback_hours: How far back to scan raw history entries.

    Returns:
        A list of :class:`RollupBucket` objects sorted by bucket ascending.
    """
    if granularity not in ("hourly", "daily"):
        raise ValueError(f"granularity must be 'hourly' or 'daily', got {granularity!r}")

    cutoff = _now_utc() - timedelta(hours=lookback_hours)
    entries = load_history(pipeline, history_dir)

    buckets: dict[str, dict] = {}

    for entry in entries:
        ts = _parse_ts(entry.get("checked_at", ""))
        if ts is None or ts < cutoff:
            continue

        key = _bucket_key(ts, granularity)
        if key not in buckets:
            buckets[key] = {"total": 0, "failures": 0, "latencies": []}

        buckets[key]["total"] += 1
        if not entry.get("healthy", True):
            buckets[key]["failures"] += 1

        lat = entry.get("latency_seconds")
        if lat is not None:
            buckets[key]["latencies"].append(float(lat))

    results: List[RollupBucket] = []
    for key in sorted(buckets):
        b = buckets[key]
        lats = b["latencies"]
        avg_lat = sum(lats) / len(lats) if lats else None
        results.append(
            RollupBucket(
                pipeline=pipeline,
                bucket=key,
                granularity=granularity,
                total=b["total"],
                failures=b["failures"],
                avg_latency_seconds=avg_lat,
            )
        )

    return results


def build_all_rollups(
    config: PipewatchConfig,
    history_dir: str,
    granularity: str = "hourly",
    lookback_hours: int = 48,
) -> dict[str, List[RollupBucket]]:
    """Build rollups for every pipeline defined in *config*.

    Returns:
        Mapping of pipeline name → list of :class:`RollupBucket`.
    """
    return {
        name: build_rollup(name, history_dir, granularity, lookback_hours)
        for name in config.pipelines
    }
