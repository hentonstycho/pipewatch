"""Watchdog: detect pipelines that have stopped reporting results."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from pipewatch.history import load_history
from pipewatch.config import PipewatchConfig


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class StaleResult:
    pipeline: str
    last_checked: Optional[datetime]
    age_seconds: Optional[float]
    threshold_seconds: float
    is_stale: bool


def check_staleness(
    pipeline_name: str,
    history_dir: str,
    threshold_seconds: float,
    now: Optional[datetime] = None,
) -> StaleResult:
    """Return a StaleResult for a single pipeline."""
    if now is None:
        now = _now_utc()

    records = load_history(pipeline_name, history_dir=history_dir)
    if not records:
        return StaleResult(
            pipeline=pipeline_name,
            last_checked=None,
            age_seconds=None,
            threshold_seconds=threshold_seconds,
            is_stale=True,
        )

    last = records[-1]
    last_checked: datetime = last["checked_at"]  # type: ignore[assignment]
    if isinstance(last_checked, str):
        last_checked = datetime.fromisoformat(last_checked)
    if last_checked.tzinfo is None:
        last_checked = last_checked.replace(tzinfo=timezone.utc)

    age = (now - last_checked).total_seconds()
    return StaleResult(
        pipeline=pipeline_name,
        last_checked=last_checked,
        age_seconds=age,
        threshold_seconds=threshold_seconds,
        is_stale=age > threshold_seconds,
    )


def check_all_staleness(
    config: PipewatchConfig,
    history_dir: str,
    default_threshold_seconds: float = 3600.0,
    now: Optional[datetime] = None,
) -> List[StaleResult]:
    """Check staleness for every pipeline in the config."""
    results = []
    for pipeline in config.pipelines:
        threshold = getattr(pipeline, "stale_after_seconds", None) or default_threshold_seconds
        results.append(
            check_staleness(
                pipeline_name=pipeline.name,
                history_dir=history_dir,
                threshold_seconds=float(threshold),
                now=now,
            )
        )
    return results
