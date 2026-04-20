"""windower.py – sliding-window failure analysis for pipelines.

Computes failure counts and rates over configurable rolling time windows
(e.g. last 1 h, 6 h, 24 h) from the history store.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.config import PipewatchConfig
from pipewatch.history import _history_path


@dataclass
class WindowResult:
    pipeline: str
    window_hours: int
    total: int
    failures: int

    @property
    def failure_rate(self) -> float:
        return self.failures / self.total if self.total else 0.0

    @property
    def healthy(self) -> bool:
        return self.failures == 0

    def __str__(self) -> str:
        status = "OK" if self.healthy else "DEGRADED"
        return (
            f"{self.pipeline} [{self.window_hours}h] "
            f"{self.failures}/{self.total} failures "
            f"({self.failure_rate:.1%}) – {status}"
        )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def analyse_window(
    pipeline: str,
    window_hours: int = 24,
    history_dir: Optional[Path] = None,
) -> WindowResult:
    """Return failure stats for *pipeline* within the last *window_hours*."""
    path = _history_path(pipeline, base_dir=history_dir)
    cutoff = _now_utc() - timedelta(hours=window_hours)
    total = 0
    failures = 0
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                ts = _parse_ts(entry.get("checked_at", ""))
                if ts >= cutoff:
                    total += 1
                    if not entry.get("healthy", True):
                        failures += 1
            except (json.JSONDecodeError, ValueError):
                continue
    return WindowResult(
        pipeline=pipeline,
        window_hours=window_hours,
        total=total,
        failures=failures,
    )


def analyse_all_windows(
    config: PipewatchConfig,
    window_hours: int = 24,
    history_dir: Optional[Path] = None,
) -> List[WindowResult]:
    """Analyse all pipelines in *config* over *window_hours*."""
    return [
        analyse_window(p.name, window_hours=window_hours, history_dir=history_dir)
        for p in config.pipelines
    ]
