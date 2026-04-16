"""Failure heatmap: bucket pipeline check results by hour-of-day to surface
recurring failure windows."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List

from pipewatch.config import PipewatchConfig
from pipewatch.history import load_history


@dataclass
class HeatmapRow:
    pipeline: str
    # index 0-23 = hour of day (UTC); value = failure count
    buckets: List[int] = field(default_factory=lambda: [0] * 24)

    @property
    def peak_hour(self) -> int:
        """Hour (UTC) with the most failures."""
        return int(max(range(24), key=lambda h: self.buckets[h]))

    @property
    def total_failures(self) -> int:
        return sum(self.buckets)


def build_heatmap(config: PipewatchConfig, history_dir: str = ".pipewatch/history") -> List[HeatmapRow]:
    """Return one HeatmapRow per pipeline in *config*."""
    rows: List[HeatmapRow] = []
    for pipeline in config.pipelines:
        buckets: List[int] = [0] * 24
        for entry in load_history(pipeline.name, history_dir=history_dir):
            if entry.get("healthy", True):
                continue
            ts_raw = entry.get("checked_at") or entry.get("timestamp")
            if not ts_raw:
                continue
            try:
                dt = datetime.fromisoformat(ts_raw.rstrip("Z")).replace(tzinfo=timezone.utc)
                buckets[dt.hour] += 1
            except (ValueError, AttributeError):
                continue
        rows.append(HeatmapRow(pipeline=pipeline.name, buckets=buckets))
    return rows


def format_heatmap(rows: List[HeatmapRow], *, bar_char: str = "█", max_width: int = 20) -> str:
    """Render heatmap rows as a simple ASCII grid."""
    if not rows:
        return "No data."
    lines: List[str] = []
    header = "Pipeline" + " " * 22 + "".join(f"{h:02d}" for h in range(0, 24, 2))
    lines.append(header)
    lines.append("-" * len(header))
    for row in rows:
        name = row.pipeline[:28].ljust(30)
        peak = max(row.buckets) or 1
        bar = ""
        for h in range(24):
            count = row.buckets[h]
            intensity = int(count / peak * 3)
            bar += ["·", "▒", "▓", bar_char][intensity]
        lines.append(f"{name}{bar}  (peak={row.peak_hour:02d}h, total={row.total_failures})")
    return "\n".join(lines)
