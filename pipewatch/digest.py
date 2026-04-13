"""Daily/periodic digest report generation for pipewatch."""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetrics, compute_all_metrics
from pipewatch.config import PipewatchConfig
from pipewatch.reporter import build_report, Report


@dataclass
class DigestEntry:
    pipeline: str
    success_rate: Optional[float]
    avg_latency_seconds: Optional[float]
    avg_row_count: Optional[float]
    total_checks: int
    consecutive_failures: int


@dataclass
class Digest:
    generated_at: datetime.datetime
    overall_healthy: bool
    entries: List[DigestEntry] = field(default_factory=list)

    def to_text(self) -> str:
        lines = [
            f"Pipewatch Digest — {self.generated_at.strftime('%Y-%m-%d %H:%M UTC')}",
            f"Overall health: {'OK' if self.overall_healthy else 'DEGRADED'}",
            "-" * 48,
        ]
        for e in self.entries:
            sr = f"{e.success_rate * 100:.1f}%" if e.success_rate is not None else "n/a"
            lat = f"{e.avg_latency_seconds:.1f}s" if e.avg_latency_seconds is not None else "n/a"
            rows = f"{e.avg_row_count:.0f}" if e.avg_row_count is not None else "n/a"
            lines.append(
                f"  {e.pipeline}: success={sr}  latency={lat}  rows={rows}"
                f"  checks={e.total_checks}  consec_fail={e.consecutive_failures}"
            )
        return "\n".join(lines)


def build_digest(config: PipewatchConfig, history_dir: str = ".pipewatch_history") -> Digest:
    """Build a digest from current metrics and report data."""
    all_metrics: dict[str, PipelineMetrics] = compute_all_metrics(config, history_dir)
    report: Report = build_report(config, history_dir)

    entries: List[DigestEntry] = []
    for pipeline_name, m in all_metrics.items():
        entries.append(
            DigestEntry(
                pipeline=pipeline_name,
                success_rate=m.success_rate,
                avg_latency_seconds=m.avg_latency_seconds,
                avg_row_count=m.avg_row_count,
                total_checks=m.total_checks,
                consecutive_failures=m.consecutive_failures,
            )
        )

    return Digest(
        generated_at=datetime.datetime.utcnow(),
        overall_healthy=report.overall_healthy,
        entries=entries,
    )
