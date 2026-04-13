"""Generate summary reports from pipeline check history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.checker import CheckResult
from pipewatch.history import load_history


@dataclass
class PipelineSummary:
    pipeline_name: str
    total_checks: int
    failures: int
    success_rate: float
    last_checked: Optional[datetime]
    last_status: Optional[str]


@dataclass
class Report:
    generated_at: datetime
    summaries: List[PipelineSummary]

    @property
    def overall_health(self) -> str:
        if not self.summaries:
            return "unknown"
        failing = [s for s in self.summaries if s.last_status == "FAIL"]
        return "degraded" if failing else "healthy"


def _summarise(name: str, results: List[CheckResult]) -> PipelineSummary:
    total = len(results)
    failures = sum(1 for r in results if not r.healthy)
    success_rate = round((total - failures) / total * 100, 1) if total else 0.0
    last = results[-1] if results else None
    last_checked = (
        datetime.fromisoformat(last.checked_at).replace(tzinfo=timezone.utc)
        if last and last.checked_at
        else None
    )
    last_status = ("OK" if last.healthy else "FAIL") if last else None
    return PipelineSummary(
        pipeline_name=name,
        total_checks=total,
        failures=failures,
        success_rate=success_rate,
        last_checked=last_checked,
        last_status=last_status,
    )


def build_report(pipeline_names: List[str], history_dir: str = ".pipewatch") -> Report:
    summaries = []
    for name in pipeline_names:
        results = load_history(name, history_dir=history_dir)
        summaries.append(_summarise(name, results))
    return Report(
        generated_at=datetime.now(tz=timezone.utc),
        summaries=summaries,
    )


def format_report(report: Report) -> str:
    lines = [
        f"PipeWatch Report — {report.generated_at.strftime('%Y-%m-%d %H:%M UTC')}",
        f"Overall health: {report.overall_health.upper()}",
        "-" * 50,
    ]
    for s in report.summaries:
        last_str = s.last_checked.strftime("%H:%M UTC") if s.last_checked else "never"
        lines.append(
            f"  {s.pipeline_name}: {s.last_status or 'N/A'} | "
            f"{s.success_rate}% success ({s.total_checks} checks, {s.failures} failures) | "
            f"last: {last_str}"
        )
    return "\n".join(lines)
