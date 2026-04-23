"""Inspector: surface per-pipeline diagnostic details from recent history."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.config import PipewatchConfig
from pipewatch.history import load_history


@dataclass
class InspectionResult:
    pipeline: str
    total_runs: int
    healthy_runs: int
    failed_runs: int
    last_status: Optional[str]  # "ok" | "fail" | None
    last_checked: Optional[str]
    failure_messages: List[str] = field(default_factory=list)

    @property
    def success_rate(self) -> Optional[float]:
        if self.total_runs == 0:
            return None
        return round(self.healthy_runs / self.total_runs * 100, 1)

    def __str__(self) -> str:
        sr = f"{self.success_rate}%" if self.success_rate is not None else "n/a"
        return (
            f"[{self.pipeline}] runs={self.total_runs} "
            f"healthy={self.healthy_runs} failed={self.failed_runs} "
            f"success_rate={sr} last={self.last_status or 'n/a'}"
        )


def inspect_pipeline(
    pipeline_name: str,
    cfg: PipewatchConfig,
    *,
    history_dir: str = ".pipewatch/history",
    limit: int = 50,
) -> Optional[InspectionResult]:
    """Return an InspectionResult for *pipeline_name*, or None if not configured."""
    if not any(p.name == pipeline_name for p in cfg.pipelines):
        return None

    entries = load_history(pipeline_name, history_dir=history_dir)
    entries = entries[-limit:] if len(entries) > limit else entries

    total = len(entries)
    healthy = sum(1 for e in entries if e.get("healthy", False))
    failed = total - healthy
    last_status = None
    last_checked = None
    failure_messages: List[str] = []

    if entries:
        last = entries[-1]
        last_status = "ok" if last.get("healthy", False) else "fail"
        last_checked = last.get("checked_at")

    for e in entries:
        if not e.get("healthy", True):
            msg = e.get("message") or e.get("reason")
            if msg:
                failure_messages.append(msg)

    return InspectionResult(
        pipeline=pipeline_name,
        total_runs=total,
        healthy_runs=healthy,
        failed_runs=failed,
        last_status=last_status,
        last_checked=last_checked,
        failure_messages=failure_messages,
    )


def inspect_all(
    cfg: PipewatchConfig,
    *,
    history_dir: str = ".pipewatch/history",
    limit: int = 50,
) -> List[InspectionResult]:
    """Return InspectionResult for every configured pipeline."""
    return [
        r
        for p in cfg.pipelines
        if (r := inspect_pipeline(p.name, cfg, history_dir=history_dir, limit=limit))
        is not None
    ]
