"""Escalation logic: upgrade alert severity after N consecutive failures."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.checker import CheckResult
from pipewatch.history import consecutive_failures


@dataclass
class EscalationResult:
    pipeline: str
    consecutive: int
    level: str  # "ok" | "warn" | "critical"
    message: str

    def __str__(self) -> str:
        return f"[{self.level.upper()}] {self.pipeline}: {self.message}"


def _level(consecutive: int, warn_after: int, critical_after: int) -> str:
    if consecutive >= critical_after:
        return "critical"
    if consecutive >= warn_after:
        return "warn"
    return "ok"


def escalate(
    result: CheckResult,
    history_dir: str,
    warn_after: int = 2,
    critical_after: int = 5,
) -> EscalationResult:
    """Return an EscalationResult for a single pipeline check result."""
    if result.healthy:
        return EscalationResult(
            pipeline=result.pipeline,
            consecutive=0,
            level="ok",
            message="pipeline is healthy",
        )

    n = consecutive_failures(result.pipeline, history_dir)
    level = _level(n, warn_after, critical_after)
    msg = f"{n} consecutive failure(s) — threshold violations: {', '.join(result.violations)}"
    return EscalationResult(pipeline=result.pipeline, consecutive=n, level=level, message=msg)


def escalate_all(
    results: list[CheckResult],
    history_dir: str,
    warn_after: int = 2,
    critical_after: int = 5,
) -> list[EscalationResult]:
    return [
        escalate(r, history_dir, warn_after=warn_after, critical_after=critical_after)
        for r in results
    ]
