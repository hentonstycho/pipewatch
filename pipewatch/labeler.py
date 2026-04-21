"""labeler.py — Assign severity labels (critical/warning/info) to check results
based on configurable thresholds and consecutive failure counts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.checker import CheckResult
from pipewatch.history import consecutive_failures


SEVERITY_CRITICAL = "critical"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"
SEVERITY_OK = "ok"


@dataclass
class LabeledResult:
    result: CheckResult
    severity: str
    reason: Optional[str] = None

    @property
    def is_actionable(self) -> bool:
        """Return True if this result requires attention (non-OK severity)."""
        return self.severity != SEVERITY_OK


def _consecutive_failures_severity(failures: int, warning_after: int, critical_after: int) -> str:
    if failures >= critical_after:
        return SEVERITY_CRITICAL
    if failures >= warning_after:
        return SEVERITY_WARNING
    return SEVERITY_OK


def label_result(
    result: CheckResult,
    history_dir: str,
    warning_after: int = 2,
    critical_after: int = 5,
) -> LabeledResult:
    """Assign a severity label to a CheckResult.

    - ok results are always labelled SEVERITY_OK.
    - failing results are escalated based on consecutive failure count.
    """
    if result.healthy:
        return LabeledResult(result=result, severity=SEVERITY_OK)

    failures = consecutive_failures(result.pipeline, history_dir)
    severity = _consecutive_failures_severity(failures, warning_after, critical_after)

    reason = (
        f"{failures} consecutive failure(s) for '{result.pipeline}' "
        f"[{', '.join(result.violations)}]"
    )
    return LabeledResult(result=result, severity=severity, reason=reason)


def label_all(
    results: list[CheckResult],
    history_dir: str,
    warning_after: int = 2,
    critical_after: int = 5,
) -> list[LabeledResult]:
    """Label every result in a list."""
    return [
        label_result(r, history_dir, warning_after, critical_after)
        for r in results
    ]


def filter_by_severity(
    labeled_results: list[LabeledResult],
    *severities: str,
) -> list[LabeledResult]:
    """Return only the labeled results whose severity is in *severities*.

    Example::

        critical_only = filter_by_severity(results, SEVERITY_CRITICAL)
        alerts = filter_by_severity(results, SEVERITY_WARNING, SEVERITY_CRITICAL)
    """
    severity_set = set(severities)
    return [lr for lr in labeled_results if lr.severity in severity_set]
