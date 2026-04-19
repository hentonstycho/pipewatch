"""summarizer.py – produce a human-readable health summary per pipeline.

Builds a compact text block suitable for CLI output or notification bodies.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.checker import CheckResult
from pipewatch.history import load_history, consecutive_failures
from pipewatch.metrics import PipelineMetrics, compute_metrics
from pipewatch.config import PipelineConfig


@dataclass
class PipelineSummaryLine:
    pipeline: str
    status: str          # "OK" | "FAIL" | "NO DATA"
    consecutive_fails: int
    avg_row_count: Optional[float]
    avg_error_rate: Optional[float]
    avg_latency_seconds: Optional[float]
    last_message: Optional[str]


def _status_icon(status: str) -> str:
    return {"OK": "✅", "FAIL": "❌", "NO DATA": "⚪"}.get(status, "?")


def summarise_pipeline(
    pipeline: PipelineConfig,
    *,
    history_dir: str = ".pipewatch/history",
) -> PipelineSummaryLine:
    """Return a summary line for a single pipeline."""
    history = load_history(pipeline.name, history_dir=history_dir)
    if not history:
        return PipelineSummaryLine(
            pipeline=pipeline.name,
            status="NO DATA",
            consecutive_fails=0,
            avg_row_count=None,
            avg_error_rate=None,
            avg_latency_seconds=None,
            last_message=None,
        )

    last: CheckResult = history[-1]
    status = "OK" if last.healthy else "FAIL"
    consec = consecutive_failures(pipeline.name, history_dir=history_dir)
    metrics: PipelineMetrics = compute_metrics(pipeline.name, history_dir=history_dir)

    return PipelineSummaryLine(
        pipeline=pipeline.name,
        status=status,
        consecutive_fails=consec,
        avg_row_count=metrics.avg_row_count,
        avg_error_rate=metrics.avg_error_rate,
        avg_latency_seconds=metrics.avg_latency_seconds,
        last_message=last.message,
    )


def format_summary_line(line: PipelineSummaryLine) -> str:
    """Render a PipelineSummaryLine as a single text line."""
    icon = _status_icon(line.status)
    parts = [f"{icon} {line.pipeline} [{line.status}]"]
    if line.consecutive_fails:
        parts.append(f"consec_fails={line.consecutive_fails}")
    if line.avg_row_count is not None:
        parts.append(f"avg_rows={line.avg_row_count:.1f}")
    if line.avg_error_rate is not None:
        parts.append(f"avg_err={line.avg_error_rate:.3f}")
    if line.avg_latency_seconds is not None:
        parts.append(f"avg_latency={line.avg_latency_seconds:.1f}s")
    if line.last_message:
        parts.append(f"msg={line.last_message!r}")
    return "  ".join(parts)


def summarise_all(
    pipelines: List[PipelineConfig],
    *,
    history_dir: str = ".pipewatch/history",
) -> List[PipelineSummaryLine]:
    """Return summary lines for every pipeline."""
    return [summarise_pipeline(p, history_dir=history_dir) for p in pipelines]


def format_report(lines: List[PipelineSummaryLine]) -> str:
    """Format a list of summary lines into a full multi-line report string.

    Includes a header, one line per pipeline, and a footer with counts of
    healthy, failing, and no-data pipelines.
    """
    formatted = [format_summary_line(line) for line in lines]
    ok = sum(1 for l in lines if l.status == "OK")
    fail = sum(1 for l in lines if l.status == "FAIL")
    no_data = sum(1 for l in lines if l.status == "NO DATA")
    footer = f"Pipelines: {len(lines)} total  ✅ {ok}  ❌ {fail}  ⚪ {no_data}"
    return "\n".join(formatted + ["", footer])
