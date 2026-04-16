"""pager.py – on-call paging logic: escalate alerts after N consecutive failures."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.history import load_history, consecutive_failures
from pipewatch.config import PipelineConfig


@dataclass
class PageEvent:
    pipeline: str
    consecutive: int
    message: str
    should_page: bool


def _build_message(pipeline: str, consecutive: int) -> str:
    return (
        f"[PIPEWATCH PAGE] Pipeline '{pipeline}' has failed "
        f"{consecutive} consecutive time(s) and requires immediate attention."
    )


def evaluate_page(
    pipeline: PipelineConfig,
    history_dir: str = ".pipewatch/history",
    escalate_after: int = 3,
) -> PageEvent:
    """Return a PageEvent indicating whether this pipeline should trigger a page."""
    history = load_history(pipeline.name, history_dir=history_dir)
    consec = consecutive_failures(history)
    should = consec >= escalate_after
    return PageEvent(
        pipeline=pipeline.name,
        consecutive=consec,
        message=_build_message(pipeline.name, consec) if should else "",
        should_page=should,
    )


def evaluate_all_pages(
    pipelines: list[PipelineConfig],
    history_dir: str = ".pipewatch/history",
    escalate_after: int = 3,
) -> list[PageEvent]:
    """Evaluate paging for every pipeline; return only those that should page."""
    events = [
        evaluate_page(p, history_dir=history_dir, escalate_after=escalate_after)
        for p in pipelines
    ]
    return [e for e in events if e.should_page]
