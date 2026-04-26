"""dispatcher.py — Route check results to the appropriate notification channels
based on pipeline tags, severity, and channel configuration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.checker import CheckResult
from pipewatch.config import PipewatchConfig
from pipewatch.labeler import LabeledResult, label_result
from pipewatch.notifier import dispatch_notifications
from pipewatch.silencer import is_silenced
from pipewatch.suppressor import is_suppressed
from pipewatch.tagger import get_tags

logger = logging.getLogger(__name__)


@dataclass
class DispatchDecision:
    """Records why a result was dispatched or skipped."""

    pipeline: str
    dispatched: bool
    reason: str
    channels: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        status = "dispatched" if self.dispatched else "skipped"
        channels = ", ".join(self.channels) if self.channels else "none"
        return f"[{self.pipeline}] {status} — {self.reason} (channels: {channels})"


def _resolve_channels(
    labeled: LabeledResult,
    config: PipewatchConfig,
    tags: List[str],
) -> List[str]:
    """Determine which notification channels apply for this result.

    Channels are enabled based on NotificationConfig; tags may restrict
    or expand routing in future extensions.
    """
    channels: List[str] = []
    notif = config.notification
    if notif is None:
        return channels
    if notif.slack_webhook:
        channels.append("slack")
    if notif.email_recipients:
        channels.append("email")
    return channels


def dispatch_result(
    result: CheckResult,
    config: PipewatchConfig,
    history_dir: Optional[str] = None,
    silence_file: Optional[str] = None,
    suppress_dir: Optional[str] = None,
) -> DispatchDecision:
    """Evaluate a single CheckResult and dispatch notifications if warranted.

    Skips dispatch when:
    - The pipeline is silenced.
    - The pipeline is suppressed.
    - The result is healthy (no violation).

    Args:
        result: The check result to evaluate.
        config: Full pipewatch configuration.
        history_dir: Optional override for history directory.
        silence_file: Optional override for silence state file.
        suppress_dir: Optional override for suppression state directory.

    Returns:
        A DispatchDecision describing what happened.
    """
    name = result.pipeline

    if is_silenced(name, silence_file=silence_file):
        return DispatchDecision(pipeline=name, dispatched=False, reason="pipeline is silenced")

    if is_suppressed(name, state_dir=suppress_dir):
        return DispatchDecision(pipeline=name, dispatched=False, reason="pipeline is suppressed")

    if result.healthy:
        return DispatchDecision(pipeline=name, dispatched=False, reason="result is healthy")

    labeled: LabeledResult = label_result(result, history_dir=history_dir)
    tags = get_tags(name)
    channels = _resolve_channels(labeled, config, tags)

    if not channels:
        return DispatchDecision(
            pipeline=name,
            dispatched=False,
            reason="no notification channels configured",
        )

    try:
        dispatch_notifications([result], config.notification)
        logger.info("Dispatched notifications for '%s' via %s", name, channels)
        return DispatchDecision(
            pipeline=name,
            dispatched=True,
            reason="threshold violation detected",
            channels=channels,
        )
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to dispatch notifications for '%s': %s", name, exc)
        return DispatchDecision(
            pipeline=name,
            dispatched=False,
            reason=f"dispatch error: {exc}",
        )


def dispatch_all(
    results: List[CheckResult],
    config: PipewatchConfig,
    history_dir: Optional[str] = None,
    silence_file: Optional[str] = None,
    suppress_dir: Optional[str] = None,
) -> List[DispatchDecision]:
    """Dispatch notifications for a list of check results.

    Args:
        results: List of CheckResult objects to evaluate.
        config: Full pipewatch configuration.
        history_dir: Optional override for history directory.
        silence_file: Optional override for silence state file.
        suppress_dir: Optional override for suppression state directory.

    Returns:
        A list of DispatchDecision objects, one per result.
    """
    return [
        dispatch_result(
            r,
            config,
            history_dir=history_dir,
            silence_file=silence_file,
            suppress_dir=suppress_dir,
        )
        for r in results
    ]
