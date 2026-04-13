"""Simple cron-style scheduler for running pipewatch checks periodically."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Callable, Optional

log = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def parse_interval(interval_str: str) -> int:
    """Parse a human-readable interval string into seconds.

    Supported suffixes: s (seconds), m (minutes), h (hours).
    Plain integers are treated as seconds.

    Examples::

        parse_interval('30s')  -> 30
        parse_interval('5m')   -> 300
        parse_interval('2h')   -> 7200
        parse_interval('120')  -> 120
    """
    interval_str = interval_str.strip()
    if interval_str.endswith('s'):
        return int(interval_str[:-1])
    if interval_str.endswith('m'):
        return int(interval_str[:-1]) * 60
    if interval_str.endswith('h'):
        return int(interval_str[:-1]) * 3600
    return int(interval_str)


def run_scheduler(
    interval_str: str,
    task: Callable[[], None],
    *,
    max_iterations: Optional[int] = None,
    stop_on_error: bool = False,
) -> None:
    """Block and repeatedly call *task* every *interval_str* seconds.

    Args:
        interval_str: Human-readable interval, e.g. ``'5m'``.
        task: Zero-argument callable executed on each tick.
        max_iterations: If set, stop after this many iterations (useful for
            testing).
        stop_on_error: If ``True``, re-raise any exception from *task*.
    """
    interval_secs = parse_interval(interval_str)
    log.info("Scheduler starting — interval %s s", interval_secs)
    iteration = 0

    while True:
        start = _now_utc()
        log.debug("Scheduler tick at %s", start.isoformat())
        try:
            task()
        except Exception as exc:  # noqa: BLE001
            log.error("Task raised an exception: %s", exc, exc_info=True)
            if stop_on_error:
                raise

        iteration += 1
        if max_iterations is not None and iteration >= max_iterations:
            log.info("Scheduler reached max_iterations=%d, stopping.", max_iterations)
            break

        elapsed = (_now_utc() - start).total_seconds()
        sleep_for = max(0.0, interval_secs - elapsed)
        log.debug("Sleeping %.1f s until next tick", sleep_for)
        time.sleep(sleep_for)
