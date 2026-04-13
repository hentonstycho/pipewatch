"""Alerting rules: suppress noise by respecting consecutive-failure thresholds
and cooldown windows before re-notifying on a known-bad pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional
import json

from pipewatch.checker import CheckResult
from pipewatch.history import consecutive_failures

_COOLDOWN_PATH = Path(".pipewatch_cooldowns.json")


@dataclass
class AlertPolicy:
    """Controls when a notification is actually dispatched."""
    min_consecutive_failures: int = 1
    cooldown_minutes: int = 0  # 0 means always alert


@dataclass
class AlertState:
    pipeline: str
    last_notified: Optional[datetime] = field(default=None)


def _load_cooldowns(path: Path = _COOLDOWN_PATH) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_cooldowns(data: Dict[str, str], path: Path = _COOLDOWN_PATH) -> None:
    path.write_text(json.dumps(data, indent=2))


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def should_alert(
    result: CheckResult,
    policy: AlertPolicy,
    history_dir: Optional[Path] = None,
    cooldown_path: Path = _COOLDOWN_PATH,
) -> bool:
    """Return True if a notification should be dispatched for *result*."""
    if result.healthy:
        return False

    consec = consecutive_failures(result.pipeline, history_dir=history_dir)
    if consec < policy.min_consecutive_failures:
        return False

    if policy.cooldown_minutes <= 0:
        return True

    cooldowns = _load_cooldowns(cooldown_path)
    last_str = cooldowns.get(result.pipeline)
    if last_str is not None:
        last_dt = datetime.fromisoformat(last_str)
        if _now_utc() - last_dt < timedelta(minutes=policy.cooldown_minutes):
            return False

    return True


def record_alert(pipeline: str, cooldown_path: Path = _COOLDOWN_PATH) -> None:
    """Persist the current UTC timestamp as the last-notified time for *pipeline*."""
    cooldowns = _load_cooldowns(cooldown_path)
    cooldowns[pipeline] = _now_utc().isoformat()
    _save_cooldowns(cooldowns, cooldown_path)
