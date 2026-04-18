"""Pipeline alert quota tracking — limits how many alerts fire per pipeline per day."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

_DEFAULT_DIR = Path(".pipewatch/quotas")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _quota_path(pipeline: str, base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{pipeline}.json"


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def _save_state(path: Path, state: dict) -> None:
    with path.open("w") as f:
        json.dump(state, f)


@dataclass
class QuotaResult:
    pipeline: str
    date: str
    count: int
    limit: int
    exhausted: bool

    def __str__(self) -> str:
        status = "EXHAUSTED" if self.exhausted else "ok"
        return f"{self.pipeline} [{self.date}]: {self.count}/{self.limit} alerts ({status})"


def record_alert(pipeline: str, limit: int, base_dir: Path = _DEFAULT_DIR) -> QuotaResult:
    """Record one alert firing for pipeline. Returns QuotaResult after increment."""
    path = _quota_path(pipeline, base_dir)
    state = _load_state(path)
    today = _now_utc().strftime("%Y-%m-%d")
    if state.get("date") != today:
        state = {"date": today, "count": 0}
    state["count"] += 1
    _save_state(path, state)
    count = state["count"]
    return QuotaResult(pipeline=pipeline, date=today, count=count, limit=limit, exhausted=count > limit)


def get_quota(pipeline: str, limit: int, base_dir: Path = _DEFAULT_DIR) -> QuotaResult:
    """Return current quota status without incrementing."""
    path = _quota_path(pipeline, base_dir)
    state = _load_state(path)
    today = _now_utc().strftime("%Y-%m-%d")
    if state.get("date") != today:
        return QuotaResult(pipeline=pipeline, date=today, count=0, limit=limit, exhausted=False)
    count = state["count"]
    return QuotaResult(pipeline=pipeline, date=today, count=count, limit=limit, exhausted=count > limit)


def is_quota_exhausted(pipeline: str, limit: int, base_dir: Path = _DEFAULT_DIR) -> bool:
    return get_quota(pipeline, limit, base_dir).exhausted
