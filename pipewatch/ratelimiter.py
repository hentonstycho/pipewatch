"""Rate limiter: suppress notifications when a pipeline fires too frequently."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

_DEFAULT_DIR = Path(".pipewatch/ratelimits")


@dataclass
class RateLimitState:
    pipeline: str
    timestamps: List[float] = field(default_factory=list)


def _state_path(pipeline: str, base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{pipeline}.json"


def _load_state(pipeline: str, base_dir: Path) -> RateLimitState:
    p = _state_path(pipeline, base_dir)
    if not p.exists():
        return RateLimitState(pipeline=pipeline)
    data = json.loads(p.read_text())
    return RateLimitState(pipeline=pipeline, timestamps=data.get("timestamps", []))


def _save_state(state: RateLimitState, base_dir: Path) -> None:
    p = _state_path(state.pipeline, base_dir)
    p.write_text(json.dumps({"timestamps": state.timestamps}))


def _now() -> float:
    return time.time()


def is_rate_limited(
    pipeline: str,
    max_alerts: int,
    window_seconds: int,
    base_dir: Path = _DEFAULT_DIR,
) -> bool:
    """Return True if the pipeline has fired >= max_alerts within window_seconds."""
    state = _load_state(pipeline, base_dir)
    cutoff = _now() - window_seconds
    recent = [t for t in state.timestamps if t >= cutoff]
    return len(recent) >= max_alerts


def record_alert(
    pipeline: str,
    base_dir: Path = _DEFAULT_DIR,
) -> None:
    """Record that an alert was fired for this pipeline right now."""
    state = _load_state(pipeline, base_dir)
    state.timestamps.append(_now())
    _save_state(state, base_dir)


def clear_state(
    pipeline: str,
    base_dir: Path = _DEFAULT_DIR,
) -> None:
    """Remove all recorded alert timestamps for a pipeline."""
    p = _state_path(pipeline, base_dir)
    if p.exists():
        p.unlink()
