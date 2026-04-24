"""soaker.py – absorb transient failures within a soak window.

A pipeline is considered 'soaking' when it has recently recovered from
failures but has not yet accumulated enough consecutive healthy runs to
be declared stable.  During the soak window, alerts are suppressed so
that flapping pipelines do not generate noise.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

HISTORY_DIR = Path(".pipewatch/history")
_SOAK_DIR = Path(".pipewatch/soak")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SoakResult:
    pipeline: str
    soaking: bool
    healthy_streak: int
    required: int
    started_at: Optional[str]

    def __str__(self) -> str:
        if self.soaking:
            return (
                f"{self.pipeline}: SOAKING "
                f"({self.healthy_streak}/{self.required} healthy runs)"
            )
        return f"{self.pipeline}: STABLE"


def _soak_path(pipeline: str, soak_dir: Path = _SOAK_DIR) -> Path:
    return soak_dir / f"{pipeline}.json"


def _load_soak(pipeline: str, soak_dir: Path = _SOAK_DIR) -> dict:
    p = _soak_path(pipeline, soak_dir)
    if p.exists():
        return json.loads(p.read_text())
    return {}


def _save_soak(pipeline: str, state: dict, soak_dir: Path = _SOAK_DIR) -> None:
    soak_dir.mkdir(parents=True, exist_ok=True)
    _soak_path(pipeline, soak_dir).write_text(json.dumps(state))


def _load_history(pipeline: str, history_dir: Path = HISTORY_DIR) -> List[dict]:
    p = history_dir / f"{pipeline}.jsonl"
    if not p.exists():
        return []
    lines = [ln for ln in p.read_text().splitlines() if ln.strip()]
    return [json.loads(ln) for ln in lines]


def evaluate_soak(
    pipeline: str,
    required: int = 3,
    history_dir: Path = HISTORY_DIR,
    soak_dir: Path = _SOAK_DIR,
) -> SoakResult:
    """Evaluate whether *pipeline* is still within its soak window."""
    history = _load_history(pipeline, history_dir)
    state = _load_soak(pipeline, soak_dir)

    # Count trailing consecutive healthy runs
    streak = 0
    for entry in reversed(history):
        if entry.get("healthy", False):
            streak += 1
        else:
            break

    # Determine if we just transitioned from failing to healthy
    previously_failing = state.get("previously_failing", False)
    started_at = state.get("started_at")

    if streak == 0:
        # Currently failing – record so we start soak on next recovery
        _save_soak(pipeline, {"previously_failing": True, "started_at": None}, soak_dir)
        return SoakResult(pipeline, False, 0, required, None)

    if previously_failing and streak < required:
        if not started_at:
            started_at = _now_utc().isoformat()
        _save_soak(
            pipeline,
            {"previously_failing": True, "started_at": started_at},
            soak_dir,
        )
        return SoakResult(pipeline, True, streak, required, started_at)

    # Stable – clear soak state
    _save_soak(pipeline, {"previously_failing": False, "started_at": None}, soak_dir)
    return SoakResult(pipeline, False, streak, required, None)


def evaluate_all_soaks(
    pipelines: List[str],
    required: int = 3,
    history_dir: Path = HISTORY_DIR,
    soak_dir: Path = _SOAK_DIR,
) -> List[SoakResult]:
    return [
        evaluate_soak(p, required=required, history_dir=history_dir, soak_dir=soak_dir)
        for p in pipelines
    ]
