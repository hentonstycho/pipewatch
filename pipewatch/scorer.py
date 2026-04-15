"""Pipeline health scorer — produces a 0-100 score per pipeline based on
recent check history, error rate, latency ratio, and consecutive failures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.config import PipelineConfig
from pipewatch.history import load_history, consecutive_failures
from pipewatch.checker import CheckResult


@dataclass
class PipelineScore:
    pipeline: str
    score: float          # 0 (worst) – 100 (best)
    grade: str            # A / B / C / D / F
    reasons: List[str]


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def _penalise_consecutive(failures: int) -> float:
    """Return a penalty (0-40) that grows with consecutive failure count."""
    return min(40.0, failures * 10.0)


def score_pipeline(
    cfg: PipelineConfig,
    history_dir: str = ".pipewatch/history",
    window: int = 20,
) -> PipelineScore:
    """Compute a health score for *cfg* using the last *window* check results."""
    results: List[CheckResult] = load_history(cfg.name, history_dir)[-window:]
    reasons: List[str] = []

    if not results:
        return PipelineScore(
            pipeline=cfg.name,
            score=50.0,
            grade="C",
            reasons=["no history available — defaulting to neutral score"],
        )

    total = len(results)
    failures = sum(1 for r in results if not r.healthy)
    failure_rate = failures / total

    score = 100.0

    # Deduct for overall failure rate (up to 50 pts)
    rate_penalty = failure_rate * 50.0
    if rate_penalty > 0:
        score -= rate_penalty
        reasons.append(f"failure rate {failure_rate:.0%} over last {total} checks")

    # Deduct for consecutive failures (up to 40 pts)
    consec = consecutive_failures(cfg.name, history_dir)
    consec_penalty = _penalise_consecutive(consec)
    if consec_penalty > 0:
        score -= consec_penalty
        reasons.append(f"{consec} consecutive failure(s)")

    # Small bonus when all recent checks are healthy
    if failures == 0:
        reasons.append("all recent checks healthy")

    score = max(0.0, min(100.0, score))
    return PipelineScore(
        pipeline=cfg.name,
        score=round(score, 1),
        grade=_grade(score),
        reasons=reasons,
    )


def score_all(
    pipelines: List[PipelineConfig],
    history_dir: str = ".pipewatch/history",
    window: int = 20,
) -> List[PipelineScore]:
    """Return scores for every pipeline, sorted worst-first."""
    scores = [score_pipeline(p, history_dir, window) for p in pipelines]
    return sorted(scores, key=lambda s: s.score)
