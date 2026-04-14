"""ranker.py – rank pipelines by health score based on recent history.

A pipeline's score is computed from its failure rate, average latency
(relative to threshold), and consecutive failures.  Lower score = healthier.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.config import PipewatchConfig
from pipewatch.metrics import PipelineMetrics, compute_all_metrics
from pipewatch.history import consecutive_failures


@dataclass
class PipelineRank:
    pipeline: str
    score: float  # 0.0 = perfect, higher = worse
    failure_rate: float
    consecutive_failures: int
    avg_latency: Optional[float]
    latency_threshold: Optional[float]


def _latency_ratio(metrics: PipelineMetrics, threshold: Optional[float]) -> float:
    """Return avg_latency / threshold, or 0 if either is unavailable."""
    if metrics.avg_latency is None or not threshold:
        return 0.0
    return metrics.avg_latency / threshold


def _score(metrics: PipelineMetrics, consec: int, latency_threshold: Optional[float]) -> float:
    """Compute a composite health score (higher = worse).

    Weights:
      - failure_rate     : 50 %
      - latency ratio    : 30 %
      - consecutive fails: 20 % (capped at 10 for normalisation)
    """
    failure_component = metrics.failure_rate * 0.5
    latency_component = min(_latency_ratio(metrics, latency_threshold), 2.0) * 0.3
    consec_component = (min(consec, 10) / 10.0) * 0.2
    return round(failure_component + latency_component + consec_component, 4)


def rank_pipelines(
    config: PipewatchConfig,
    history_dir: str = ".pipewatch/history",
) -> List[PipelineRank]:
    """Return pipelines sorted from worst (highest score) to best."""
    all_metrics = compute_all_metrics(config, history_dir=history_dir)
    ranks: List[PipelineRank] = []

    for pipeline_cfg in config.pipelines:
        name = pipeline_cfg.name
        metrics = all_metrics.get(name, PipelineMetrics(pipeline=name))
        consec = consecutive_failures(name, history_dir=history_dir)
        lat_threshold = (
            pipeline_cfg.thresholds.max_latency_seconds
            if pipeline_cfg.thresholds
            else None
        )
        score = _score(metrics, consec, lat_threshold)
        ranks.append(
            PipelineRank(
                pipeline=name,
                score=score,
                failure_rate=metrics.failure_rate,
                consecutive_failures=consec,
                avg_latency=metrics.avg_latency,
                latency_threshold=lat_threshold,
            )
        )

    ranks.sort(key=lambda r: r.score, reverse=True)
    return ranks
