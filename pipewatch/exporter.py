"""Prometheus-compatible metrics exporter for pipewatch."""
from __future__ import annotations

import time
from typing import List

from pipewatch.metrics import PipelineMetrics

_METRIC_PREFIX = "pipewatch"


def _gauge(name: str, help_text: str, labels: dict[str, str], value: float) -> str:
    """Render a single Prometheus gauge line."""
    label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
    full_name = f"{_METRIC_PREFIX}_{name}"
    lines = [
        f"# HELP {full_name} {help_text}",
        f"# TYPE {full_name} gauge",
        f"{full_name}{{{label_str}}} {value}",
    ]
    return "\n".join(lines)


def render_metrics(all_metrics: List[PipelineMetrics], timestamp: float | None = None) -> str:
    """Render all pipeline metrics in Prometheus exposition format."""
    ts = timestamp if timestamp is not None else time.time()
    blocks: list[str] = []

    for m in all_metrics:
        lbl = {"pipeline": m.pipeline_name}
        healthy_val = 1.0 if m.success_rate is not None and m.success_rate >= 1.0 else 0.0

        blocks.append(_gauge("success_rate", "Fraction of successful checks.", lbl,
                             m.success_rate if m.success_rate is not None else float("nan")))
        blocks.append(_gauge("avg_latency_seconds", "Average pipeline latency in seconds.", lbl,
                             m.avg_latency if m.avg_latency is not None else float("nan")))
        blocks.append(_gauge("avg_error_rate", "Average error rate across checks.", lbl,
                             m.avg_error_rate if m.avg_error_rate is not None else float("nan")))
        blocks.append(_gauge("total_checks", "Total number of recorded checks.", lbl,
                             float(m.total_checks)))
        blocks.append(_gauge("consecutive_failures", "Current consecutive failure count.", lbl,
                             float(m.consecutive_failures)))

    blocks.append(f"# EOF")
    return "\n\n".join(blocks)
