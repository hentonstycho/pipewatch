"""Baseline management for pipeline metrics.

Allows capturing a named baseline snapshot of computed metrics and
comparing future metrics against it to detect regressions.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipewatch.metrics import PipelineMetrics

_BASELINES_DIR = os.environ.get("PIPEWATCH_BASELINES_DIR", ".pipewatch/baselines")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _baseline_path(name: str, baselines_dir: str = _BASELINES_DIR) -> Path:
    return Path(baselines_dir) / f"{name}.json"


def save_baseline(
    name: str,
    metrics: dict[str, PipelineMetrics],
    baselines_dir: str = _BASELINES_DIR,
) -> Path:
    """Persist *metrics* as a named baseline and return the file path."""
    path = _baseline_path(name, baselines_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": name,
        "captured_at": _now_utc(),
        "metrics": {
            pipeline: {
                "avg_row_count": m.avg_row_count,
                "avg_error_rate": m.avg_error_rate,
                "avg_latency_seconds": m.avg_latency_seconds,
                "total_runs": m.total_runs,
                "failure_count": m.failure_count,
            }
            for pipeline, m in metrics.items()
        },
    }
    path.write_text(json.dumps(payload, indent=2))
    return path


def load_baseline(
    name: str, baselines_dir: str = _BASELINES_DIR
) -> Optional[dict]:
    """Load a previously saved baseline by name.  Returns *None* if not found."""
    path = _baseline_path(name, baselines_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text())


def list_baselines(baselines_dir: str = _BASELINES_DIR) -> list[str]:
    """Return sorted list of available baseline names."""
    d = Path(baselines_dir)
    if not d.exists():
        return []
    return sorted(p.stem for p in d.glob("*.json"))


def diff_baseline(
    name: str,
    current: dict[str, PipelineMetrics],
    baselines_dir: str = _BASELINES_DIR,
) -> dict[str, dict[str, Optional[float]]]:
    """Compare *current* metrics against a saved baseline.

    Returns a mapping of pipeline -> field -> delta (current - baseline).
    Missing pipelines or fields are represented as *None*.
    """
    baseline = load_baseline(name, baselines_dir)
    if baseline is None:
        raise FileNotFoundError(f"Baseline '{name}' not found in {baselines_dir}")

    result: dict[str, dict[str, Optional[float]]] = {}
    fields = ("avg_row_count", "avg_error_rate", "avg_latency_seconds")
    for pipeline, m in current.items():
        base_m = baseline["metrics"].get(pipeline)
        deltas: dict[str, Optional[float]] = {}
        for field in fields:
            cur_val = getattr(m, field)
            base_val = base_m.get(field) if base_m else None
            if cur_val is None or base_val is None:
                deltas[field] = None
            else:
                deltas[field] = cur_val - base_val
        result[pipeline] = deltas
    return result
