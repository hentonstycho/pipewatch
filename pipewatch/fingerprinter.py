"""fingerprinter.py – generate a stable failure fingerprint for a pipeline.

A fingerprint is a short hash that captures the current failure pattern
(which thresholds are violated and in what order) so that repeated alerts
for the *same* failure mode can be deduplicated or grouped.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from pipewatch.checker import CheckResult
from pipewatch.history import load_history
from pipewatch.config import PipelineConfig


@dataclass
class Fingerprint:
    pipeline: str
    digest: str          # 8-char hex
    violations: List[str]

    def __str__(self) -> str:
        viol = ", ".join(self.violations) if self.violations else "none"
        return f"{self.pipeline}  [{self.digest}]  violations={viol}"


def _violation_keys(result: CheckResult) -> List[str]:
    """Return sorted list of violated threshold names for a CheckResult."""
    keys: List[str] = []
    if not result.healthy:
        if result.row_count is not None and result.row_count == 0:
            keys.append("row_count")
        if result.error_rate is not None and result.error_rate > 0:
            keys.append("error_rate")
        if result.latency_seconds is not None and result.latency_seconds > 0:
            keys.append("latency")
        # Fall back: mark as generic failure when no metric detail available
        if not keys:
            keys.append("unknown")
    return sorted(keys)


def _hash_violations(pipeline: str, violations: List[str]) -> str:
    payload = json.dumps({"pipeline": pipeline, "violations": violations}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:8]


def fingerprint_pipeline(
    cfg: PipelineConfig,
    history_dir: Path,
    window: int = 5,
) -> Optional[Fingerprint]:
    """Compute a fingerprint from the most recent *window* check results.

    Returns ``None`` when there is no history for the pipeline.
    """
    entries = load_history(cfg.name, history_dir=history_dir)
    if not entries:
        return None

    recent = entries[-window:]
    # Aggregate violation keys seen across the window
    seen: dict[str, int] = {}
    for entry in recent:
        for key in _violation_keys(entry):
            seen[key] = seen.get(key, 0) + 1

    # Only include violations that appeared in the majority of recent runs
    majority = len(recent) / 2
    dominant = sorted(k for k, cnt in seen.items() if cnt > majority)

    digest = _hash_violations(cfg.name, dominant)
    return Fingerprint(pipeline=cfg.name, digest=digest, violations=dominant)


def fingerprint_all(
    pipelines: List[PipelineConfig],
    history_dir: Path,
    window: int = 5,
) -> List[Fingerprint]:
    """Return fingerprints for every pipeline that has history."""
    results: List[Fingerprint] = []
    for cfg in pipelines:
        fp = fingerprint_pipeline(cfg, history_dir=history_dir, window=window)
        if fp is not None:
            results.append(fp)
    return results
