"""ASCII sparkline graphs for pipeline health over time."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_history
from pipewatch.config import PipewatchConfig

_SPARKS = " ▁▂▃▄▅▆▇█"


@dataclass
class GraphResult:
    pipeline: str
    sparkline: str
    window: int
    total: int
    failures: int

    def __str__(self) -> str:
        return f"{self.pipeline}: {self.sparkline}  ({self.failures}/{self.total} failed)"


def _normalise(values: List[float]) -> List[float]:
    """Scale values to [0.0, 1.0]."""
    lo, hi = min(values), max(values)
    if hi == lo:
        return [0.5 for _ in values]
    return [(v - lo) / (hi - lo) for v in values]


def _spark_char(ratio: float, inverted: bool = False) -> str:
    """Return a single spark character for a normalised ratio."""
    idx = round(ratio * (len(_SPARKS) - 1))
    idx = max(0, min(idx, len(_SPARKS) - 1))
    if inverted:
        idx = len(_SPARKS) - 1 - idx
    return _SPARKS[idx]


def build_graph(
    pipeline_name: str,
    history_dir: str,
    window: int = 30,
) -> Optional[GraphResult]:
    """Build a sparkline graph from recent check history."""
    entries = load_history(history_dir, pipeline_name)
    if not entries:
        return None

    recent = entries[-window:]
    # 1.0 = healthy, 0.0 = failing
    health_values = [1.0 if e.get("healthy", False) else 0.0 for e in recent]
    failures = health_values.count(0.0)

    if len(health_values) == 1:
        spark = _spark_char(health_values[0])
    else:
        normalised = _normalise(health_values)
        spark = "".join(_spark_char(v) for v in normalised)

    return GraphResult(
        pipeline=pipeline_name,
        sparkline=spark,
        window=window,
        total=len(recent),
        failures=int(failures),
    )


def build_all_graphs(
    config: PipewatchConfig,
    history_dir: str,
    window: int = 30,
) -> List[GraphResult]:
    """Build sparkline graphs for all configured pipelines."""
    results = []
    for pipeline in config.pipelines:
        result = build_graph(pipeline.name, history_dir, window)
        if result is not None:
            results.append(result)
    return results
