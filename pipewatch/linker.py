"""linker.py – Dependency linking between pipelines.

Tracks declared upstream/downstream relationships and surfaces
which pipelines are affected when a given pipeline is unhealthy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipewatch.config import PipewatchConfig
from pipewatch.history import load_history


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class LinkGraph:
    """Adjacency representation of pipeline dependencies."""

    # upstream[pipeline] = list of pipelines it depends on
    upstream: Dict[str, List[str]] = field(default_factory=dict)
    # downstream[pipeline] = list of pipelines that depend on it
    downstream: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class ImpactResult:
    """Result of an impact analysis for a single pipeline."""

    pipeline: str
    healthy: bool
    affected_downstream: List[str]
    root_cause_candidates: List[str]

    def __str__(self) -> str:  # pragma: no cover
        status = "OK" if self.healthy else "FAIL"
        affected = ", ".join(self.affected_downstream) or "none"
        roots = ", ".join(self.root_cause_candidates) or "none"
        return (
            f"{self.pipeline} [{status}] "
            f"→ affects: {affected} | root candidates: {roots}"
        )


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_graph(config: PipewatchConfig) -> LinkGraph:
    """Build a dependency graph from pipeline config.

    Pipelines may declare ``depends_on`` as a list of pipeline names
    in their configuration block.  If the field is absent the pipeline
    is treated as a root node.
    """
    graph = LinkGraph()
    for pipeline in config.pipelines:
        name = pipeline.name
        deps: List[str] = getattr(pipeline, "depends_on", None) or []
        graph.upstream[name] = list(deps)
        graph.downstream.setdefault(name, [])
        for dep in deps:
            graph.downstream.setdefault(dep, [])
            if name not in graph.downstream[dep]:
                graph.downstream[dep].append(name)
    return graph


def _reachable_downstream(graph: LinkGraph, start: str) -> List[str]:
    """Return all pipelines transitively downstream of *start* (BFS)."""
    visited: Set[str] = set()
    queue = list(graph.downstream.get(start, []))
    while queue:
        node = queue.pop(0)
        if node in visited:
            continue
        visited.add(node)
        queue.extend(graph.downstream.get(node, []))
    return sorted(visited)


# ---------------------------------------------------------------------------
# Impact analysis
# ---------------------------------------------------------------------------

def analyse_impact(
    pipeline_name: str,
    config: PipewatchConfig,
    history_dir: Optional[str] = None,
    graph: Optional[LinkGraph] = None,
) -> Optional[ImpactResult]:
    """Analyse the blast radius of a failing pipeline.

    Returns ``None`` if *pipeline_name* is not known to the config.
    """
    known = {p.name for p in config.pipelines}
    if pipeline_name not in known:
        return None

    if graph is None:
        graph = build_graph(config)

    # Determine health from most-recent history entry
    history = load_history(pipeline_name, history_dir=history_dir)
    healthy = True
    if history:
        latest = history[-1]
        healthy = latest.get("healthy", True)

    affected = _reachable_downstream(graph, pipeline_name)

    # Root-cause candidates: unhealthy upstream dependencies
    root_candidates: List[str] = []
    for dep in graph.upstream.get(pipeline_name, []):
        dep_history = load_history(dep, history_dir=history_dir)
        if dep_history and not dep_history[-1].get("healthy", True):
            root_candidates.append(dep)

    return ImpactResult(
        pipeline=pipeline_name,
        healthy=healthy,
        affected_downstream=affected,
        root_cause_candidates=root_candidates,
    )


def analyse_all_impacts(
    config: PipewatchConfig,
    history_dir: Optional[str] = None,
) -> List[ImpactResult]:
    """Run impact analysis for every pipeline in the config."""
    graph = build_graph(config)
    results: List[ImpactResult] = []
    for pipeline in config.pipelines:
        result = analyse_impact(
            pipeline.name,
            config,
            history_dir=history_dir,
            graph=graph,
        )
        if result is not None:
            results.append(result)
    return results
