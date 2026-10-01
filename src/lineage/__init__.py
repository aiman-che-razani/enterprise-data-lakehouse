"""Table-level lineage.

Only the dependency-free graph models are re-exported here so the API image
(which has no PySpark) can import them. ``tracker`` and ``static_graph`` pull in
the pipeline metadata (and therefore PySpark) — import them explicitly:

    from src.lineage.tracker import LineageTracker
    from src.lineage.static_graph import build_static_edges
"""

from .models import Direction, LineageEdge, LineageNode, walk

__all__ = ["Direction", "LineageEdge", "LineageNode", "walk"]
