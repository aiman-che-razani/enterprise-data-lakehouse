"""Table-level lineage models and graph traversal (pure Python, no Spark/DB)."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Iterable

Direction = Literal["upstream", "downstream"]


@dataclass(frozen=True)
class LineageEdge:
    """``source_table`` feeds ``target_table`` via ``job_name``.

    Tables are addressed as ``<layer>.<name>`` (``silver.orders``, ``gold.fact_sales``,
    ``kafka.retailnexus.orders``), matching ``catalog.tables.table_name``.
    """

    source_table: str
    target_table: str
    transformation_type: str
    job_name: str


@dataclass(frozen=True)
class LineageNode:
    table: str
    depth: int  # hops away from the queried table (1 = direct neighbour)


def walk(
    edges: Iterable[LineageEdge],
    table: str,
    direction: Direction = "upstream",
    max_depth: int | None = None,
) -> list[LineageNode]:
    """Breadth-first traversal from *table*; each reachable table appears once.

    ``upstream`` follows edges backwards (what feeds *table*), ``downstream``
    forwards (what *table* feeds). Cycles are tolerated. Results are ordered by
    depth, then name, so output is deterministic.
    """
    if direction not in ("upstream", "downstream"):
        raise ValueError(f"direction must be 'upstream' or 'downstream', got {direction!r}")

    adjacency: dict[str, set[str]] = {}
    for e in edges:
        src, dst = (
            (e.target_table, e.source_table)
            if direction == "upstream"
            else (e.source_table, e.target_table)
        )
        adjacency.setdefault(src, set()).add(dst)

    seen = {table}
    out: list[LineageNode] = []
    queue: deque[tuple[str, int]] = deque([(table, 0)])
    while queue:
        current, depth = queue.popleft()
        if max_depth is not None and depth >= max_depth:
            continue
        for nxt in sorted(adjacency.get(current, ())):
            if nxt in seen:
                continue
            seen.add(nxt)
            out.append(LineageNode(nxt, depth + 1))
            queue.append((nxt, depth + 1))
    return sorted(out, key=lambda n: (n.depth, n.table))
