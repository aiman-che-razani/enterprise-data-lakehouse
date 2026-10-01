"""Persist and query lineage in ``catalog.lineage`` (PostgreSQL)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import psycopg2
import psycopg2.extras

from src.common.config.settings import Settings, get_settings
from src.common.logging import get_logger

from .models import Direction, LineageEdge, LineageNode, walk
from .static_graph import build_static_edges

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = get_logger(__name__)


class LineageTracker:
    def __init__(self, settings: Settings | None = None, conn: Any | None = None) -> None:
        """*conn* lets callers (and tests) inject an existing connection; it is not closed."""
        self.settings = settings or get_settings()
        self._injected_conn = conn

    # ─── Write ────────────────────────────────────────────────────────────────
    def record(self, edges: Iterable[LineageEdge], run_id: str | None = None) -> int:
        """Upsert *edges*; returns how many were written."""
        rows = [
            (e.source_table, e.target_table, e.transformation_type, e.job_name, run_id)
            for e in edges
        ]
        if not rows:
            return 0
        with self._connection() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO catalog.lineage
                        (source_table, target_table, transformation_type, job_name, run_id)
                    VALUES %s
                    ON CONFLICT (source_table, target_table, job_name) DO UPDATE SET
                        transformation_type = EXCLUDED.transformation_type,
                        run_id              = COALESCE(EXCLUDED.run_id, catalog.lineage.run_id)
                    """,
                    rows,
                )
            conn.commit()
        logger.info("lineage_recorded", edges=len(rows), run_id=run_id)
        return len(rows)

    def sync_static(self) -> int:
        """Seed/refresh the declared platform lineage graph."""
        return self.record(build_static_edges())

    # ─── Read ─────────────────────────────────────────────────────────────────
    def edges(self) -> list[LineageEdge]:
        with self._connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT source_table, target_table, COALESCE(transformation_type, ''), "
                "COALESCE(job_name, '') FROM catalog.lineage"
            )
            return [LineageEdge(*row) for row in cur.fetchall()]

    def trace(
        self, table: str, direction: Direction = "upstream", max_depth: int | None = None
    ) -> list[LineageNode]:
        return walk(self.edges(), table, direction, max_depth)

    # ─── Internals ────────────────────────────────────────────────────────────
    def _connection(self) -> _ConnCtx:
        return _ConnCtx(self.settings, self._injected_conn)


class _ConnCtx:
    """Context manager: yields the injected connection untouched, or opens/closes its own."""

    def __init__(self, settings: Settings, injected: Any | None) -> None:
        self._settings = settings
        self._injected = injected
        self._own: Any | None = None

    def __enter__(self) -> Any:
        if self._injected is not None:
            return self._injected
        self._own = psycopg2.connect(self._settings.postgres_dsn)
        return self._own

    def __exit__(self, *exc: object) -> None:
        if self._own is not None:
            self._own.close()
