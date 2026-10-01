"""SQL access for the data catalog (catalog.tables / catalog.columns / catalog.lineage)."""

from __future__ import annotations

from typing import Any

import psycopg2.extras

from src.lineage.models import LineageEdge


class CatalogRepository:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def list_tables(
        self,
        layer: str | None = None,
        domain: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if layer:
            conditions.append("layer = %s")
            params.append(layer)
        if domain:
            conditions.append("domain = %s")
            params.append(domain)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT table_name, layer, domain, s3_location AS location,
                       COALESCE(format, 'delta') AS format, row_count, size_bytes,
                       created_at::text AS registered_at, updated_at::text AS last_updated,
                       description
                FROM catalog.tables
                {where}
                ORDER BY layer, domain
                LIMIT %s OFFSET %s
                """,
                [*params, limit, offset],
            )
            return [dict(r) for r in cur.fetchall()]

    def get_table(self, table_name: str) -> dict[str, Any] | None:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT table_name, layer, domain, s3_location AS location,
                       COALESCE(format, 'delta') AS format, row_count, size_bytes,
                       created_at::text AS registered_at, updated_at::text AS last_updated,
                       description
                FROM catalog.tables
                WHERE table_name = %s
                """,
                (table_name,),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def table_exists(self, table_name: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM catalog.tables WHERE table_name = %s", (table_name,))
            return cur.fetchone() is not None

    def list_columns(self, table_name: str) -> list[dict[str, Any]]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT c.column_name, c.data_type, COALESCE(c.is_nullable, TRUE) AS is_nullable,
                       COALESCE(c.is_pii, FALSE) AS is_pii, c.description, c.ordinal_position
                FROM catalog.columns c
                JOIN catalog.tables t ON t.table_id = c.table_id
                WHERE t.table_name = %s
                ORDER BY c.ordinal_position, c.column_name
                """,
                (table_name,),
            )
            return [dict(r) for r in cur.fetchall()]

    def lineage_edges(self) -> list[LineageEdge]:
        """All lineage edges. The platform graph is a few hundred rows, so traversal is in Python."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT source_table, target_table,
                       COALESCE(transformation_type, '') AS transformation_type,
                       COALESCE(job_name, '')            AS job_name
                FROM catalog.lineage
                """
            )
            return [LineageEdge(**dict(r)) for r in cur.fetchall()]
