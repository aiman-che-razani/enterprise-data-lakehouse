"""
PostgreSQL repository for pipeline schema — watermarks, runs, SLA misses.
All methods accept a live psycopg2 connection (injected via FastAPI deps).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg2.extras


class PipelineRepository:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    # ── Watermarks ────────────────────────────────────────────────────────────
    def list_watermarks(self) -> list[dict[str, Any]]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT domain,
                       bronze_last_ingest,
                       silver_last_run,
                       gold_last_run,
                       redshift_last_load,
                       snowflake_last_load,
                       bigquery_last_load
                FROM pipeline.watermarks
                ORDER BY domain
                """
            )
            return [dict(r) for r in cur.fetchall()]

    def get_watermark(self, domain: str) -> dict[str, Any] | None:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT domain,
                       bronze_last_ingest,
                       silver_last_run,
                       gold_last_run,
                       redshift_last_load,
                       snowflake_last_load,
                       bigquery_last_load
                FROM pipeline.watermarks
                WHERE domain = %s
                """,
                (domain,),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    # ── SLA misses ────────────────────────────────────────────────────────────
    def list_sla_misses(
        self,
        hours: float = 24.0,
        domain: str | None = None,
        layer: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        conditions = ["detected_at >= NOW() - (INTERVAL '1 hour' * %s)"]
        params: list[Any] = [hours]
        if domain:
            conditions.append("domain = %s")
            params.append(domain)
        if layer:
            conditions.append("layer = %s")
            params.append(layer)

        where = " AND ".join(conditions)
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, run_id, domain, layer, metric,
                       actual_value, sla_threshold, message, detected_at
                FROM pipeline.sla_misses
                WHERE {where}
                ORDER BY detected_at DESC
                LIMIT %s
                """,
                [*params, limit],
            )
            return [dict(r) for r in cur.fetchall()]

    def count_sla_misses(self, hours: float = 24.0) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM pipeline.sla_misses
                WHERE detected_at >= NOW() - (INTERVAL '1 hour' * %s)
                """,
                (hours,),
            )
            return int(cur.fetchone()[0])

    # ── Pipeline runs ─────────────────────────────────────────────────────────
    def list_runs(
        self,
        layer: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if layer:
            conditions.append("layer = %s")
            params.append(layer)
        if status:
            conditions.append("status = %s")
            params.append(status)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, run_id, dag_id, layer, status,
                       started_at, ended_at, rows_written, error_message
                FROM pipeline.runs
                {where}
                ORDER BY started_at DESC
                LIMIT %s
                """,
                params,
            )
            return [dict(r) for r in cur.fetchall()]

    # ── Freshness computation (for status endpoint) ───────────────────────────
    def freshness_hours(self, ts: datetime | None) -> float | None:
        if ts is None:
            return None
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return round((datetime.now(UTC) - ts).total_seconds() / 3600, 2)
