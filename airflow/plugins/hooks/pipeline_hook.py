"""
PipelinePostgresHook — typed wrapper around Airflow's PostgresHook that
provides high-level methods for pipeline.watermarks, quality.check_results,
quality.reports, and pipeline.sla_misses.

Usage:
    from hooks.pipeline_hook import PipelinePostgresHook

    hook = PipelinePostgresHook()
    wm = hook.get_watermark("orders", "bronze_last_ingest")
    failures = hook.get_recent_dq_failures(hours=1)
    sla_count = hook.get_sla_miss_count(hours=24)
"""

from __future__ import annotations

from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook


class PipelinePostgresHook(PostgresHook):
    """Extends PostgresHook with typed pipeline schema accessors."""

    conn_name_attr = "postgres_conn_id"
    default_conn_name = "postgres_pipeline"
    conn_type = "postgres"
    hook_name = "Pipeline PostgreSQL"

    # ------------------------------------------------------------------
    # Watermarks
    # ------------------------------------------------------------------
    def get_watermark(self, domain: str, column: str) -> Any:
        """Return the scalar watermark value for *domain.column* or None."""
        from psycopg2 import sql as pgsql

        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    pgsql.SQL("SELECT {} FROM pipeline.watermarks WHERE domain = %s").format(
                        pgsql.Identifier(column)
                    ),
                    (domain,),
                )
                row = cur.fetchone()
        finally:
            conn.close()
        return row[0] if row else None

    def get_all_watermarks(self) -> list[dict]:
        """Return all rows in pipeline.watermarks as a list of dicts."""
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
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
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
        finally:
            conn.close()

    def upsert_watermark(self, domain: str, **column_values) -> None:
        """Update one or more watermark columns for *domain*.

        Example:
            hook.upsert_watermark("orders", bronze_last_ingest=datetime.now(timezone.utc))
        """
        from psycopg2 import sql as pgsql

        if not column_values:
            return
        conn = self.get_conn()
        try:
            set_clause = pgsql.SQL(", ").join(
                pgsql.SQL("{} = %s").format(pgsql.Identifier(col)) for col in column_values
            )
            with conn.cursor() as cur:
                cur.execute(
                    pgsql.SQL("UPDATE pipeline.watermarks SET {} WHERE domain = %s").format(
                        set_clause
                    ),
                    [*column_values.values(), domain],
                )
                if cur.rowcount == 0:
                    # Insert if domain doesn't exist yet
                    cols = pgsql.SQL(", ").join(
                        pgsql.Identifier(c) for c in ["domain", *column_values]
                    )
                    vals = pgsql.SQL(", ").join(pgsql.Placeholder() * (1 + len(column_values)))
                    cur.execute(
                        pgsql.SQL("INSERT INTO pipeline.watermarks ({}) VALUES ({})").format(
                            cols, vals
                        ),
                        [domain, *column_values.values()],
                    )
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Quality
    # ------------------------------------------------------------------
    def get_recent_dq_failures(self, hours: float = 1.0) -> list[dict]:
        """Return check results with status='failed' from the last *hours* hours."""
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT domain, layer, check_name, column_name,
                           metric_value, threshold, message, run_at
                    FROM quality.check_results
                    WHERE status   = 'failed'
                      AND run_at  >= NOW() - (INTERVAL '1 hour' * %s)
                    ORDER BY run_at DESC
                    """,
                    (hours,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
        finally:
            conn.close()

    def get_dq_pass_rate(self, hours: float = 24.0) -> dict[str, float]:
        """Return {layer: avg_pass_rate} for DQ reports in the last *hours*."""
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT layer, AVG(pass_rate)::float
                    FROM quality.reports
                    WHERE run_at >= NOW() - (INTERVAL '1 hour' * %s)
                    GROUP BY layer
                    """,
                    (hours,),
                )
                return {row[0]: round(row[1], 4) for row in cur.fetchall()}
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # SLA misses
    # ------------------------------------------------------------------
    def get_sla_miss_count(self, hours: float = 24.0) -> int:
        """Return count of SLA misses detected in the last *hours* hours."""
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM pipeline.sla_misses
                    WHERE detected_at >= NOW() - (INTERVAL '1 hour' * %s)
                    """,
                    (hours,),
                )
                return cur.fetchone()[0]
        finally:
            conn.close()

    def get_recent_sla_misses(self, hours: float = 6.0) -> list[dict]:
        """Return SLA miss rows from the last *hours* hours."""
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT domain, layer, metric, actual_value,
                           sla_threshold, message, detected_at
                    FROM pipeline.sla_misses
                    WHERE detected_at >= NOW() - (INTERVAL '1 hour' * %s)
                    ORDER BY detected_at DESC
                    """,
                    (hours,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
        finally:
            conn.close()
