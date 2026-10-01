"""
PostgreSQL repository for quality schema — check_results and reports.
"""

from __future__ import annotations

from typing import Any

import psycopg2.extras


class QualityRepository:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    # ── Check results ─────────────────────────────────────────────────────────
    def list_checks(
        self,
        domain: str | None = None,
        layer: str | None = None,
        status: str | None = None,
        hours: float = 24.0,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Return (total_count, page_of_results)."""
        conditions = ["run_at >= NOW() - (INTERVAL '1 hour' * %s)"]
        params: list[Any] = [hours]
        if domain:
            conditions.append("domain = %s")
            params.append(domain)
        if layer:
            conditions.append("layer = %s")
            params.append(layer)
        if status:
            conditions.append("status = %s")
            params.append(status)

        where = "WHERE " + " AND ".join(conditions)

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) FROM quality.check_results {where}", params)
            total = cur.fetchone()["count"]

            cur.execute(
                f"""
                SELECT id, run_id, domain, layer, check_name, column_name,
                       status, metric_value, threshold, message, rows_checked, run_at
                FROM quality.check_results
                {where}
                ORDER BY run_at DESC, domain, check_name
                LIMIT %s OFFSET %s
                """,
                [*params, limit, offset],
            )
            rows = [dict(r) for r in cur.fetchall()]
        return int(total), rows

    # ── Reports ───────────────────────────────────────────────────────────────
    def list_reports(
        self,
        domain: str | None = None,
        layer: str | None = None,
        hours: float = 24.0,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        conditions = ["run_at >= NOW() - (INTERVAL '1 hour' * %s)"]
        params: list[Any] = [hours]
        if domain:
            conditions.append("domain = %s")
            params.append(domain)
        if layer:
            conditions.append("layer = %s")
            params.append(layer)

        where = "WHERE " + " AND ".join(conditions)
        params.append(limit)

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, run_id, domain, layer, total_checks,
                       passed, failed, warnings, errors, pass_rate, run_at
                FROM quality.reports
                {where}
                ORDER BY run_at DESC
                LIMIT %s
                """,
                params,
            )
            return [dict(r) for r in cur.fetchall()]

    # ── Summary aggregation ───────────────────────────────────────────────────
    def summary_by_layer(self, hours: float = 24.0) -> list[dict[str, Any]]:
        """Return one aggregated row per layer for the last *hours* window."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    layer,
                    %s                              AS period_hours,
                    SUM(total_checks)::int          AS total_checks,
                    SUM(passed)::int                AS passed,
                    SUM(failed)::int                AS failed,
                    SUM(warnings)::int              AS warnings,
                    SUM(errors)::int                AS errors,
                    ROUND(AVG(pass_rate)::numeric, 4)::float AS pass_rate,
                    COUNT(DISTINCT domain)::int     AS domains_checked
                FROM quality.reports
                WHERE run_at >= NOW() - (INTERVAL '1 hour' * %s)
                GROUP BY layer
                ORDER BY layer
                """,
                (hours, hours),
            )
            return [dict(r) for r in cur.fetchall()]

    # ── Trend ─────────────────────────────────────────────────────────────────
    def trend(
        self,
        domain: str | None = None,
        layer: str | None = None,
        hours: float = 48.0,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Pass-rate trend over time — useful for time-series chart panels."""
        conditions = ["run_at >= NOW() - (INTERVAL '1 hour' * %s)"]
        params: list[Any] = [hours]
        if domain:
            conditions.append("domain = %s")
            params.append(domain)
        if layer:
            conditions.append("layer = %s")
            params.append(layer)

        where = "WHERE " + " AND ".join(conditions)

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT run_at, layer, domain, pass_rate, failed
                FROM quality.reports
                {where}
                ORDER BY run_at ASC
                LIMIT %s
                """,
                [*params, limit],
            )
            return [dict(r) for r in cur.fetchall()]
