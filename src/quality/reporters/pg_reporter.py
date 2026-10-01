"""PostgreSQL reporter — persists check results to quality.check_results and
quality.reports, and SLA misses to pipeline.sla_misses.

Table definitions (already in scripts/setup/init_postgres.sql):
  quality.check_results  — one row per CheckResult
  quality.reports        — one row per (run_id, domain, layer) summary
  pipeline.sla_misses    — one row per SLA breach (freshness FAILED)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import psycopg2
import psycopg2.extras

from src.common.logging import get_logger
from src.quality.checks.base_check import CheckResult, CheckStatus

logger = get_logger(__name__)


class PostgresReporter:
    """Writes DQ results to PostgreSQL.

    Accepts a live psycopg2 connection so that callers can share a single
    connection with the pipeline watermark writer.
    """

    def __init__(self, conn: Any) -> None:
        self.conn = conn

    # ------------------------------------------------------------------
    def save_results(
        self,
        results: list[CheckResult],
        run_id: str | None = None,
    ) -> str:
        """Insert all results and return the *run_id* used."""
        if run_id is None:
            run_id = str(uuid.uuid4())

        rows = [
            (
                run_id,
                r.domain,
                r.layer,
                r.check_name,
                r.column_name,
                r.status.value,
                r.metric_value,
                r.threshold,
                r.message,
                r.rows_checked,
                r.run_at,
            )
            for r in results
        ]

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO quality.check_results
                    (run_id, domain, layer, check_name, column_name,
                     status, metric_value, threshold, message,
                     rows_checked, run_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
                page_size=500,
            )

        self.conn.commit()
        logger.info("dq_results_saved", run_id=run_id, count=len(results))
        return run_id

    # ------------------------------------------------------------------
    def save_report(
        self,
        results: list[CheckResult],
        run_id: str,
    ) -> None:
        """Upsert one summary row per (run, domain, layer); a retried run updates in place."""
        from collections import defaultdict

        groups: dict[tuple[str, str], list[CheckResult]] = defaultdict(list)
        for r in results:
            groups[(r.domain, r.layer)].append(r)

        rows = []
        for (domain, layer), group in groups.items():
            total = len(group)
            passed = sum(1 for r in group if r.status == CheckStatus.PASSED)
            failed = sum(1 for r in group if r.status == CheckStatus.FAILED)
            warnings = sum(1 for r in group if r.status == CheckStatus.WARNING)
            errors = sum(1 for r in group if r.status == CheckStatus.ERROR)
            pass_rate = passed / total if total > 0 else 0.0
            rows.append(
                (
                    run_id,
                    domain,
                    layer,
                    total,
                    passed,
                    failed,
                    warnings,
                    errors,
                    round(pass_rate, 4),
                    datetime.now(UTC),
                )
            )

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO quality.reports
                    (run_id, domain, layer, total_checks, passed, failed,
                     warnings, errors, pass_rate, run_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, domain, layer) DO UPDATE SET
                    total_checks = EXCLUDED.total_checks, passed = EXCLUDED.passed,
                    failed = EXCLUDED.failed, warnings = EXCLUDED.warnings,
                    errors = EXCLUDED.errors, pass_rate = EXCLUDED.pass_rate,
                    run_at = EXCLUDED.run_at
                """,
                rows,
            )
        self.conn.commit()
        logger.info("dq_report_saved", run_id=run_id, domains=len(rows))

    # ------------------------------------------------------------------
    def save_sla_misses(
        self,
        results: list[CheckResult],
        run_id: str,
    ) -> None:
        """Insert freshness FAILED results into pipeline.sla_misses."""
        sla_failures = [
            r for r in results if r.check_name == "freshness" and r.status == CheckStatus.FAILED
        ]
        if not sla_failures:
            return

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO pipeline.sla_misses
                    (run_id, domain, layer, metric, actual_value,
                     sla_threshold, message, detected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        run_id,
                        r.domain,
                        r.layer,
                        f"freshness_hours:{r.column_name}",
                        r.metric_value,
                        r.threshold,
                        r.message,
                        r.run_at,
                    )
                    for r in sla_failures
                ],
            )
        self.conn.commit()
        logger.warning(
            "sla_misses_recorded",
            run_id=run_id,
            count=len(sla_failures),
            domains=[r.domain for r in sla_failures],
        )
