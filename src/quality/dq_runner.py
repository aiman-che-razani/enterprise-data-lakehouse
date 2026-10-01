"""
DQ Runner — orchestrates Bronze / Silver / Gold quality engines for one or
more domains, persists results to PostgreSQL, and surfaces alerts.

Typical usage (from Spark job or Airflow PythonOperator):

    runner = DQRunner(spark=spark, settings=settings)
    summary = runner.run(layer="silver", domains=["orders", "payments"])

The runner is intentionally stateless: each call opens its own PostgreSQL
connection from *settings.postgres_dsn* and closes it when done.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import psycopg2

from src.common.logging import get_logger
from src.quality.reporters.alert_reporter import AlertReporter
from src.quality.reporters.pg_reporter import PostgresReporter

if TYPE_CHECKING:
    from src.common.config import Settings
    from src.quality.checks.base_check import CheckResult

logger = get_logger(__name__)

_VALID_LAYERS = frozenset({"bronze", "silver", "gold"})


class DQRunner:
    """Top-level orchestrator for data quality checks.

    Parameters
    ----------
    spark:            active SparkSession
    settings:         common Settings
    fail_on_critical: propagate a RuntimeError if critical checks fail
    """

    def __init__(
        self,
        spark: Any,
        settings: Settings,
        fail_on_critical: bool = False,
    ) -> None:
        self.spark = spark
        self.settings = settings
        self.fail_on_critical = fail_on_critical

    # ------------------------------------------------------------------
    def run(
        self,
        layer: str,
        domains: list[str] | None = None,
        batch_id: str | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Run all DQ checks for *layer* and return a JSON-serialisable summary.

        Parameters
        ----------
        layer:   "bronze" | "silver" | "gold"
        domains: specific domains/tables to check (default: all configured)
        batch_id: passed through to the report for traceability
        run_id:  UUID string; generated if not supplied
        """
        if layer not in _VALID_LAYERS:
            raise ValueError(f"layer must be one of {_VALID_LAYERS}, got {layer!r}")

        run_id = run_id or str(uuid.uuid4())
        batch_id = batch_id or datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")
        logger.info("dq_run_start", layer=layer, run_id=run_id, batch_id=batch_id)

        conn = psycopg2.connect(self.settings.postgres_dsn)
        try:
            results = self._run_engine(layer, domains, conn)
        finally:
            conn.close()

        # Persist
        pg = PostgresReporter(psycopg2.connect(self.settings.postgres_dsn))
        try:
            pg.save_results(results, run_id)
            pg.save_report(results, run_id)
            pg.save_sla_misses(results, run_id)
        finally:
            pg.conn.close()

        # Alert
        alert = AlertReporter(fail_on_critical=self.fail_on_critical)
        alert.report(results)
        summary = alert.summary(results)
        summary.update({"run_id": run_id, "batch_id": batch_id, "layer": layer})

        logger.info("dq_run_complete", **summary)
        return summary

    # ------------------------------------------------------------------
    def run_all_layers(
        self,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Run checks across all three layers and return a combined summary."""
        run_id = str(uuid.uuid4())
        batch_id = batch_id or datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")
        combined: list[CheckResult] = []
        layer_summaries: dict[str, dict[str, Any]] = {}

        conn = psycopg2.connect(self.settings.postgres_dsn)
        try:
            for layer in ("bronze", "silver", "gold"):
                layer_results = self._run_engine(layer, None, conn)
                combined.extend(layer_results)
                layer_summaries[layer] = AlertReporter().summary(layer_results)
        finally:
            conn.close()

        # Persist everything under the same run_id
        pg = PostgresReporter(psycopg2.connect(self.settings.postgres_dsn))
        try:
            pg.save_results(combined, run_id)
            pg.save_report(combined, run_id)
            pg.save_sla_misses(combined, run_id)
        finally:
            pg.conn.close()

        alert = AlertReporter(fail_on_critical=self.fail_on_critical)
        alert.report(combined)
        overall = alert.summary(combined)
        return {
            "run_id": run_id,
            "batch_id": batch_id,
            "overall": overall,
            "by_layer": layer_summaries,
        }

    # ------------------------------------------------------------------
    def _run_engine(
        self,
        layer: str,
        domains: list[str] | None,
        history_conn: Any,
    ) -> list[CheckResult]:
        if layer == "bronze":
            from src.quality.engines.bronze_dq import BronzeDQEngine

            return BronzeDQEngine(self.spark, self.settings, history_conn).run_all(domains)
        if layer == "silver":
            from src.quality.engines.silver_dq import SilverDQEngine

            return SilverDQEngine(self.spark, self.settings, history_conn).run_all(domains)
        # gold
        from src.quality.engines.gold_dq import GoldDQEngine

        return GoldDQEngine(self.spark, self.settings, history_conn).run_all(domains)
