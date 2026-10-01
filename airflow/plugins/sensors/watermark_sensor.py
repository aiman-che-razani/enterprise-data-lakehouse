"""
WatermarkSensor — polls pipeline.watermarks until a column exceeds a target
value, enabling cross-DAG dependencies without TriggerDagRunOperator.

Typical use: wait for Bronze ingestion to advance past a specific version
before starting Silver processing in a separate DAG or task group.

Usage:
    from sensors.watermark_sensor import WatermarkSensor

    wait_for_bronze = WatermarkSensor(
        task_id="wait_for_bronze_orders",
        domain="orders",
        column="bronze_last_ingest",
        min_value="{{ ds }} 02:00:00",
        postgres_conn_id="postgres_pipeline",
        poke_interval=60,        # seconds
        timeout=7200,            # 2 hours
        dag=dag,
    )

    wait_for_bronze >> silver_orders_task
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import psycopg2
from psycopg2 import sql as pgsql

from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class WatermarkSensor(BaseSensorOperator):
    """Poll pipeline.watermarks until *column* for *domain* >= *min_value*.

    Parameters
    ----------
    domain:          domain name to check (must exist in pipeline.watermarks)
    column:          watermark column name (e.g. "bronze_last_ingest",
                     "silver_last_run", "gold_last_run")
    min_value:       earliest acceptable timestamp string "YYYY-MM-DD HH:MM:SS"
                     or a Jinja expression (template_fields includes this)
    postgres_conn_id: Airflow connection for the PostgreSQL pipeline DB
    """

    template_fields = ("domain", "column", "min_value")
    ui_color = "#fff3e0"

    def __init__(
        self,
        domain: str,
        column: str,
        min_value: str,
        postgres_conn_id: str = "postgres_pipeline",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.domain = domain
        self.column = column
        self.min_value = min_value
        self.postgres_conn_id = postgres_conn_id

    # ------------------------------------------------------------------
    def poke(self, context: Context) -> bool:
        conn_info = BaseHook.get_connection(self.postgres_conn_id)
        conn = psycopg2.connect(
            host=conn_info.host,
            port=conn_info.port or 5432,
            dbname=conn_info.schema,
            user=conn_info.login,
            password=conn_info.password,
        )
        try:
            with conn.cursor() as cur:
                # Use parameterised column name via psycopg2.sql for safety
                cur.execute(
                    pgsql.SQL("SELECT {} FROM pipeline.watermarks WHERE domain = %s").format(
                        pgsql.Identifier(self.column)
                    ),
                    (self.domain,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if row is None or row[0] is None:
            self.log.info(
                "WatermarkSensor: domain=%s column=%s — no value yet, retrying",
                self.domain,
                self.column,
            )
            return False

        actual = row[0]
        # Compare as strings if both are str; otherwise datetime comparison
        if isinstance(actual, datetime):
            target = datetime.fromisoformat(self.min_value)
            satisfied = actual >= target
        else:
            satisfied = str(actual) >= str(self.min_value)

        self.log.info(
            "WatermarkSensor: domain=%s column=%s actual=%s min=%s satisfied=%s",
            self.domain,
            self.column,
            actual,
            self.min_value,
            satisfied,
        )
        return satisfied
