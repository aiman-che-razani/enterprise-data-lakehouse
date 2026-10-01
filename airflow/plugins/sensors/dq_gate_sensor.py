"""
DQPassRateSensor — holds a downstream task until the latest data-quality run for a
layer is recent *and* healthy. Use it as a quality gate between layers:

    from sensors.dq_gate_sensor import DQPassRateSensor

    silver_gate = DQPassRateSensor(
        task_id="silver_dq_gate",
        layer="silver",
        min_pass_rate=0.95,
        max_age_hours=6,
        postgres_conn_id="postgres_pipeline",
        poke_interval=120,
        timeout=3600,
        dag=dag,
    )
    silver_gate >> build_gold

The sensor keeps poking while there is no report in the window or the pass rate
is below the threshold (the DQ DAG may simply not have run yet).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import psycopg2

from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DQPassRateSensor(BaseSensorOperator):
    """Succeed once the mean ``pass_rate`` of ``quality.reports`` rows for *layer* in the
    last *max_age_hours* is >= *min_pass_rate* (pass_rate is a fraction in [0, 1])."""

    template_fields = ("layer", "min_pass_rate", "max_age_hours")
    ui_color = "#e8f5e9"

    def __init__(
        self,
        layer: str,
        min_pass_rate: float = 0.9,
        max_age_hours: float = 26.0,
        postgres_conn_id: str = "postgres_pipeline",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if layer not in ("bronze", "silver", "gold"):
            raise ValueError(f"layer must be bronze|silver|gold, got {layer!r}")
        if not 0.0 <= float(min_pass_rate) <= 1.0:
            raise ValueError("min_pass_rate must be within [0, 1]")
        self.layer = layer
        self.min_pass_rate = min_pass_rate
        self.max_age_hours = max_age_hours
        self.postgres_conn_id = postgres_conn_id

    def poke(self, context: Context) -> bool:
        info = BaseHook.get_connection(self.postgres_conn_id)
        conn = psycopg2.connect(
            host=info.host,
            port=info.port or 5432,
            dbname=info.schema,
            user=info.login,
            password=info.password,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT AVG(pass_rate), COUNT(*)
                    FROM quality.reports
                    WHERE layer = %s
                      AND run_at >= NOW() - (INTERVAL '1 hour' * %s)
                    """,
                    (self.layer, float(self.max_age_hours)),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        avg_rate, n_reports = row or (None, 0)
        if not n_reports or avg_rate is None:
            self.log.info(
                "DQPassRateSensor: no %s report in the last %sh — waiting",
                self.layer,
                self.max_age_hours,
            )
            return False

        ok = float(avg_rate) >= float(self.min_pass_rate)
        self.log.info(
            "DQPassRateSensor: layer=%s pass_rate=%.4f (min %.4f, %s reports) satisfied=%s",
            self.layer,
            float(avg_rate),
            float(self.min_pass_rate),
            n_reports,
            ok,
        )
        return ok
