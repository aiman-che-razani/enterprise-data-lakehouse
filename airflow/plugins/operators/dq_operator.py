"""
DQSparkOperator — convenience wrapper around SparkSubmitOperator that
pre-wires all DQ Spark job defaults so DAG authors only need to supply
layer/domains/fail_on_critical.

Usage in a DAG:
    from operators.dq_operator import DQSparkOperator

    bronze_dq = DQSparkOperator(
        task_id="bronze_dq",
        layer="bronze",
        dag=dag,
    )

    silver_orders_dq = DQSparkOperator(
        task_id="silver_orders_dq",
        layer="silver",
        domains=["orders", "order_lines", "payments"],
        fail_on_critical=True,
        dag=dag,
    )
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from airflow.utils.context import Context

_DELTA_JARS = "io.delta:delta-spark_2.12:3.2.0," "org.apache.hadoop:hadoop-aws:3.3.4"
_DEFAULT_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
    "spark.driver.memory": "4g",
    "spark.executor.memory": os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.executor.cores": os.environ.get("SPARK_EXECUTOR_CORES", "2"),
}

_VALID_LAYERS = frozenset({"bronze", "silver", "gold", "all"})


class DQSparkOperator(BaseOperator):
    """Submit a DQ check job for one medallion layer.

    Parameters
    ----------
    layer:            "bronze" | "silver" | "gold" | "all"
    domains:          specific domain/table names to check (default: all)
    fail_on_critical: pass --fail-fast to the Spark job (exit code 3 on breach)
    batch_id:         Jinja-templated batch identifier (default: ``{{ ds }}T00:00:00``)
    spark_conn_id:    Airflow connection for the Spark cluster
    packages:         Maven coordinates (default: Delta + Hadoop AWS JARs)
    extra_conf:       additional Spark conf overrides
    """

    template_fields: Sequence[str] = ("batch_id",)
    ui_color = "#e8f5e9"

    def __init__(
        self,
        layer: str,
        domains: list[str] | None = None,
        fail_on_critical: bool = False,
        batch_id: str = "{{ ds }}T00:00:00",
        spark_conn_id: str = "spark_default",
        packages: str = _DELTA_JARS,
        extra_conf: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if layer not in _VALID_LAYERS:
            raise ValueError(f"layer must be one of {_VALID_LAYERS}, got {layer!r}")
        self.layer = layer
        self.domains = domains or []
        self.fail_on_critical = fail_on_critical
        self.batch_id = batch_id
        self.spark_conn_id = spark_conn_id
        self.packages = packages
        self.conf = {**_DEFAULT_CONF, **(extra_conf or {})}

    # ------------------------------------------------------------------
    def execute(self, context: Context) -> None:
        app_args = ["--layer", self.layer, "--batch-id", self.batch_id]
        if self.domains:
            app_args += ["--domains", *self.domains]
        if self.fail_on_critical:
            app_args.append("--fail-fast")

        self.log.info(
            "DQSparkOperator submitting: layer=%s domains=%s fail_on_critical=%s",
            self.layer,
            self.domains or "all",
            self.fail_on_critical,
        )

        # Delegate to SparkSubmitOperator for actual submission
        # The inner operator joins the same DAG, so it needs an id of its own (this task's id is taken).
        submit = SparkSubmitOperator(
            task_id=f"_spark_{self.task_id}",
            application="spark/jobs/quality/run_dq.py",
            application_args=app_args,
            packages=self.packages,
            conf=self.conf,
            conn_id=self.spark_conn_id,
            verbose=False,
            dag=self.dag,
        )
        # Copy context and run
        submit.pre_execute(context)
        submit.execute(context)
        submit.post_execute(context)
