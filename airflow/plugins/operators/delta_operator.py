"""
Custom Airflow operator for Delta Lake operations.

DeltaIngestOperator       — wraps BronzeBatchIngestor for a single domain
DeltaOptimizeOperator     — runs OPTIMIZE + ZORDER via spark-submit
DeltaVacuumOperator       — runs VACUUM to enforce retention policy
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


# Delta + the S3A connector (MinIO / S3). Keep in sync with docker/airflow/Dockerfile, which pre-fetches them.
_PACKAGES = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"


class DeltaIngestOperator(BaseOperator):
    """
    Ingests a single domain into the Bronze Delta table.

    Delegates to spark/jobs/bronze/ingest_bronze.py via SparkSubmitOperator
    so it runs on the Spark cluster (not on the Airflow worker).
    """

    template_fields = ("domain", "raw_dir", "batch_id")
    ui_color = "#c8a951"

    def __init__(
        self,
        *,
        domain: str,
        raw_dir: str | None = None,
        batch_id: str = "{{ run_id }}",
        spark_conn_id: str = "spark_default",
        executor_memory: str = "4g",
        executor_cores: int = 2,
        num_executors: int = 2,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.domain = domain
        self.raw_dir = raw_dir
        self.batch_id = batch_id
        self.spark_conn_id = spark_conn_id
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.num_executors = num_executors

    def execute(self, context: Context) -> dict[str, Any]:
        from airflow.utils.log.logging_mixin import LoggingMixin

        log = LoggingMixin().log

        log.info("DeltaIngestOperator: domain=%s, batch_id=%s", self.domain, self.batch_id)

        op = SparkSubmitOperator(
            task_id=f"_spark_ingest_{self.domain}",
            conn_id=self.spark_conn_id,
            application="spark/jobs/bronze/ingest_bronze.py",
            application_args=[
                "--domain",
                self.domain,
                # Unset -> the job picks s3a://<bucket>/raw (object storage) or datasets/raw
                *(["--raw-dir", self.raw_dir] if self.raw_dir else []),
                "--batch-id",
                self.batch_id,
            ],
            executor_memory=self.executor_memory,
            executor_cores=self.executor_cores,
            num_executors=self.num_executors,
            packages=_PACKAGES,
            conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            },
            dag=self.dag,
        )
        return op.execute(context)


class DeltaOptimizeOperator(BaseOperator):
    """Runs OPTIMIZE + ZORDER on a Bronze Delta table after ingestion."""

    template_fields = ("domain",)
    ui_color = "#6ea6d4"

    def __init__(
        self,
        *,
        domain: str,
        spark_conn_id: str = "spark_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.domain = domain
        self.spark_conn_id = spark_conn_id

    def execute(self, context: Context) -> None:
        op = SparkSubmitOperator(
            task_id=f"_spark_optimize_{self.domain}",
            conn_id=self.spark_conn_id,
            application="spark/jobs/bronze/optimize_bronze.py",
            application_args=["--domain", self.domain, "--skip-vacuum"],
            packages=_PACKAGES,
            dag=self.dag,
        )
        op.execute(context)


class DeltaVacuumOperator(BaseOperator):
    """Runs VACUUM on a Bronze Delta table to enforce retention policy."""

    template_fields = ("domain",)
    ui_color = "#91c47a"

    def __init__(
        self,
        *,
        domain: str,
        retain_hours: int = 7 * 365 * 24 + 48,  # Bronze keeps 7 years (+2 leap days) of history
        spark_conn_id: str = "spark_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.domain = domain
        self.retain_hours = retain_hours
        self.spark_conn_id = spark_conn_id

    def execute(self, context: Context) -> None:
        op = SparkSubmitOperator(
            task_id=f"_spark_vacuum_{self.domain}",
            conn_id=self.spark_conn_id,
            application="spark/jobs/bronze/optimize_bronze.py",
            application_args=["--domain", self.domain, "--vacuum-hours", str(self.retain_hours)],
            packages=_PACKAGES,
            dag=self.dag,
        )
        op.execute(context)
