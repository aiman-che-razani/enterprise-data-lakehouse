"""
RetailNexus — Pipeline Maintenance DAG

Schedule: "0 1 * * 0"  (Sunday 01:00 UTC — quietest traffic window)
Max active runs: 1

Tasks:
  start
  ├── [delta_maintenance]
  │     ├── bronze_optimize_all    (OPTIMIZE + ZORDER + VACUUM)
  │     ├── silver_optimize_all    (OPTIMIZE + ZORDER + VACUUM)
  │     └── gold_optimize_all      (OPTIMIZE + ZORDER + VACUUM)
  ├── catalog_sync                 (reflect Delta tables + lineage into catalog.*)
  ├── postgres_vacuum              (VACUUM ANALYZE all pipeline/quality tables)
  ├── watermark_audit              (log stale watermarks — domains > 25h behind)
  └── end

Wall-clock: ~2h expected.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

_DELTA_JARS = "io.delta:delta-spark_2.12:3.2.0," "org.apache.hadoop:hadoop-aws:3.3.4"
_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "8g",
}

# VACUUM retention: Bronze keeps its full 7-year window (delta.deletedFileRetentionDuration is
# set to 7 years at ingest; a shorter VACUUM would delete files time travel still needs),
# 3 days on Silver/Gold (storage cost vs. time-travel trade-off). delta_maintenance.py refuses
# a Bronze value below 61,368h.
_VACUUM_HOURS: dict[str, int] = {"bronze": 7 * 365 * 24 + 48, "silver": 72, "gold": 72}


def _postgres_vacuum(**_context) -> None:
    """VACUUM ANALYZE all pipeline / quality PostgreSQL tables."""
    import psycopg2
    from psycopg2 import sql

    from airflow.hooks.base import BaseHook

    conn_info = BaseHook.get_connection("postgres_pipeline")
    conn = psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port or 5432,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )
    # VACUUM cannot run inside a transaction block
    conn.autocommit = True

    tables = [
        "pipeline.watermarks",
        "pipeline.runs",
        "pipeline.sla_misses",
        "quality.check_results",
        "quality.reports",
    ]
    with conn.cursor() as cur:
        for tbl in tables:
            cur.execute(sql.SQL("VACUUM ANALYZE {}").format(sql.Identifier(*tbl.split("."))))
            print(f"VACUUM ANALYZE {tbl} — done")
    conn.close()


def _watermark_audit(**_context) -> None:
    """Log domains whose watermarks are stale (> 25h since last Bronze ingest)."""
    import psycopg2

    from airflow.hooks.base import BaseHook

    conn_info = BaseHook.get_connection("postgres_pipeline")
    conn = psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port or 5432,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT domain,
                   bronze_last_ingest,
                   EXTRACT(EPOCH FROM (NOW() - bronze_last_ingest)) / 3600 AS age_hours
            FROM pipeline.watermarks
            WHERE bronze_last_ingest IS NOT NULL
              AND NOW() - bronze_last_ingest > INTERVAL '25 hours'
            ORDER BY age_hours DESC
            """
        )
        stale = cur.fetchall()
    conn.close()

    if stale:
        print(f"\nWARNING — {len(stale)} domain(s) have stale Bronze watermarks:")
        for domain, last_ingest, age_hours in stale:
            print(f"  {domain:<25s}  last_ingest={last_ingest}  age={age_hours:.1f}h")
    else:
        print("All Bronze watermarks are current (within 25h).")


with DAG(
    dag_id="pipeline_maintenance",
    description="Weekly Delta OPTIMIZE/VACUUM and PostgreSQL maintenance",
    schedule="0 1 * * 0",
    start_date=datetime(2025, 1, 5),  # first Sunday
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["maintenance", "delta", "vacuum", "optimize"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    with TaskGroup("delta_maintenance", tooltip="OPTIMIZE + VACUUM all Delta layers") as delta_tg:
        layer_tasks = []
        for layer, vacuum_hours in _VACUUM_HOURS.items():
            task = SparkSubmitOperator(
                task_id=f"{layer}_optimize_vacuum",
                application="spark/jobs/maintenance/delta_maintenance.py",
                application_args=["--layer", layer, "--all", "--vacuum-hours", str(vacuum_hours)],
                packages=_DELTA_JARS,
                conf=_SPARK_CONF,
                conn_id="spark_default",
                verbose=False,
                execution_timeout=timedelta(hours=2),
            )
            layer_tasks.append(task)
        # Layers run in parallel (each manages its own S3 bucket prefix)

    catalog_sync = SparkSubmitOperator(
        task_id="catalog_sync",
        application="scripts/maintenance/sync_catalog.py",
        application_args=["--count-rows"],
        packages=_DELTA_JARS,
        conf=_SPARK_CONF,
        conn_id="spark_default",
        verbose=False,
        execution_timeout=timedelta(hours=1),
    )

    pg_vacuum = PythonOperator(
        task_id="postgres_vacuum",
        python_callable=_postgres_vacuum,
        execution_timeout=timedelta(minutes=30),
    )

    wm_audit = PythonOperator(
        task_id="watermark_audit",
        python_callable=_watermark_audit,
        execution_timeout=timedelta(minutes=5),
    )

    start >> delta_tg >> catalog_sync >> pg_vacuum >> wm_audit >> end
