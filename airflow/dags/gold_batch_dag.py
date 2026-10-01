"""
Gold Batch DAG — RetailNexus Star Schema Build

Schedule:  "0 5 * * *"  (daily at 05:00 UTC, 90 min after Silver completes at 03:30)
Max active runs: 1 (dimension SK stability requires serial runs)

Build order:
  Phase 1 — Generated/derived dims (no Silver SCD2 dep):
      dim_date, dim_location

  Phase 2 — Core SCD2 dimensions:
      dim_supplier, dim_store, dim_employee

  Phase 3 — Product + customer dims (depend on supplier/store SKs):
      dim_product, dim_customer, dim_promotion

  Phase 4 — Fact tables (all dims must be built first):
      fact_sales, fact_inventory, fact_marketing,
      fact_returns, fact_shipments, fact_finance

  Phase 5 — Quality report (row counts from pipeline.watermarks)

Quality gate: nothing is built until the Silver DQ run passes — a CRITICAL failure
(--fail-fast, exit code 3) fails ``silver_dq_gate`` and blocks every Gold task.
"""

from __future__ import annotations

from datetime import datetime, timedelta

# Plugin packages import as top-level modules (Airflow adds $AIRFLOW_HOME/plugins to sys.path).
from operators.dq_operator import DQSparkOperator

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=3),
}

_DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.2.0," "org.apache.hadoop:hadoop-aws:3.3.4"

_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    # Broadcast join threshold — dim tables are small enough to broadcast
    "spark.sql.autoBroadcastJoinThreshold": "209715200",  # 200MB
}

_PHASES: list[tuple[str, list[str]]] = [
    ("generated_dims", ["dim_date", "dim_location"]),
    ("core_scd2_dims", ["dim_supplier", "dim_store", "dim_employee"]),
    ("product_customer_dims", ["dim_product", "dim_customer", "dim_promotion"]),
    (
        "facts",
        [
            "fact_sales",
            "fact_inventory",
            "fact_marketing",
            "fact_returns",
            "fact_shipments",
            "fact_finance",
        ],
    ),
]


def _gold_task(table: str, dag: DAG) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=f"gold_{table}",
        application="spark/jobs/gold/build_gold.py",
        application_args=["--table", table, "--batch-id", "{{ ds }}T05:00:00"],
        packages=_DELTA_PACKAGES,
        conf=_SPARK_CONF,
        conn_id="spark_default",
        verbose=False,
        dag=dag,
    )


def _gold_quality_report(**context) -> None:
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
            SELECT domain, gold_rows_written, gold_last_run
            FROM pipeline.watermarks
            WHERE gold_last_run IS NOT NULL
            ORDER BY domain
            """
        )
        rows = cur.fetchall()
    conn.close()

    print("=== Gold Quality Report ===")
    total = 0
    for table, rows_written, last_run in rows:
        rows_written = rows_written or 0
        total += rows_written
        print(f"  {table:25s}  rows={rows_written:>10,}  last_run={last_run}")
    print(f"  {'TOTAL':25s}  rows={total:>10,}")

    context["ti"].xcom_push(key="total_gold_rows", value=total)


with DAG(
    dag_id="gold_batch",
    description="Daily Gold star-schema build — 8 dims + 6 facts from Silver Delta",
    schedule="0 5 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["gold", "star-schema", "delta", "batch", "dimensional"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    quality_report = PythonOperator(
        task_id="gold_quality_report",
        python_callable=_gold_quality_report,
    )

    # Gate: Gold only reads Silver that has passed its data-quality checks.
    silver_dq_gate = DQSparkOperator(
        task_id="silver_dq_gate",
        layer="silver",
        fail_on_critical=True,
    )
    start >> silver_dq_gate

    prev_phase_tasks: list = [silver_dq_gate]

    for _phase_name, tables in _PHASES:
        phase_tasks = [_gold_task(t, dag) for t in tables]

        for prev in prev_phase_tasks:
            for task in phase_tasks:
                prev >> task

        prev_phase_tasks = phase_tasks

    for task in prev_phase_tasks:
        task >> quality_report

    quality_report >> end
