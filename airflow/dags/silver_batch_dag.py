"""
Silver Batch DAG — RetailNexus Medallion Architecture

Schedule:  "30 3 * * *"  (daily at 03:30 UTC, 90 min after Bronze batch completes)
Max active runs: 1 (prevents overlapping SCD2 MERGEs)

Dependency groups (parallel within each group, sequential across groups):

Group 1 — Core dimensions (no FK deps)
    suppliers, stores, employees

Group 2 — Product + customer dimensions (depend on suppliers/stores)
    products, customers

Group 3 — Campaign + order core (depend on customers/products)
    campaigns, orders

Group 4 — Transactional facts (depend on orders)
    order_lines, payments, shipments, returns

Group 5 — Operations + engagement (depend on products/stores)
    inventory, reviews, clickstream, iot_sensors, finance

Each domain task is a SparkSubmitOperator wrapping process_silver.py --domain X.
A quality_report task at the end collects row counts from pipeline.watermarks.
"""

from __future__ import annotations

from datetime import datetime, timedelta

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
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

_DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.2.0," "org.apache.hadoop:hadoop-aws:3.3.4"

_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    "spark.sql.adaptive.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}

_GROUPS: list[tuple[str, list[str]]] = [
    ("core_dims", ["suppliers", "stores", "employees"]),
    ("product_customer_dims", ["products", "customers"]),
    ("campaign_order_core", ["campaigns", "orders"]),
    ("transactional_facts", ["order_lines", "payments", "shipments", "returns"]),
    ("ops_engagement_facts", ["inventory", "reviews", "clickstream", "iot_sensors", "finance"]),
]


def _make_silver_task(domain: str, dag: DAG) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=f"silver_{domain}",
        application="spark/jobs/silver/process_silver.py",
        application_args=["--domain", domain, "--batch-id", "{{ ds }}T03:30:00"],
        packages=_DELTA_PACKAGES,
        conf=_SPARK_CONF,
        conn_id="spark_default",
        verbose=False,
        dag=dag,
    )


def _silver_quality_report(**context) -> None:
    """Log Silver row counts from pipeline.watermarks to Airflow task logs."""
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
                   silver_bronze_version,
                   silver_rows_written,
                   silver_last_run
            FROM pipeline.watermarks
            ORDER BY domain
            """
        )
        rows = cur.fetchall()
    conn.close()

    print("=== Silver Quality Report ===")
    total_rows = 0
    for domain, version, rows_written, last_run in rows:
        rows_written = rows_written or 0
        total_rows += rows_written
        print(f"  {domain:20s}  version={version}  rows={rows_written:>10,}  last_run={last_run}")
    print(f"  {'TOTAL':20s}  {'':>8}  rows={total_rows:>10,}")

    context["ti"].xcom_push(key="total_silver_rows", value=total_rows)


with DAG(
    dag_id="silver_batch",
    description="Daily Silver processing — cleanse, PII-tokenise, SCD2 merge all 16 domains",
    schedule="30 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["silver", "medallion", "delta", "batch", "scd2"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    quality_report = PythonOperator(
        task_id="silver_quality_report",
        python_callable=_silver_quality_report,
    )

    # Build group task lists and chain them sequentially
    prev_group_end: list = [start]

    for _group_name, domains in _GROUPS:
        domain_tasks = [_make_silver_task(d, dag) for d in domains]

        # All tasks in this group depend on all tasks in the previous group
        for prev in prev_group_end:
            for task in domain_tasks:
                prev >> task

        prev_group_end = domain_tasks

    for task in prev_group_end:
        task >> quality_report

    quality_report >> end
