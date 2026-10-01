"""
Airflow DAG: Bronze Batch Ingestion

Schedule: daily at 02:00 UTC
Ingests all 16 domains from datasets/raw/ into Bronze Delta tables on MinIO/S3.

DAG structure:
  start
    → [parallel group 1: dimension-like domains]
        suppliers, stores, employees, products, customers, campaigns
    → [parallel group 2: transactional domains]
        orders, payments
    → [parallel group 3: order-dependent domains]
        order_lines, shipments, returns
    → [parallel group 4: analytics domains]
        inventory, reviews, clickstream, iot_sensors, finance
    → quality_report
    → end

Each domain task: DeltaIngestOperator → DeltaOptimizeOperator (sequential within domain)
"""

from __future__ import annotations

from datetime import datetime, timedelta

# Airflow puts $AIRFLOW_HOME/plugins on sys.path, so plugin packages import as top-level
# ``operators`` / ``sensors`` / ``hooks`` (the repo-root ``airflow/`` folder is not a package).
from operators.delta_operator import DeltaIngestOperator, DeltaOptimizeOperator

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# Dependency groups — domains in the same group run in parallel
_GROUPS: list[list[str]] = [
    # Group 1: Reference / dimension data
    ["suppliers", "stores", "employees", "products", "customers", "campaigns"],
    # Group 2: Core transactions (orders must precede order_lines)
    ["orders", "payments"],
    # Group 3: Order-dependent entities
    ["order_lines", "shipments", "returns"],
    # Group 4: Analytics / operational
    ["inventory", "reviews", "clickstream", "iot_sensors", "finance"],
]


def _write_quality_report(**context):
    """Log a summary of the ingestion run to pipeline.runs."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    run_id = context["run_id"]
    hook.run(
        """
        INSERT INTO pipeline.runs
            (run_id, dag_id, execution_date, status, started_at, ended_at)
        VALUES (%s, %s, %s, 'success', NOW() - INTERVAL '2 hours', NOW())
        ON CONFLICT (run_id) DO UPDATE SET status = 'success', ended_at = NOW()
        """,
        parameters=(run_id, "bronze_batch_ingestion", context["ds"]),
    )


with DAG(
    dag_id="bronze_batch_ingestion",
    description="Ingest all 16 domains from raw files into Bronze Delta tables",
    schedule="0 2 * * *",  # 02:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs stomping the same tables
    tags=["bronze", "ingestion", "delta", "batch"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    quality_report = PythonOperator(
        task_id="write_quality_report",
        python_callable=_write_quality_report,
    )

    prev_group_tasks: list = [start]

    for _group_idx, group_domains in enumerate(_GROUPS):
        group_end_tasks = []

        for domain in group_domains:
            ingest = DeltaIngestOperator(
                task_id=f"ingest_{domain}",
                domain=domain,
                batch_id="{{ run_id }}",
                executor_memory="4g",
                executor_cores=2,
                num_executors=2,
            )
            optimize = DeltaOptimizeOperator(
                task_id=f"optimize_{domain}",
                domain=domain,
            )

            # Within a domain: ingest → optimize
            ingest >> optimize

            # All previous group tasks → this domain's ingest
            for prev_task in prev_group_tasks:
                prev_task >> ingest

            group_end_tasks.append(optimize)

        prev_group_tasks = group_end_tasks

    # Last group → quality report → end
    for task in prev_group_tasks:
        task >> quality_report

    quality_report >> end
