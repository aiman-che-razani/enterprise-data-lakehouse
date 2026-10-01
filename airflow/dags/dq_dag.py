"""
Data Quality DAG — RetailNexus medallion DQ checks

Schedule:  "0 10 * * *"  (10:00 UTC — after warehouse loads finish at ~09:30)
Max active runs: 1

Pipeline:
  start
    ├── bronze_dq    (all 16 Bronze domains)
    ├── silver_dq    (all 16 Silver domains)
    └── gold_dq      (all 14 Gold tables)
  dq_summary_report  (PythonOperator — reads quality.reports from PostgreSQL)
  end

Each DQ task is a SparkSubmitOperator.  The DAG is trigger_rule="all_done"
on the summary so that quality reporting always runs even if a layer fails.
"""

from __future__ import annotations

import os
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
    "execution_timeout": timedelta(hours=3),
}

_DELTA_JARS = "io.delta:delta-spark_2.12:3.2.0," "org.apache.hadoop:hadoop-aws:3.3.4"

_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
    "spark.driver.memory": "4g",
    "spark.executor.memory": os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.executor.cores": os.environ.get("SPARK_EXECUTOR_CORES", "2"),
}


def _dq_task(task_id: str, layer: str, dag: DAG) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application="spark/jobs/quality/run_dq.py",
        application_args=[
            "--layer",
            layer,
            "--batch-id",
            "{{ ds }}T10:00:00",
        ],
        packages=_DELTA_JARS,
        conf=_SPARK_CONF,
        conn_id="spark_default",
        verbose=False,
        dag=dag,
    )


def _dq_summary_report(**context) -> None:
    """Pull pass rates per layer from quality.reports for the current run."""
    import psycopg2

    from airflow.hooks.base import BaseHook

    run_date = context["ds"]  # YYYY-MM-DD
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
            SELECT
                r.layer,
                r.domain,
                r.total_checks,
                r.passed,
                r.failed,
                r.warnings,
                r.pass_rate,
                r.run_at
            FROM quality.reports r
            WHERE DATE(r.run_at) = %s
            ORDER BY r.layer, r.domain
            """,
            (run_date,),
        )
        rows = cur.fetchall()
    conn.close()

    if not rows:
        print(f"No DQ report rows found for {run_date}")
        return

    print(f"\n{'='*70}")
    print(f"  RetailNexus Data Quality Summary — {run_date}")
    print(f"{'='*70}")
    current_layer = None
    total_checks = total_passed = total_failed = 0
    for layer, domain, checks, passed, failed, warnings, pass_rate, _run_at in rows:
        if layer != current_layer:
            print(f"\n  [{layer.upper()}]")
            current_layer = layer
        status_icon = "OK" if failed == 0 else "FAIL"
        print(
            f"    [{status_icon}] {domain:<30s} "
            f"checks={checks:3d}  passed={passed:3d}  "
            f"failed={failed:3d}  warnings={warnings:3d}  "
            f"pass_rate={pass_rate:.1%}"
        )
        total_checks += checks
        total_passed += passed
        total_failed += failed

    overall_rate = total_passed / total_checks if total_checks else 0
    print(
        f"\n  OVERALL: {total_checks} checks  |  "
        f"passed={total_passed}  failed={total_failed}  "
        f"pass_rate={overall_rate:.1%}"
    )
    print(f"{'='*70}\n")

    # Fail the Airflow task if any domain has DQ failures (informational — no
    # upstream data is blocked; this is a monitoring signal only)
    if total_failed > 0:
        raise Exception(
            f"DQ summary: {total_failed} failed checks across "
            f"{total_checks} total — review quality.check_results for details"
        )


with DAG(
    dag_id="data_quality",
    description=("Daily data quality checks for Bronze / Silver / Gold medallion layers"),
    schedule="0 10 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["dq", "quality", "bronze", "silver", "gold", "monitoring"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    bronze_dq = _dq_task("bronze_dq", "bronze", dag)
    silver_dq = _dq_task("silver_dq", "silver", dag)
    gold_dq = _dq_task("gold_dq", "gold", dag)

    summary = PythonOperator(
        task_id="dq_summary_report",
        python_callable=_dq_summary_report,
        trigger_rule="all_done",  # always generate the report even on failures
    )

    # All three layers run in parallel; summary waits for all
    start >> [bronze_dq, silver_dq, gold_dq] >> summary >> end
