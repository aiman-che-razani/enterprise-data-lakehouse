"""
Warehouse Load DAG — RetailNexus Gold → Redshift + Snowflake + BigQuery

Schedule:  "30 7 * * *"  (07:30 UTC — after Gold batch at 05:00 which takes ~2h)
Max active runs: 1

All three warehouse targets load in parallel, after the Gold quality gate:
  start
    └── gold_dq_gate   (run_dq --layer gold --fail-fast: a CRITICAL failure blocks every load)
    ├── [redshift_dims → redshift_facts]
    ├── [snowflake_dims → snowflake_facts]
    └── [bigquery_dims  → bigquery_facts]
  warehouse_quality_report
  end

Each group (dims / facts) is a single SparkSubmitOperator.
Connector JARs are declared per task because Redshift / Snowflake / BigQuery
each need different JARs on the Spark cluster classpath.
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
    "retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(hours=4),
}

_DELTA_JARS = "io.delta:delta-spark_2.12:3.2.0," "org.apache.hadoop:hadoop-aws:3.3.4"

_BASE_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
}

_REDSHIFT_PKGS = (
    f"{_DELTA_JARS},"
    "io.github.spark_redshift_community:spark-redshift_2.12:5.1.0,"
    "com.amazon.redshift:redshift-jdbc42:2.1.0.29"
)

_SNOWFLAKE_PKGS = (
    f"{_DELTA_JARS},"
    "net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5,"
    "net.snowflake:snowflake-jdbc:3.14.5"
)

_BQ_PKGS = f"{_DELTA_JARS}," "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0"


def _spark_task(
    task_id: str,
    target: str,
    scope: str,
    packages: str,
    dag: DAG,
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application="spark/jobs/warehouse/load_warehouse.py",
        application_args=[
            "--target",
            target,
            f"--{scope}",
            "--batch-id",
            "{{ ds }}T07:30:00",
        ],
        packages=packages,
        conf=_BASE_CONF,
        conn_id="spark_default",
        verbose=False,
        dag=dag,
    )


def _warehouse_quality_report(**context) -> None:
    """Pull load counts from pipeline.watermarks for all 3 targets."""
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
            SELECT
                domain,
                redshift_last_load,
                snowflake_last_load,
                bigquery_last_load
            FROM pipeline.watermarks
            WHERE redshift_last_load  IS NOT NULL
               OR snowflake_last_load IS NOT NULL
               OR bigquery_last_load  IS NOT NULL
            ORDER BY domain
            """
        )
        rows = cur.fetchall()
    conn.close()

    print("=== Warehouse Load Quality Report ===")
    for domain, rs, sf, bq in rows:
        print(
            f"  {domain:25s}  redshift={rs or 'N/A'!s:25}  "
            f"snowflake={sf or 'N/A'!s:25}  bq={bq or 'N/A'!s:25}"
        )


with DAG(
    dag_id="warehouse_load",
    description="Daily Gold → Redshift + Snowflake + BigQuery warehouse load (all 3 in parallel)",
    schedule="30 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["warehouse", "redshift", "snowflake", "bigquery", "gold", "load"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    quality_report = PythonOperator(
        task_id="warehouse_quality_report",
        python_callable=_warehouse_quality_report,
    )

    # ── Redshift ──────────────────────────────────────────────────────────────
    rs_dims = _spark_task("redshift_dims", "redshift", "dims-only", _REDSHIFT_PKGS, dag)
    rs_facts = _spark_task("redshift_facts", "redshift", "facts-only", _REDSHIFT_PKGS, dag)

    # ── Snowflake ─────────────────────────────────────────────────────────────
    sf_dims = _spark_task("snowflake_dims", "snowflake", "dims-only", _SNOWFLAKE_PKGS, dag)
    sf_facts = _spark_task("snowflake_facts", "snowflake", "facts-only", _SNOWFLAKE_PKGS, dag)

    # ── BigQuery ──────────────────────────────────────────────────────────────
    bq_dims = _spark_task("bigquery_dims", "bigquery", "dims-only", _BQ_PKGS, dag)
    bq_facts = _spark_task("bigquery_facts", "bigquery", "facts-only", _BQ_PKGS, dag)

    # Dependency chain: dims before facts within each target; targets in parallel
    # Gate: warehouses only receive Gold that has passed its data-quality checks.
    gold_dq_gate = DQSparkOperator(
        task_id="gold_dq_gate",
        layer="gold",
        fail_on_critical=True,
    )

    start >> gold_dq_gate >> [rs_dims, sf_dims, bq_dims]
    rs_dims >> rs_facts >> quality_report
    sf_dims >> sf_facts >> quality_report
    bq_dims >> bq_facts >> quality_report
    quality_report >> end
