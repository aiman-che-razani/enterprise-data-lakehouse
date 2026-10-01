"""
RetailNexus — End-to-End Pipeline DAG

Schedule: "0 0 * * *"  (midnight UTC, runs all layers sequentially)
Max active runs: 1

Topology (TaskGroups):
  start
  ├── [bronze] ─────────── ingest (4 parallel groups) → optimize
  ├── [silver] ─────────── scd2_dims → transactional → fact_edges → analytics → campaigns
  ├── [gold] ───────────── generated_dims → scd2_dims → derived_dims → facts
  ├── [warehouse] ──────── {redshift,snowflake,bigquery}: dims → facts  (parallel)
  └── [data_quality] ────  bronze_dq + silver_dq + gold_dq (parallel) → dq_report
  pipeline_report
  end

Individual per-layer DAGs (bronze_batch_dag, silver_batch_dag, etc.) remain
active for targeted reruns and monitoring.  This DAG is the primary SLA-bearing
end-to-end run that Airflow's SLA alerting hooks into.

Expected wall-clock: ~9h total (Bronze 2h · Silver 1.5h · Gold 1.5h · WH 2.5h · DQ 1.5h)
SLA deadline: 12:00 UTC (pipeline_report task has explicit SLA)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

# ─────────────────────────────────────────────────────────────────────────────
# Shared constants
# ─────────────────────────────────────────────────────────────────────────────
_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=3),
}

_DELTA_JARS = "io.delta:delta-spark_2.12:3.2.0," "org.apache.hadoop:hadoop-aws:3.3.4"
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

_BASE_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
}
_HEAVY_CONF = {
    **_BASE_CONF,
    "spark.driver.memory": "4g",
    "spark.executor.memory": os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.executor.cores": os.environ.get("SPARK_EXECUTOR_CORES", "2"),
}

_BATCH_ID = "{{ ds }}T00:00:00"

# ─────────────────────────────────────────────────────────────────────────────
# Bronze domain groups (mirrors bronze_batch_dag.py parallel groups)
# ─────────────────────────────────────────────────────────────────────────────
_BRONZE_GROUPS: list[list[str]] = [
    ["customers", "products", "suppliers", "stores", "employees"],  # dims
    ["orders", "payments", "shipments", "returns"],  # transactional
    ["inventory", "reviews", "finance", "campaigns"],  # operational / analytical
    ["order_lines", "clickstream", "iot_sensors"],  # high-volume
]

# Silver sequential dependency phases (mirrors silver_batch_dag.py)
_SILVER_PHASES: list[list[str]] = [
    ["suppliers", "stores"],
    ["customers", "products", "employees"],
    ["orders"],
    ["order_lines", "payments", "shipments", "returns", "inventory"],
    ["reviews", "clickstream", "iot_sensors", "finance", "campaigns"],
]

# Gold build phases (mirrors gold_batch_dag.py)
_GOLD_PHASES: list[list[str]] = [
    ["dim_date", "dim_location"],
    ["dim_supplier", "dim_store", "dim_employee"],
    ["dim_product", "dim_customer", "dim_promotion"],
    [
        "fact_sales",
        "fact_inventory",
        "fact_marketing",
        "fact_returns",
        "fact_shipments",
        "fact_finance",
    ],
]


# ─────────────────────────────────────────────────────────────────────────────
# Task factories
# ─────────────────────────────────────────────────────────────────────────────
def _bronze_ingest(domain: str, dag: DAG) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=f"ingest_{domain}",
        application="spark/jobs/bronze/ingest_bronze.py",
        application_args=["--domain", domain, "--batch-id", _BATCH_ID],
        packages=_DELTA_JARS,
        conf=_BASE_CONF,
        conn_id="spark_default",
        verbose=False,
        dag=dag,
    )


def _silver_process(domain: str, dag: DAG) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=f"process_{domain}",
        application="spark/jobs/silver/process_silver.py",
        application_args=["--domain", domain, "--batch-id", _BATCH_ID],
        packages=_DELTA_JARS,
        conf=_HEAVY_CONF,
        conn_id="spark_default",
        verbose=False,
        dag=dag,
    )


def _gold_build(table: str, dag: DAG) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=f"build_{table}",
        application="spark/jobs/gold/build_gold.py",
        application_args=["--table", table, "--batch-id", _BATCH_ID],
        packages=_DELTA_JARS,
        conf=_HEAVY_CONF,
        conn_id="spark_default",
        verbose=False,
        dag=dag,
    )


def _wh_task(task_id: str, target: str, scope: str, packages: str, dag: DAG) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application="spark/jobs/warehouse/load_warehouse.py",
        application_args=[
            "--target",
            target,
            f"--{scope}",
            "--batch-id",
            _BATCH_ID,
        ],
        packages=packages,
        conf=_BASE_CONF,
        conn_id="spark_default",
        verbose=False,
        dag=dag,
    )


def _dq_task(task_id: str, layer: str, dag: DAG) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application="spark/jobs/quality/run_dq.py",
        application_args=["--layer", layer, "--batch-id", _BATCH_ID],
        packages=_DELTA_JARS,
        conf=_HEAVY_CONF,
        conn_id="spark_default",
        verbose=False,
        dag=dag,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline report (end of run)
# ─────────────────────────────────────────────────────────────────────────────
def _pipeline_report(**context) -> None:
    """Pull a cross-layer health summary from pipeline.watermarks and quality.reports."""
    import psycopg2

    from airflow.hooks.base import BaseHook

    run_date = context["ds"]
    conn_info = BaseHook.get_connection("postgres_pipeline")
    conn = psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port or 5432,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )
    with conn.cursor() as cur:
        # Watermarks snapshot
        cur.execute(
            """
            SELECT domain,
                   bronze_last_ingest,
                   silver_last_run,
                   gold_last_run,
                   redshift_last_load,
                   snowflake_last_load,
                   bigquery_last_load
            FROM pipeline.watermarks
            ORDER BY domain
            """
        )
        watermarks = cur.fetchall()

        # DQ summary for today
        cur.execute(
            """
            SELECT layer,
                   SUM(total_checks) AS total,
                   SUM(passed)       AS passed,
                   SUM(failed)       AS failed,
                   ROUND(AVG(pass_rate)::numeric, 4) AS avg_pass_rate
            FROM quality.reports
            WHERE DATE(run_at) = %s
            GROUP BY layer
            ORDER BY layer
            """,
            (run_date,),
        )
        dq_rows = cur.fetchall()

        # SLA misses today
        cur.execute(
            """
            SELECT COUNT(*) FROM pipeline.sla_misses
            WHERE DATE(detected_at) = %s
            """,
            (run_date,),
        )
        sla_misses = cur.fetchone()[0]

    conn.close()

    print(f"\n{'='*72}")
    print(f"  RetailNexus Pipeline Health Report — {run_date}")
    print(f"{'='*72}")

    print("\n  WATERMARKS (last processed timestamp per domain):")
    for row in watermarks:
        domain = row[0]
        vals = [str(v or "—")[:19] for v in row[1:]]
        print(
            f"    {domain:<20s}  bronze={vals[0]}  silver={vals[1]}  "
            f"gold={vals[2]}  rs={vals[3]}  sf={vals[4]}  bq={vals[5]}"
        )

    print("\n  DATA QUALITY SUMMARY:")
    for layer, total, passed, failed, pass_rate in dq_rows:
        icon = "OK  " if failed == 0 else "FAIL"
        print(
            f"    [{icon}] {layer:<8s}  "
            f"total={total}  passed={passed}  failed={failed}  "
            f"pass_rate={float(pass_rate):.1%}"
        )

    print(f"\n  SLA MISSES TODAY: {sla_misses}")
    print(f"{'='*72}\n")

    if sla_misses > 0:
        raise Exception(
            f"Pipeline completed with {sla_misses} SLA miss(es) — "
            "check pipeline.sla_misses for details"
        )


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="pipeline_e2e",
    description=("End-to-end RetailNexus pipeline: " "Bronze → Silver → Gold → Warehouse → DQ"),
    schedule="0 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["e2e", "pipeline", "bronze", "silver", "gold", "warehouse", "dq"],
    sla_miss_callback=lambda context, sla, blocking_tasks, slas, blocking_tis: None,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    # ── Bronze ────────────────────────────────────────────────────────────────
    with TaskGroup("bronze", tooltip="Raw ingestion → Delta Bronze") as bronze_tg:

        with TaskGroup("optimize", tooltip="OPTIMIZE + ZORDER all Bronze tables") as bz_opt_tg:
            bronze_optimize = SparkSubmitOperator(
                task_id="bronze_optimize_all",
                application="spark/jobs/bronze/optimize_bronze.py",
                application_args=["--all"],
                packages=_DELTA_JARS,
                conf=_BASE_CONF,
                conn_id="spark_default",
                verbose=False,
                execution_timeout=timedelta(hours=1),
            )

        prev_group = None
        ingest_groups = []
        for idx, group_domains in enumerate(_BRONZE_GROUPS):
            with TaskGroup(
                f"ingest_group_{idx + 1}",
                tooltip=f"Ingest {', '.join(group_domains)}",
            ) as grp_tg:
                tasks = [_bronze_ingest(d, dag) for d in group_domains]
            ingest_groups.append(grp_tg)

        # All ingest groups run in parallel, then optimize
        ingest_groups >> bz_opt_tg

    # ── Silver ────────────────────────────────────────────────────────────────
    with TaskGroup("silver", tooltip="Cleanse · Validate · SCD2 · PII-tokenise") as silver_tg:
        phase_tasks = []
        for idx, phase_domains in enumerate(_SILVER_PHASES):
            with TaskGroup(
                f"phase_{idx + 1}",
                tooltip=f"Silver phase {idx + 1}: {', '.join(phase_domains)}",
            ) as phase_tg:
                [_silver_process(d, dag) for d in phase_domains]
            phase_tasks.append(phase_tg)

        # Sequential phases within Silver
        for i in range(len(phase_tasks) - 1):
            phase_tasks[i] >> phase_tasks[i + 1]

    # ── Gold ──────────────────────────────────────────────────────────────────
    with TaskGroup("gold", tooltip="Star schema: 8 dims + 6 facts") as gold_tg:
        gold_phase_tasks = []
        for idx, phase_tables in enumerate(_GOLD_PHASES):
            with TaskGroup(
                f"phase_{idx + 1}",
                tooltip=f"Gold phase {idx + 1}: {', '.join(phase_tables)}",
            ) as gp_tg:
                [_gold_build(t, dag) for t in phase_tables]
            gold_phase_tasks.append(gp_tg)

        for i in range(len(gold_phase_tasks) - 1):
            gold_phase_tasks[i] >> gold_phase_tasks[i + 1]

    # ── Warehouse ─────────────────────────────────────────────────────────────
    with TaskGroup("warehouse", tooltip="Gold → Redshift + Snowflake + BigQuery") as wh_tg:
        with TaskGroup("redshift") as rs_tg:
            rs_dims = _wh_task("dims", "redshift", "dims-only", _REDSHIFT_PKGS, dag)
            rs_facts = _wh_task("facts", "redshift", "facts-only", _REDSHIFT_PKGS, dag)
            rs_dims >> rs_facts

        with TaskGroup("snowflake") as sf_tg:
            sf_dims = _wh_task("dims", "snowflake", "dims-only", _SNOWFLAKE_PKGS, dag)
            sf_facts = _wh_task("facts", "snowflake", "facts-only", _SNOWFLAKE_PKGS, dag)
            sf_dims >> sf_facts

        with TaskGroup("bigquery") as bq_tg:
            bq_dims = _wh_task("dims", "bigquery", "dims-only", _BQ_PKGS, dag)
            bq_facts = _wh_task("facts", "bigquery", "facts-only", _BQ_PKGS, dag)
            bq_dims >> bq_facts

    # ── Data Quality ──────────────────────────────────────────────────────────
    with TaskGroup("data_quality", tooltip="DQ checks for all three layers") as dq_tg:
        bz_dq = _dq_task("bronze_dq", "bronze", dag)
        sv_dq = _dq_task("silver_dq", "silver", dag)
        gd_dq = _dq_task("gold_dq", "gold", dag)

        dq_report = PythonOperator(
            task_id="dq_summary",
            python_callable=lambda **ctx: None,  # covered by pipeline_report below
            trigger_rule="all_done",
        )
        [bz_dq, sv_dq, gd_dq] >> dq_report

    # ── End-of-pipeline report (SLA target: 12:00 UTC) ────────────────────────
    pipeline_report = PythonOperator(
        task_id="pipeline_report",
        python_callable=_pipeline_report,
        trigger_rule="all_done",
        sla=timedelta(hours=12),
    )

    # ── Global dependency chain ────────────────────────────────────────────────
    start >> bronze_tg >> silver_tg >> gold_tg >> wh_tg >> dq_tg >> pipeline_report >> end
