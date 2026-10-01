"""
RetailNexus — Parameterized Backfill DAG

Schedule: None  (manually triggered via Airflow UI or CLI)

Purpose: replay one or more domains through a specific layer for a date range.
Useful when:
  - a Silver domain processor bug is fixed and data must be reprocessed
  - a Bronze ingest failed for a subset of domains on a specific date
  - Gold dims need to be rebuilt after a schema change

Usage (Airflow CLI):
    airflow dags trigger pipeline_backfill \
        --conf '{"layer":"silver","domains":"orders,payments","start_date":"2025-06-01","end_date":"2025-06-07"}'

Usage (Airflow UI):
    Trigger DAG → fill in the Params form

Parameters:
    layer       : "bronze" | "silver" | "gold" | "warehouse" | "dq"
    domains     : comma-separated domain/table names (empty string = all)
    start_date  : ISO date string YYYY-MM-DD (inclusive)
    end_date    : ISO date string YYYY-MM-DD (inclusive)
    target      : warehouse target for layer=warehouse ("redshift" | "snowflake" | "bigquery" | "all")
    fail_fast   : stop on first domain failure if true
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,  # no auto-retry on manual backfills
    "execution_timeout": timedelta(hours=8),
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

_LAYER_APPS = {
    "bronze": "spark/jobs/bronze/ingest_bronze.py",
    "silver": "spark/jobs/silver/process_silver.py",
    "gold": "spark/jobs/gold/build_gold.py",
    "warehouse": "spark/jobs/warehouse/load_warehouse.py",
    "dq": "spark/jobs/quality/run_dq.py",
}

_WH_PACKAGES = {
    "redshift": (
        f"{_DELTA_JARS},"
        "io.github.spark_redshift_community:spark-redshift_2.12:5.1.0,"
        "com.amazon.redshift:redshift-jdbc42:2.1.0.29"
    ),
    "snowflake": (
        f"{_DELTA_JARS},"
        "net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5,"
        "net.snowflake:snowflake-jdbc:3.14.5"
    ),
    "bigquery": (
        f"{_DELTA_JARS}," "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0"
    ),
}


def _build_args(**context) -> None:
    """Validate params and push the Spark application_args to XCom."""
    params = context["params"]
    layer = params["layer"]
    domains_raw = params.get("domains", "").strip()
    start_date = params.get("start_date", context["ds"])
    end_date = params.get("end_date", context["ds"])
    fail_fast = params.get("fail_fast", False)
    target = params.get("target", "all")

    batch_id = f"backfill_{start_date}_to_{end_date}"

    app_args: list[str] = ["--batch-id", batch_id]

    if layer in ("bronze", "silver", "gold", "dq"):
        if domains_raw:
            domains = [d.strip() for d in domains_raw.split(",") if d.strip()]
            if layer in ("bronze", "silver"):
                for d in domains:
                    app_args += ["--domain", d]
            elif layer == "gold":
                for d in domains:
                    app_args += ["--table", d]
            elif layer == "dq":
                app_args += ["--domains", *domains]
        else:
            app_args.append("--all")

        if fail_fast:
            app_args.append("--fail-fast")

    elif layer == "warehouse":
        app_args += ["--target", target]
        if domains_raw:
            domains = [d.strip() for d in domains_raw.split(",") if d.strip()]
            for d in domains:
                app_args += ["--table", d]
        else:
            app_args.append("--all")

    context["ti"].xcom_push(key="app_args", value=app_args)
    context["ti"].xcom_push(key="layer", value=layer)
    context["ti"].xcom_push(key="target", value=target)

    print("Backfill params validated.")
    print(f"  layer={layer}  domains={domains_raw or 'all'}")
    print(f"  date_range={start_date} → {end_date}")
    print(f"  app_args={app_args}")


def _run_backfill(**context) -> None:
    """Dynamically submit the Spark job based on XCom args (PythonOperator path).

    For production use, replace this with the SparkSubmitOperator below and
    wire through the XCom args via Jinja templating.
    """
    import subprocess

    layer = context["ti"].xcom_pull(key="layer")
    app = _LAYER_APPS.get(layer, "")
    args = context["ti"].xcom_pull(key="app_args") or []
    target = context["ti"].xcom_pull(key="target")

    pkgs = _WH_PACKAGES.get(target, _DELTA_JARS) if layer == "warehouse" else _DELTA_JARS

    cmd = [
        "spark-submit",
        "--packages",
        pkgs,
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf",
        "spark.driver.memory=4g",
        "--conf",
        "spark.executor.memory=" + os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"),
        app,
        *args,
    ]

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Backfill Spark job exited with code {result.returncode}")


with DAG(
    dag_id="pipeline_backfill",
    description="Manually triggered parameterized backfill for any pipeline layer",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,  # allow concurrent backfills of different layers
    default_args=_DEFAULT_ARGS,
    render_template_as_native_obj=True,
    params={
        "layer": Param(
            default="silver",
            type="string",
            enum=["bronze", "silver", "gold", "warehouse", "dq"],
            description="Medallion layer to backfill",
        ),
        "domains": Param(
            default="",
            type="string",
            description=(
                "Comma-separated domain/table names. " "Empty = all configured for the layer."
            ),
        ),
        "start_date": Param(
            default="",
            type="string",
            description="Backfill start date YYYY-MM-DD (informational — used in batch_id)",
        ),
        "end_date": Param(
            default="",
            type="string",
            description="Backfill end date YYYY-MM-DD (informational — used in batch_id)",
        ),
        "target": Param(
            default="all",
            type="string",
            enum=["redshift", "snowflake", "bigquery", "all"],
            description="Warehouse target (only used when layer=warehouse)",
        ),
        "fail_fast": Param(
            default=False,
            type="boolean",
            description="Abort the entire backfill on the first domain failure",
        ),
    },
    tags=["backfill", "manual", "ops"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    validate = PythonOperator(
        task_id="validate_and_build_args",
        python_callable=_build_args,
    )

    run = PythonOperator(
        task_id="run_backfill",
        python_callable=_run_backfill,
    )

    start >> validate >> run >> end
