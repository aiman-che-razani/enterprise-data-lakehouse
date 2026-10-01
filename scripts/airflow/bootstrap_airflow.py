"""
Airflow Bootstrap Script — idempotently creates Connections and Variables.

Run once after airflow db init / airflow db migrate:
    python scripts/airflow/bootstrap_airflow.py

Environment variables consumed (same as .env.example):
  SPARK_MASTER            default: spark://spark-master:7077
  POSTGRES_HOST           default: postgres
  POSTGRES_PORT           default: 5432
  POSTGRES_DB             default: retailnexus
  POSTGRES_USER           default: pipeline_user
  POSTGRES_PASSWORD       (required)
  MINIO_ENDPOINT          default: http://minio:9000
  MINIO_ACCESS_KEY        default: minioadmin
  MINIO_SECRET_KEY        (required)
  AWS_ACCESS_KEY_ID       (optional — for real S3)
  AWS_SECRET_ACCESS_KEY   (optional)
  AWS_DEFAULT_REGION      default: us-east-1
  S3_BUCKET               default: retailnexus-datalake
  AIRFLOW_ADMIN_EMAIL     default: data-engineering@retailnexus.com
  SMTP_HOST               (optional — for email alerts)
  SMTP_PORT               default: 587
  SMTP_USER               (optional)
  SMTP_PASSWORD           (optional)
"""

from __future__ import annotations

import argparse
import os
import sys


# ─────────────────────────────────────────────────────────────────────────────
# Connection definitions
# ─────────────────────────────────────────────────────────────────────────────
def _connections() -> list[dict]:
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_db = os.getenv("POSTGRES_DB", "retailnexus")
    pg_user = os.getenv("POSTGRES_USER", "pipeline_user")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "")

    # The Spark provider joins host and port itself, so the master URL must not carry the port.
    spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077").rsplit(":", 1)[0]
    spark_ui = os.getenv("SPARK_MASTER_UI", "http://spark-master:8080")

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "")

    aws_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")

    return [
        {
            "conn_id": "spark_default",
            "conn_type": "spark",
            "host": spark_master,
            "port": 7077,
            "extra": '{"queue": "default", "deploy-mode": "client"}',
        },
        {
            # Spark master REST API (stream health checks in bronze_streaming_dag)
            "conn_id": "spark_master",
            "conn_type": "http",
            "host": spark_ui,
        },
        {
            "conn_id": "postgres_pipeline",
            "conn_type": "postgres",
            "host": pg_host,
            "port": pg_port,
            "schema": pg_db,
            "login": pg_user,
            "password": pg_pass,
        },
        {
            "conn_id": "postgres_default",
            "conn_type": "postgres",
            "host": pg_host,
            "port": pg_port,
            "schema": pg_db,
            "login": pg_user,
            "password": pg_pass,
        },
        {
            "conn_id": "minio_default",
            "conn_type": "aws",
            "login": minio_access_key,
            "password": minio_secret_key,
            "extra": (f'{{"endpoint_url": "{minio_endpoint}", ' f'"region_name": "{aws_region}"}}'),
        },
        {
            "conn_id": "aws_default",
            "conn_type": "aws",
            "login": aws_key,
            "password": aws_secret,
            "extra": f'{{"region_name": "{aws_region}"}}',
        },
        *(
            [
                {
                    "conn_id": "smtp_default",
                    "conn_type": "smtp",
                    "host": smtp_host,
                    "port": smtp_port,
                    "login": smtp_user,
                    "password": smtp_pass,
                    "extra": '{"ssl": false, "starttls": true}',
                }
            ]
            if smtp_host
            else []
        ),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Variable definitions
# ─────────────────────────────────────────────────────────────────────────────
def _variables() -> dict[str, str]:
    s3_bucket = os.getenv("S3_BUCKET", "retailnexus-datalake")
    s3_prefix = f"s3a://{s3_bucket}"
    admin_email = os.getenv("AIRFLOW_ADMIN_EMAIL", "data-engineering@retailnexus.com")

    return {
        # Lake paths
        "BRONZE_PATH": f"{s3_prefix}/bronze",
        "SILVER_PATH": f"{s3_prefix}/silver",
        "GOLD_PATH": f"{s3_prefix}/gold",
        "DLQ_PATH": f"{s3_prefix}/dlq",
        "CHECKPOINTS_PATH": f"{s3_prefix}/checkpoints",
        # Pipeline settings
        "PIPELINE_VERSION": "1.0.0",
        "SPARK_MASTER": os.getenv("SPARK_MASTER", "spark://spark-master:7077"),
        "DEFAULT_BATCH_HOURS": "24",
        "DQ_MIN_PASS_RATE": "0.90",
        # Alert settings
        "ALERT_EMAIL": admin_email,
        "SLA_MISS_ALERT_HOURS": "6",
        # Domain lists (comma-separated, consumed by ops scripts)
        "ALL_DOMAINS": (
            "customers,products,suppliers,stores,employees,"
            "orders,order_lines,payments,shipments,returns,"
            "inventory,reviews,clickstream,iot_sensors,finance,campaigns"
        ),
        "GOLD_DIMS": "dim_date,dim_location,dim_supplier,dim_store,dim_employee,dim_product,dim_customer,dim_promotion",
        "GOLD_FACTS": "fact_sales,fact_inventory,fact_marketing,fact_returns,fact_shipments,fact_finance",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap helpers
# ─────────────────────────────────────────────────────────────────────────────
def bootstrap_connections(skip_existing: bool = False) -> None:
    from airflow import settings as af_settings
    from airflow.models import Connection

    session = af_settings.Session()
    try:
        for conn_def in _connections():
            conn_id = conn_def["conn_id"]
            exists = session.query(Connection).filter_by(conn_id=conn_id).first()
            if exists:
                if skip_existing:
                    print(f"  [SKIP]    connection '{conn_id}' already exists")
                    continue
                session.delete(exists)
                session.commit()

            conn = Connection(
                conn_id=conn_id,
                conn_type=conn_def.get("conn_type", "generic"),
                host=conn_def.get("host", ""),
                port=conn_def.get("port"),
                schema=conn_def.get("schema", ""),
                login=conn_def.get("login", ""),
                password=conn_def.get("password", ""),
            )
            if "extra" in conn_def:
                conn.set_extra(conn_def["extra"])
            session.add(conn)
            session.commit()
            print(f"  [CREATED] connection '{conn_id}'")
    finally:
        session.close()


def bootstrap_variables(skip_existing: bool = False) -> None:
    from airflow.models import Variable

    for key, value in _variables().items():
        existing = Variable.get(key, default_var=None)
        if existing is not None and skip_existing:
            print(f"  [SKIP]    variable '{key}' already exists")
            continue
        Variable.set(key, value)
        print(f"  [SET]     variable '{key}'")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bootstrap Airflow connections and variables for RetailNexus"
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=False,
        help="Do not overwrite connections/variables that already exist",
    )
    parser.add_argument(
        "--connections-only",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--variables-only",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()

    print("\n=== RetailNexus Airflow Bootstrap ===\n")

    if not args.variables_only:
        print("Connections:")
        bootstrap_connections(skip_existing=args.skip_existing)

    if not args.connections_only:
        print("\nVariables:")
        bootstrap_variables(skip_existing=args.skip_existing)

    print("\nDone.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
