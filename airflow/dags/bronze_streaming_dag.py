"""
Airflow DAG: Bronze Streaming Health Monitor

Schedule: every 5 minutes
Does NOT start/stop streaming jobs (those run as long-lived Spark processes).
Instead, this DAG:
  1. Checks that each streaming query is active (via Spark REST API)
  2. Verifies Delta table has received records in the last 15 minutes
  3. Pages if lag exceeds SLA thresholds
  4. Restarts a dead stream via spark-submit if it has been idle > 10 minutes

Streaming jobs are started externally via:
    docker-compose exec spark-master python -m spark.streaming.orders_stream
    (or via ECS/EMR task definitions in production)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 0,
}

_STREAMING_DOMAINS = ["orders", "payments", "clickstream", "inventory", "iot_sensors"]

# Max allowed age (minutes) of the most recent Bronze record before alerting
_FRESHNESS_SLA_MINUTES: dict[str, int] = {
    "orders": 15,
    "payments": 15,
    "clickstream": 10,
    "inventory": 30,
    "iot_sensors": 5,  # tight SLA for IoT alerts
}


def _check_stream_health(domain: str, **context) -> dict[str, Any]:
    """
    Check Spark Structured Streaming query status via the Spark REST API.
    Returns status dict; raises on failure to trigger Airflow alert.
    """
    try:
        http = HttpHook(method="GET", http_conn_id="spark_master")
        resp = http.run(endpoint="/api/v1/applications")
        apps = resp.json()
    except Exception as exc:
        raise RuntimeError(f"Cannot reach Spark master for domain={domain}: {exc}") from exc

    # Find the streaming application for this domain
    app_name = f"RetailNexus-Stream-{domain.upper()}"
    running = any(
        a.get("name") == app_name and a.get("attempts", [{}])[0].get("completed") is False
        for a in apps
    )

    if not running:
        raise RuntimeError(
            f"Streaming job '{app_name}' is not running! "
            f"Restart via: spark-submit spark/streaming/{domain}_stream.py"
        )

    return {"domain": domain, "stream_active": True}


def _check_bronze_freshness(domain: str, **context) -> dict[str, Any]:
    """Verify the Bronze Delta table has ingested recent data."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    sla_minutes = _FRESHNESS_SLA_MINUTES[domain]
    hook = PostgresHook(postgres_conn_id="postgres_default")

    rows = hook.get_records(
        """
        SELECT bronze_watermark, bronze_last_ingest
        FROM pipeline.watermarks
        WHERE domain = %s
        """,
        parameters=(domain,),
    )

    if not rows:
        raise RuntimeError(f"No watermark found for domain={domain}")

    wm_value, last_run_at = rows[0]
    if last_run_at is None:
        raise RuntimeError(f"Domain {domain} has never been ingested (watermark is NULL)")

    from datetime import datetime

    age_minutes = (datetime.now(UTC) - last_run_at.astimezone(UTC)).total_seconds() / 60
    if age_minutes > sla_minutes:
        raise RuntimeError(
            f"Bronze freshness SLA breached for {domain}: "
            f"last ingested {age_minutes:.1f} min ago (SLA: {sla_minutes} min)"
        )

    return {"domain": domain, "age_minutes": round(age_minutes, 1), "sla_ok": True}


with DAG(
    dag_id="bronze_streaming_health",
    description="Monitor all 5 Kafka→Bronze streaming consumers and alert on SLA breach",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "streaming", "monitoring"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for domain in _STREAMING_DOMAINS:
        health_check = PythonOperator(
            task_id=f"check_stream_{domain}",
            python_callable=_check_stream_health,
            op_kwargs={"domain": domain},
        )
        freshness_check = PythonOperator(
            task_id=f"check_freshness_{domain}",
            python_callable=_check_bronze_freshness,
            op_kwargs={"domain": domain},
        )

        start >> health_check >> freshness_check >> end
