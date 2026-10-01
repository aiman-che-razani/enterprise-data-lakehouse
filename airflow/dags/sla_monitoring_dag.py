"""
RetailNexus — SLA Monitoring DAG

Schedule: "*/30 * * * *"  (every 30 minutes — near-real-time freshness monitor)
Max active runs: 1

Checks three categories:
  1. Pipeline watermark freshness  — last ingest/process timestamps per domain
  2. DQ failure rate               — pass rate from quality.reports in last 24h
  3. Open SLA misses               — unacknowledged pipeline.sla_misses in last 6h

The dag_id "sla_monitor" surfaces in Airflow's SLA miss UI and can be wired
to an SNS/PagerDuty/Slack callback via on_failure_callback.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=5),
}

# Domains where a freshness breach is CRITICAL (should never exceed SLA)
_CRITICAL_DOMAINS = frozenset(
    {
        "clickstream",
        "iot_sensors",
        "orders",
        "payments",
    }
)

# Per-domain SLA thresholds (hours) used for watermark checks
_DOMAIN_SLA_HOURS: dict[str, float] = {
    "customers": 26.0,
    "products": 26.0,
    "suppliers": 26.0,
    "stores": 26.0,
    "employees": 48.0,
    "orders": 26.0,
    "order_lines": 26.0,
    "payments": 26.0,
    "shipments": 26.0,
    "returns": 48.0,
    "inventory": 26.0,
    "reviews": 48.0,
    "clickstream": 2.0,
    "iot_sensors": 2.0,
    "finance": 48.0,
    "campaigns": 48.0,
}

# Minimum DQ pass rate before alerting
_DQ_PASS_RATE_THRESHOLD = 0.90


def _check_watermark_freshness(**_context) -> None:
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
            ORDER BY age_hours DESC
            """
        )
        rows = cur.fetchall()
    conn.close()

    breaches: list[tuple[str, float, float]] = []
    for domain, _last_ingest, age_hours in rows:
        sla = _DOMAIN_SLA_HOURS.get(domain, 26.0)
        if age_hours > sla:
            breaches.append((domain, age_hours, sla))

    if breaches:
        print(f"\n[FRESHNESS BREACH] {len(breaches)} domain(s) exceeded SLA:")
        for domain, age, sla in breaches:
            icon = "CRITICAL" if domain in _CRITICAL_DOMAINS else "WARNING"
            print(f"  [{icon}] {domain:<20s}  age={age:.1f}h  SLA={sla}h")
        critical = [d for d, _, _ in breaches if d in _CRITICAL_DOMAINS]
        if critical:
            raise Exception(f"CRITICAL freshness breach: {', '.join(critical)} exceed SLA")
    else:
        print("[OK] All watermarks are within SLA.")


def _check_dq_pass_rate(**_context) -> None:
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
            SELECT layer,
                   ROUND(AVG(pass_rate)::numeric, 4) AS avg_pass_rate,
                   SUM(failed) AS total_failed
            FROM quality.reports
            WHERE run_at >= NOW() - INTERVAL '24 hours'
            GROUP BY layer
            ORDER BY avg_pass_rate ASC
            """
        )
        rows = cur.fetchall()
    conn.close()

    if not rows:
        print("[INFO] No DQ reports found in the last 24 hours.")
        return

    low_rate_layers = []
    for layer, pass_rate, total_failed in rows:
        rate = float(pass_rate)
        icon = "OK  " if rate >= _DQ_PASS_RATE_THRESHOLD else "WARN"
        print(f"  [{icon}] {layer:<8s}  pass_rate={rate:.1%}  failed_checks={total_failed}")
        if rate < _DQ_PASS_RATE_THRESHOLD:
            low_rate_layers.append((layer, rate))

    if low_rate_layers:
        summary = ", ".join(f"{layer}={rate:.1%}" for layer, rate in low_rate_layers)
        raise Exception(f"DQ pass rate below {_DQ_PASS_RATE_THRESHOLD:.0%} threshold: {summary}")


def _check_open_sla_misses(**_context) -> None:
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
            SELECT domain, layer, metric, actual_value, sla_threshold, message, detected_at
            FROM pipeline.sla_misses
            WHERE detected_at >= NOW() - INTERVAL '6 hours'
            ORDER BY detected_at DESC
            """
        )
        misses = cur.fetchall()
    conn.close()

    if misses:
        print(f"\n[SLA MISS] {len(misses)} SLA miss(es) in the last 6 hours:")
        for domain, layer, metric, actual, threshold, _msg, detected_at in misses:
            icon = "CRIT" if domain in _CRITICAL_DOMAINS else "WARN"
            print(
                f"  [{icon}] {domain:<20s}/{layer:<8s}  "
                f"metric={metric}  actual={actual}  sla={threshold}  "
                f"detected={detected_at}"
            )
        critical = [r for r in misses if r[0] in _CRITICAL_DOMAINS]
        if critical:
            raise Exception(f"{len(critical)} critical SLA miss(es) detected in the last 6 hours")
    else:
        print("[OK] No open SLA misses in the last 6 hours.")


with DAG(
    dag_id="sla_monitor",
    description="Every-30-min SLA monitor: freshness, DQ pass rate, open SLA misses",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["monitoring", "sla", "freshness", "dq", "ops"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    freshness_check = PythonOperator(
        task_id="watermark_freshness",
        python_callable=_check_watermark_freshness,
    )

    dq_check = PythonOperator(
        task_id="dq_pass_rate",
        python_callable=_check_dq_pass_rate,
    )

    sla_miss_check = PythonOperator(
        task_id="open_sla_misses",
        python_callable=_check_open_sla_misses,
    )

    # All checks run in parallel — a single breach should not block the others
    start >> [freshness_check, dq_check, sla_miss_check] >> end
