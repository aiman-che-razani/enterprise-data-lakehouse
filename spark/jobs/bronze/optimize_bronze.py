"""
PySpark maintenance job — OPTIMIZE + VACUUM all Bronze Delta tables.

Run on a separate schedule (weekly) after ingestion completes.
OPTIMIZE compacts small files; VACUUM removes files older than retention window.

Usage:
    spark-submit spark/jobs/bronze/optimize_bronze.py \\
        [--domain orders] [--all] [--vacuum-hours 61368]
"""

from __future__ import annotations

import argparse
import sys
import time

from src.common.config.settings import get_settings
from src.common.logging import get_logger, setup_logging
from src.processing.bronze.domain_meta import BATCH_INGESTION_ORDER, DOMAIN_ZORDER
from src.processing.bronze.spark_session import create_bronze_spark_session


def optimize_domain(spark, domain: str, bronze_path: str) -> dict:
    """Run OPTIMIZE + ZORDER then return statistics."""
    log = get_logger(__name__)
    zorder_cols = DOMAIN_ZORDER.get(domain, [])
    t0 = time.perf_counter()

    try:
        if zorder_cols:
            zorder_clause = ", ".join(zorder_cols)
            spark.sql(f"OPTIMIZE delta.`{bronze_path}` ZORDER BY ({zorder_clause})")
        else:
            spark.sql(f"OPTIMIZE delta.`{bronze_path}`")
        elapsed = round(time.perf_counter() - t0, 2)
        log.info("optimize_done", domain=domain, elapsed_s=elapsed, zorder=zorder_cols)
        return {"status": "success", "elapsed_s": elapsed}
    except Exception as exc:
        log.error("optimize_failed", domain=domain, error=str(exc))
        return {"status": "failed", "error": str(exc)}


def vacuum_domain(spark, domain: str, bronze_path: str, retain_hours: int) -> dict:
    """Run VACUUM to remove files older than retain_hours."""
    log = get_logger(__name__)
    try:
        spark.sql(f"VACUUM delta.`{bronze_path}` RETAIN {retain_hours} HOURS")
        log.info("vacuum_done", domain=domain, retain_hours=retain_hours)
        return {"status": "success"}
    except Exception as exc:
        log.error("vacuum_failed", domain=domain, error=str(exc))
        return {"status": "failed", "error": str(exc)}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze Delta maintenance (OPTIMIZE + VACUUM).")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--domain", choices=BATCH_INGESTION_ORDER)
    group.add_argument("--all", action="store_true")
    parser.add_argument(
        "--vacuum-hours",
        type=int,
        default=7 * 365 * 24
        + 48,  # 7 years: matches delta.deletedFileRetentionDuration set at ingest
        help="Retain files newer than this many hours (default: 61,368 = 7 years, the Bronze retention).",
    )
    parser.add_argument("--skip-vacuum", action="store_true", help="Run OPTIMIZE only.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    setup_logging(service_name="bronze-optimizer")
    log = get_logger(__name__)
    args = parse_args(argv)
    settings = get_settings()

    domains = BATCH_INGESTION_ORDER if args.all else [args.domain]
    spark = create_bronze_spark_session(app_name="RetailNexus-Bronze-Optimize")

    try:
        for domain in domains:
            bronze_path = settings.s3_path("bronze", domain)
            optimize_domain(spark, domain, bronze_path)
            if not args.skip_vacuum:
                vacuum_domain(spark, domain, bronze_path, args.vacuum_hours)
    finally:
        spark.stop()

    log.info("maintenance_complete", domains=domains)
    return 0


if __name__ == "__main__":
    sys.exit(main())
