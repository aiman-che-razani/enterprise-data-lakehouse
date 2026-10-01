"""
PySpark entry point for Bronze batch ingestion.

Submitted via spark-submit (standalone) or SparkSubmitOperator (Airflow).

Usage:
    spark-submit \\
        --master spark://spark-master:7077 \\
        --packages io.delta:delta-spark_2.12:3.2.0 \\
        spark/jobs/bronze/ingest_bronze.py \\
        [--domain orders] [--all] [--raw-dir datasets/raw] [--batch-id <uuid>]

Environment:
    SCALE_FACTOR, AWS_*, POSTGRES_*, KAFKA_* — sourced from .env / Secrets Manager.
"""

from __future__ import annotations

import argparse
import sys
import time

from src.common.config.settings import get_settings
from src.common.logging import get_logger, setup_logging
from src.processing.bronze.batch_ingestor import BronzeBatchIngestor
from src.processing.bronze.domain_meta import BATCH_INGESTION_ORDER
from src.processing.bronze.spark_session import create_bronze_spark_session


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ingest_bronze",
        description="RetailNexus Bronze batch ingestion job.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--domain",
        choices=BATCH_INGESTION_ORDER,
        help="Single domain to ingest.",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Ingest all domains in dependency order.",
    )
    parser.add_argument(
        "--raw-dir",
        default=None,
        help="Root of the raw source files, a path or URI (default: s3a://<bucket>/raw with object "
        "storage configured, else datasets/raw).",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Exit 0 even if no rows were ingested (default: an ingest that reads nothing fails).",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Batch UUID for idempotency tracking (generated if omitted).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    setup_logging(service_name="bronze-ingestor")
    log = get_logger(__name__)
    args = parse_args(argv)
    settings = get_settings()

    t0 = time.perf_counter()
    log.info(
        "bronze_job_starting",
        domain=args.domain if not args.all else "ALL",
        raw_dir=args.raw_dir or settings.raw_path,
        environment=settings.environment,
    )

    spark = create_bronze_spark_session(
        app_name=(f"RetailNexus-Bronze-{args.domain.upper() if args.domain else 'ALL'}")
    )

    ingestor = BronzeBatchIngestor(
        spark=spark,
        settings=settings,
        raw_data_dir=args.raw_dir,
        batch_id=args.batch_id,
    )

    try:
        if args.all:
            results = ingestor.ingest_all()
        else:
            result = ingestor.ingest_domain(args.domain)
            results = {args.domain: result}
    finally:
        spark.stop()

    # Summary
    elapsed = time.perf_counter() - t0
    failed = [d for d, r in results.items() if r.get("status") == "failed"]
    total_rows = sum(r.get("rows_written", 0) for r in results.values())

    log.info(
        "bronze_job_complete",
        elapsed_s=round(elapsed, 2),
        domains_processed=len(results),
        total_rows_written=total_rows,
        failed_domains=failed,
    )

    if failed:
        log.error("bronze_job_partial_failure", failed=failed)
        return 1
    if total_rows == 0 and not args.allow_empty:
        # Raw files are re-read in full on every run, so "nothing read" means the source is missing
        # or misconfigured - not "no new data". Failing here beats a silently green, empty run.
        log.error("bronze_job_ingested_nothing", raw_dir=args.raw_dir or settings.raw_path)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
