"""
Layer-aware Delta maintenance — OPTIMIZE (+ ZORDER) and VACUUM for Bronze, Silver or Gold.

Usage:
    spark-submit spark/jobs/maintenance/delta_maintenance.py --layer silver --all
    spark-submit spark/jobs/maintenance/delta_maintenance.py --layer gold --table fact_sales
    spark-submit spark/jobs/maintenance/delta_maintenance.py --layer bronze --all --vacuum-hours 168
    spark-submit spark/jobs/maintenance/delta_maintenance.py --layer gold --all --skip-vacuum

Exit codes: 0 = every table maintained (absent tables are skipped), 1 = at least one failure.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from src.common.config.settings import get_settings
from src.common.logging import get_logger, setup_logging
from src.processing.bronze.domain_meta import DOMAIN_ZORDER
from src.processing.bronze.spark_session import create_bronze_spark_session
from src.processing.gold.domain_meta import FACT_ZORDER_COLS, GOLD_DIM_ORDER, GOLD_FACT_ORDER
from src.processing.silver.domain_meta import SILVER_PROCESSING_ORDER

# VACUUM retention per layer. Bronze is the immutable 7-year record: BronzeBatchIngestor sets
# delta.deletedFileRetentionDuration = 7 years, and time travel needs those files, so VACUUM must
# never reach inside that window (a 7-day VACUUM would silently destroy the history). The
# rebuildable layers keep three days.
BRONZE_RETENTION_HOURS = 7 * 365 * 24 + 2 * 24  # 7 years (+2 leap days)
DEFAULT_VACUUM_HOURS = {"bronze": BRONZE_RETENTION_HOURS, "silver": 72, "gold": 72}
MIN_VACUUM_HOURS = {"bronze": BRONZE_RETENTION_HOURS}


def tables_for(layer: str) -> list[str]:
    if layer == "gold":
        return list(GOLD_DIM_ORDER) + list(GOLD_FACT_ORDER)
    return list(SILVER_PROCESSING_ORDER)


def zorder_cols(layer: str, table: str) -> list[str]:
    if layer == "bronze":
        return DOMAIN_ZORDER.get(table, [])
    if layer == "gold":
        return FACT_ZORDER_COLS.get(table, [])
    return []


def maintain_table(
    spark: Any, layer: str, table: str, path: str, vacuum_hours: int | None
) -> dict[str, Any]:
    log = get_logger(__name__)
    start = time.perf_counter()
    try:
        spark.read.format("delta").load(path).limit(0).collect()
    except Exception as exc:  # — table not created yet
        log.info("maintenance_table_absent", layer=layer, table=table, error=type(exc).__name__)
        return {"table": table, "status": "absent"}

    try:
        cols = zorder_cols(layer, table)
        clause = f" ZORDER BY ({', '.join(cols)})" if cols else ""
        spark.sql(f"OPTIMIZE delta.`{path}`{clause}")
        if vacuum_hours is not None:
            spark.sql(f"VACUUM delta.`{path}` RETAIN {vacuum_hours} HOURS")
        elapsed = round(time.perf_counter() - start, 2)
        log.info(
            "maintenance_done",
            layer=layer,
            table=table,
            elapsed_s=elapsed,
            zorder=cols,
            vacuum_hours=vacuum_hours,
        )
        return {"table": table, "status": "success", "elapsed_s": elapsed}
    except Exception as exc:
        log.error("maintenance_failed", layer=layer, table=table, error=str(exc))
        return {"table": table, "status": "failed", "error": str(exc)}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Delta OPTIMIZE + VACUUM for one medallion layer.")
    p.add_argument("--layer", required=True, choices=("bronze", "silver", "gold"))
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", help="single table/domain name")
    group.add_argument("--all", action="store_true")
    p.add_argument(
        "--vacuum-hours", type=int, default=None, help="retention window (default depends on layer)"
    )
    p.add_argument("--skip-vacuum", action="store_true", help="OPTIMIZE only")
    p.add_argument(
        "--vacuum",
        action="store_true",
        help="accepted for compatibility; VACUUM runs unless --skip-vacuum",
    )
    args = p.parse_args(argv)
    if args.table and args.table not in tables_for(args.layer):
        p.error(f"unknown {args.layer} table {args.table!r}")
    floor = MIN_VACUUM_HOURS.get(args.layer)
    if (
        floor
        and args.vacuum_hours is not None
        and args.vacuum_hours < floor
        and not args.skip_vacuum
    ):
        p.error(
            f"--vacuum-hours {args.vacuum_hours} is below the {args.layer} retention of {floor}h (7 years); "
            "shorter would destroy time-travel history"
        )
    return args


def main(argv: list[str] | None = None) -> int:
    setup_logging(service_name="delta-maintenance")
    log = get_logger(__name__)
    args = parse_args(argv)
    settings = get_settings()

    tables = tables_for(args.layer) if args.all else [args.table]
    hours = None if args.skip_vacuum else (args.vacuum_hours or DEFAULT_VACUUM_HOURS[args.layer])

    spark = create_bronze_spark_session(app_name=f"RetailNexus-{args.layer.title()}-Maintenance")
    try:
        results = [
            maintain_table(spark, args.layer, t, settings.s3_path(args.layer, t), hours)
            for t in tables
        ]
    finally:
        spark.stop()

    failed = [r["table"] for r in results if r["status"] == "failed"]
    log.info("maintenance_complete", layer=args.layer, tables=len(tables), failed=failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
