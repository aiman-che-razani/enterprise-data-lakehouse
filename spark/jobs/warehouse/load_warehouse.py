"""
Warehouse load job entry point.

Usage:
    spark-submit spark/jobs/warehouse/load_warehouse.py --target redshift --all
    spark-submit spark/jobs/warehouse/load_warehouse.py --target snowflake --dims-only
    spark-submit spark/jobs/warehouse/load_warehouse.py --target bigquery --table fact_sales
    spark-submit spark/jobs/warehouse/load_warehouse.py --target all --all

Connector JARs (add to spark-submit --packages):
  Redshift:  io.github.spark_redshift_community:spark-redshift_2.12:5.1.0
             com.amazon.redshift:redshift-jdbc42:2.1.0.29
  Snowflake: net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5
             net.snowflake:snowflake-jdbc:3.14.5
  BigQuery:  com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0

Exit codes:
    0 — all tables loaded successfully
    1 — one or more tables failed
    2 — argument error
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from typing import Any

from src.common.config import Settings
from src.common.logging import configure_logging, get_logger
from src.loading.warehouse_config import WarehouseSettings
from src.processing.bronze.spark_session import create_bronze_spark_session
from src.processing.gold.domain_meta import GOLD_DIM_ORDER, GOLD_FACT_ORDER

logger = get_logger(__name__)

_TARGETS = ("redshift", "snowflake", "bigquery", "all")

_ALL_TABLES = GOLD_DIM_ORDER + GOLD_FACT_ORDER


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RetailNexus warehouse load — Gold Delta → Redshift / Snowflake / BigQuery"
    )
    parser.add_argument(
        "--target",
        required=True,
        choices=_TARGETS,
        help="Warehouse target(s) to load",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--table",
        choices=_ALL_TABLES,
        help="Single Gold table to load",
    )
    group.add_argument("--all", action="store_true", help="Load all 14 tables")
    group.add_argument("--dims-only", action="store_true", help="Load 8 dimension tables")
    group.add_argument("--facts-only", action="store_true", help="Load 6 fact tables")
    parser.add_argument(
        "--batch-id",
        default=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S"),
    )
    parser.add_argument("--fail-fast", action="store_true", default=False)
    return parser.parse_args(argv)


def _resolve_tables(args: argparse.Namespace) -> list[str]:
    if args.table:
        return [args.table]
    if args.dims_only:
        return GOLD_DIM_ORDER
    if args.facts_only:
        return GOLD_FACT_ORDER
    return _ALL_TABLES


def _resolve_targets(target: str):
    from src.loading.bigquery.bigquery_loader import BigQueryLoader
    from src.loading.redshift.redshift_loader import RedshiftLoader
    from src.loading.snowflake.snowflake_loader import SnowflakeLoader

    mapping = {
        "redshift": RedshiftLoader,
        "snowflake": SnowflakeLoader,
        "bigquery": BigQueryLoader,
    }
    if target == "all":
        return list(mapping.values())
    return [mapping[target]]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging()

    settings = Settings()
    wh_settings = WarehouseSettings()
    tables = _resolve_tables(args)
    loader_classes = _resolve_targets(args.target)

    spark = create_bronze_spark_session(
        app_name=f"RetailNexus-Warehouse-{args.target}-{args.batch_id}",
        settings=settings,
    )

    all_results: list[dict[str, Any]] = []
    exit_code = 0

    for loader_cls in loader_classes:
        loader = loader_cls(spark=spark, settings=settings, wh_settings=wh_settings)
        logger.info("warehouse_load_start", target=loader.target, tables=tables)

        for table in tables:
            result = loader.load_table(table, batch_id=args.batch_id)
            all_results.append(result)
            if result["status"] == "failed":
                exit_code = 1
                if args.fail_fast:
                    break

        if exit_code and args.fail_fast:
            break

    spark.stop()

    summary = {
        "batch_id": args.batch_id,
        "target": args.target,
        "tables_attempted": len(all_results),
        "success": sum(1 for r in all_results if r.get("status") == "success"),
        "failed": sum(1 for r in all_results if r.get("status") == "failed"),
        "total_rows": sum(r.get("rows", 0) for r in all_results),
        "results": all_results,
    }
    print(json.dumps(summary, default=str, indent=2))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
