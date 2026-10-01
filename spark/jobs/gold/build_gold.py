"""
Gold batch build job entry point.

Usage:
    spark-submit spark/jobs/gold/build_gold.py --table dim_customer
    spark-submit spark/jobs/gold/build_gold.py --dims-only
    spark-submit spark/jobs/gold/build_gold.py --facts-only
    spark-submit spark/jobs/gold/build_gold.py --all

Build order is always dims first (facts join to dim SKs).

Exit codes:
    0 — all tables succeeded
    1 — one or more tables failed
    2 — argument error
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from datetime import UTC, datetime
from typing import Any

from src.common.config import Settings
from src.common.logging import configure_logging, get_logger
from src.processing.bronze.spark_session import create_bronze_spark_session
from src.processing.gold.domain_meta import GOLD_DIM_ORDER, GOLD_FACT_ORDER

logger = get_logger(__name__)

_BUILDER_REGISTRY: dict[str, str] = {
    # Dimensions
    "dim_date": "src.processing.gold.dimensions.dim_date.DimDateBuilder",
    "dim_location": "src.processing.gold.dimensions.dim_location.DimLocationBuilder",
    "dim_supplier": "src.processing.gold.dimensions.dim_supplier.DimSupplierBuilder",
    "dim_store": "src.processing.gold.dimensions.dim_store.DimStoreBuilder",
    "dim_employee": "src.processing.gold.dimensions.dim_employee.DimEmployeeBuilder",
    "dim_product": "src.processing.gold.dimensions.dim_product.DimProductBuilder",
    "dim_customer": "src.processing.gold.dimensions.dim_customer.DimCustomerBuilder",
    "dim_promotion": "src.processing.gold.dimensions.dim_promotion.DimPromotionBuilder",
    # Facts
    "fact_sales": "src.processing.gold.facts.fact_sales.FactSalesBuilder",
    "fact_inventory": "src.processing.gold.facts.fact_inventory.FactInventoryBuilder",
    "fact_marketing": "src.processing.gold.facts.fact_marketing.FactMarketingBuilder",
    "fact_returns": "src.processing.gold.facts.fact_returns.FactReturnsBuilder",
    "fact_shipments": "src.processing.gold.facts.fact_shipments.FactShipmentsBuilder",
    "fact_finance": "src.processing.gold.facts.fact_finance.FactFinanceBuilder",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RetailNexus Gold star-schema builder")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", choices=list(_BUILDER_REGISTRY), help="Single table to build")
    group.add_argument("--all", action="store_true", help="Build all dims then all facts")
    group.add_argument("--dims-only", action="store_true", help="Build dimension tables only")
    group.add_argument("--facts-only", action="store_true", help="Build fact tables only")
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
    return GOLD_DIM_ORDER + GOLD_FACT_ORDER  # --all


def _load_builder(dotted_path: str):
    module_path, class_name = dotted_path.rsplit(".", 1)
    return getattr(importlib.import_module(module_path), class_name)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = Settings()
    tables = _resolve_tables(args)

    spark = create_bronze_spark_session(
        app_name=f"RetailNexus-Gold-{args.batch_id}",
        settings=settings,
    )

    results: list[dict[str, Any]] = []
    exit_code = 0

    for table in tables:
        builder_cls = _load_builder(_BUILDER_REGISTRY[table])
        builder = builder_cls(spark=spark, settings=settings)
        logger.info("gold_table_start", table=table, batch_id=args.batch_id)
        try:
            result = builder.build(batch_id=args.batch_id)
            results.append(result)
            if result.get("status") == "failed":
                exit_code = 1
                if args.fail_fast:
                    break
        except Exception as exc:
            logger.error("gold_table_exception", table=table, error=str(exc))
            results.append({"table": table, "status": "failed", "error": str(exc)})
            exit_code = 1
            if args.fail_fast:
                break

    spark.stop()

    summary = {
        "batch_id": args.batch_id,
        "tables_processed": len(results),
        "success": sum(1 for r in results if r.get("status") == "success"),
        "failed": sum(1 for r in results if r.get("status") == "failed"),
        "total_rows_written": sum(r.get("rows_written", 0) for r in results),
        "results": results,
    }
    print(json.dumps(summary, default=str, indent=2))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
