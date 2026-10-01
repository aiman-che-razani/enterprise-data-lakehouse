"""
Silver batch processing job entry point.

Usage:
    spark-submit spark/jobs/silver/process_silver.py --domain customers
    spark-submit spark/jobs/silver/process_silver.py --all
    spark-submit spark/jobs/silver/process_silver.py --all --batch-id 2026-07-04T02:00:00

Exit codes:
    0  — all domains succeeded
    1  — one or more domains failed (check logs for details)
    2  — argument error
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from typing import Any

from src.common.config import Settings
from src.common.logging import configure_logging, get_logger
from src.processing.bronze.spark_session import create_bronze_spark_session
from src.processing.silver.domain_meta import SILVER_PROCESSING_ORDER

logger = get_logger(__name__)

_PROCESSOR_REGISTRY: dict[str, str] = {
    "customers": "src.processing.silver.domains.customers.CustomersSilverProcessor",
    "products": "src.processing.silver.domains.products.ProductsSilverProcessor",
    "suppliers": "src.processing.silver.domains.suppliers.SuppliersSilverProcessor",
    "stores": "src.processing.silver.domains.stores.StoresSilverProcessor",
    "employees": "src.processing.silver.domains.employees.EmployeesSilverProcessor",
    "orders": "src.processing.silver.domains.orders.OrdersSilverProcessor",
    "order_lines": "src.processing.silver.domains.order_lines.OrderLinesSilverProcessor",
    "payments": "src.processing.silver.domains.payments.PaymentsSilverProcessor",
    "shipments": "src.processing.silver.domains.shipments.ShipmentsSilverProcessor",
    "returns": "src.processing.silver.domains.returns.ReturnsSilverProcessor",
    "inventory": "src.processing.silver.domains.inventory.InventorySilverProcessor",
    "reviews": "src.processing.silver.domains.reviews.ReviewsSilverProcessor",
    "clickstream": "src.processing.silver.domains.clickstream.ClickstreamSilverProcessor",
    "iot_sensors": "src.processing.silver.domains.iot_sensors.IotSensorsSilverProcessor",
    "finance": "src.processing.silver.domains.finance.FinanceSilverProcessor",
    "campaigns": "src.processing.silver.domains.campaigns.CampaignsSilverProcessor",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RetailNexus Silver batch processor")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--domain", choices=list(_PROCESSOR_REGISTRY), help="Single domain to process"
    )
    group.add_argument("--all", action="store_true", help="Process all domains in dependency order")
    parser.add_argument(
        "--batch-id",
        default=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S"),
        help="Batch identifier injected into Silver metadata (default: current UTC timestamp)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        default=False,
        help="Abort immediately on first domain failure instead of continuing",
    )
    return parser.parse_args(argv)


def _load_processor_class(dotted_path: str):
    module_path, class_name = dotted_path.rsplit(".", 1)
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = Settings()

    domains = SILVER_PROCESSING_ORDER if args.all else [args.domain]

    spark = create_bronze_spark_session(
        app_name=f"RetailNexus-Silver-{'all' if args.all else args.domain}",
        settings=settings,
    )

    results: list[dict[str, Any]] = []
    exit_code = 0

    for domain in domains:
        processor_path = _PROCESSOR_REGISTRY[domain]
        processor_cls = _load_processor_class(processor_path)
        processor = processor_cls(spark=spark, settings=settings)

        logger.info("silver_domain_start", domain=domain, batch_id=args.batch_id)
        try:
            result = processor.process(batch_id=args.batch_id)
            results.append(result)
            status = result.get("status", "unknown")
            logger.info(
                "silver_domain_done",
                domain=domain,
                status=status,
                **{k: v for k, v in result.items() if k not in ("domain", "status")},
            )
            if status == "failed":
                exit_code = 1
                if args.fail_fast:
                    break
        except Exception as exc:
            logger.error("silver_domain_exception", domain=domain, error=str(exc))
            results.append({"domain": domain, "status": "failed", "error": str(exc)})
            exit_code = 1
            if args.fail_fast:
                break

    spark.stop()

    # Print summary as JSON for Airflow to parse from task logs
    summary = {
        "batch_id": args.batch_id,
        "domains_processed": len(results),
        "success": sum(1 for r in results if r.get("status") == "success"),
        "no_new_data": sum(1 for r in results if r.get("status") == "no_new_data"),
        "failed": sum(1 for r in results if r.get("status") == "failed"),
        "results": results,
    }
    print(json.dumps(summary, default=str, indent=2))

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
