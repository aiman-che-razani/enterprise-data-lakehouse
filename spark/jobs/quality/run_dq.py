"""
Data Quality job entry point.

Usage:
    spark-submit spark/jobs/quality/run_dq.py --layer bronze
    spark-submit spark/jobs/quality/run_dq.py --layer silver --domains orders payments
    spark-submit spark/jobs/quality/run_dq.py --layer gold --domains fact_sales dim_date
    spark-submit spark/jobs/quality/run_dq.py --layer all
    spark-submit spark/jobs/quality/run_dq.py --layer silver --fail-fast

Exit codes:
    0 — all checks passed (or only warnings); with --fail-fast, also when only non-critical checks failed
    1 — one or more FAILED checks (without --fail-fast)
    2 — argument error
    3 — critical check failed and --fail-fast is set

--fail-fast makes the job a pipeline gate: only a CRITICAL failure (row count, uniqueness) stops the
run. Non-critical failures are still alerted on and stored in quality.reports, but must not block Gold.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

from src.common.config import Settings
from src.common.logging import configure_logging, get_logger
from src.processing.bronze.spark_session import create_bronze_spark_session
from src.quality.dq_runner import DQRunner

logger = get_logger(__name__)

_VALID_LAYERS = ("bronze", "silver", "gold", "all")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RetailNexus Data Quality — run checks for one or all layers"
    )
    parser.add_argument(
        "--layer",
        required=True,
        choices=_VALID_LAYERS,
        help="Medallion layer to check, or 'all' to run all three",
    )
    parser.add_argument(
        "--domains",
        nargs="+",
        metavar="DOMAIN",
        help="Specific domains/tables to check (default: all configured for the layer)",
    )
    parser.add_argument(
        "--batch-id",
        default=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S"),
        help="Batch identifier for traceability (default: current UTC timestamp)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        default=False,
        help="Exit with code 3 immediately on a critical check failure",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging()

    settings = Settings()
    spark = create_bronze_spark_session(
        app_name=f"RetailNexus-DQ-{args.layer}-{args.batch_id}",
        settings=settings,
    )

    runner = DQRunner(
        spark=spark,
        settings=settings,
        fail_on_critical=args.fail_fast,
    )

    try:
        if args.layer == "all":
            summary = runner.run_all_layers(batch_id=args.batch_id)
        else:
            summary = runner.run(
                layer=args.layer,
                domains=args.domains,
                batch_id=args.batch_id,
            )
    except RuntimeError as exc:
        # Critical check failed with --fail-fast
        logger.error("dq_critical_failure", error=str(exc))
        spark.stop()
        print(json.dumps({"error": str(exc)}, indent=2))
        return 3
    except Exception as exc:
        logger.error("dq_run_error", error=str(exc))
        spark.stop()
        print(json.dumps({"error": str(exc)}, indent=2))
        return 1

    spark.stop()
    print(json.dumps(summary, default=str, indent=2))

    # As a gate (--fail-fast) a critical failure already returned 3 above; anything else is reported, not blocking.
    if args.fail_fast:
        return 0
    overall = summary.get("overall", summary)
    return 1 if overall.get("failed", 0) > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
