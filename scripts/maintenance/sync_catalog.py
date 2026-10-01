"""
Reflect the Delta tables on storage into the catalog and refresh the lineage graph.

Usage:
    python scripts/maintenance/sync_catalog.py                    # all layers
    python scripts/maintenance/sync_catalog.py --layers gold      # one layer
    python scripts/maintenance/sync_catalog.py --count-rows       # also store exact row counts
    python scripts/maintenance/sync_catalog.py --lineage-only     # no Spark needed

Exit codes: 0 = ok (missing tables are reported, not fatal), 1 = unexpected error.
"""

from __future__ import annotations

import argparse
import json
import sys

from src.common.config import Settings
from src.common.logging import configure_logging, get_logger

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--layers",
        nargs="+",
        choices=("bronze", "silver", "gold"),
        default=["bronze", "silver", "gold"],
    )
    p.add_argument(
        "--count-rows", action="store_true", help="run count() per table (slow on large tables)"
    )
    p.add_argument("--lineage-only", action="store_true", help="only refresh catalog.lineage")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = Settings()
    summary: dict[str, object] = {}

    from src.lineage.tracker import LineageTracker

    summary["lineage_edges"] = LineageTracker(settings).sync_static()

    if not args.lineage_only:
        from src.catalog import CatalogSynchronizer
        from src.processing.bronze.spark_session import create_bronze_spark_session

        spark = create_bronze_spark_session(app_name="RetailNexus-CatalogSync", settings=settings)
        try:
            summary.update(
                CatalogSynchronizer(spark, settings, count_rows=args.count_rows).sync_all(
                    args.layers
                )
            )
        finally:
            spark.stop()

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
