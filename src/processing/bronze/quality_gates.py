"""
Bronze quality gates — first-pass validation at ingestion time.

These are lightweight structural checks only:
  1. Corrupt record detection (rows that failed schema parsing)
  2. Primary key null check (PKs must never be null)
  3. Minimum record count (alert if batch is suspiciously small)
  4. Schema completeness (required fields must have < 5% nulls)

Silver-layer quality runs deeper domain-specific business rules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.logging import get_logger
from src.processing.bronze.domain_meta import DOMAIN_PKS

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

logger = get_logger(__name__)

# Required columns that must have < MAX_NULL_PCT null rate
_REQUIRED_COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "email", "segment", "country"],
    "orders": ["order_id", "customer_id", "order_date", "total_amount"],
    "order_lines": ["order_line_id", "order_id", "sku", "quantity"],
    "payments": ["payment_id", "order_id", "amount", "status"],
    "products": ["sku", "name", "list_price"],
    "shipments": ["shipment_id", "order_id", "carrier", "tracking_number"],
    "returns": ["return_id", "order_id", "customer_id", "reason"],
    "inventory": ["inventory_id", "sku", "store_id", "quantity_on_hand"],
    "clickstream": ["event_id", "session_id", "event_type", "page_url"],
    "iot_sensors": ["reading_id", "sensor_id", "store_id", "value"],
    "finance": ["transaction_id", "debit_amount", "credit_amount"],
    "reviews": ["review_id", "customer_id", "sku", "rating"],
}

_MAX_NULL_PCT = 0.05  # 5% null tolerance at Bronze
_MIN_BATCH_ROWS = 10  # Alert threshold for suspiciously small batches


class QualityResult:
    """Holds pass/fail status and metrics for a single quality check."""

    def __init__(self, name: str, passed: bool, metrics: dict[str, Any] | None = None) -> None:
        self.name = name
        self.passed = passed
        self.metrics = metrics or {}

    def to_dict(self) -> dict[str, Any]:
        return {"check": self.name, "passed": self.passed, **self.metrics}


class BronzeQualityGates:
    """Runs quality checks on a Bronze-bound DataFrame."""

    def split_valid_records(
        self,
        df: DataFrame,
        schema: StructType,
        domain: str,
    ) -> tuple[DataFrame, DataFrame | None]:
        """
        Split the DataFrame into (good_records, bad_records).

        Bad records are rows where _corrupt_record IS NOT NULL (set by
        Spark's PERMISSIVE mode when JSON/CSV fails to parse).
        """
        has_corrupt_col = "_corrupt_record" in df.columns

        if has_corrupt_col:
            # Spark refuses a query on a raw JSON/CSV read that references only the corrupt-record column
            # ("...disallowed when the referenced columns only include the internal corrupt record
            # column"). Parse once and cache; the count materialises it before the filters below.
            df = df.cache()
            df.count()
            bad_df = df.filter(F.col("_corrupt_record").isNotNull())
            good_df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
        else:
            bad_df = None
            good_df = df

        # Run structural checks (log warnings; don't fail the pipeline)
        self._check_pk_nulls(good_df, domain)
        self._check_null_rates(good_df, domain)
        self._check_min_rows(good_df, domain)

        return good_df, bad_df

    def run_all_checks(self, df: DataFrame, domain: str) -> list[QualityResult]:
        """Run all quality checks and return results (used in pipeline tests)."""
        results = []
        results.append(self._check_pk_nulls(df, domain))
        results.append(self._check_null_rates(df, domain))
        results.append(self._check_min_rows(df, domain))
        return results

    # ─── Individual checks ────────────────────────────────────────────────────

    def _check_pk_nulls(self, df: DataFrame, domain: str) -> QualityResult:
        pk = DOMAIN_PKS.get(domain)
        if not pk or pk not in df.columns:
            return QualityResult("pk_not_null", passed=True)

        null_count = df.filter(F.col(pk).isNull()).count()
        passed = null_count == 0
        if not passed:
            logger.warning("pk_null_check_failed", domain=domain, pk=pk, null_count=null_count)
        return QualityResult("pk_not_null", passed=passed, metrics={"null_count": null_count})

    def _check_null_rates(self, df: DataFrame, domain: str) -> QualityResult:
        required = _REQUIRED_COLUMNS.get(domain, [])
        if not required:
            return QualityResult("null_rates", passed=True)

        total = df.count()
        if total == 0:
            return QualityResult("null_rates", passed=False, metrics={"total_rows": 0})

        violations: dict[str, float] = {}
        for col_name in required:
            if col_name not in df.columns:
                continue
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_pct = null_count / total
            if null_pct > _MAX_NULL_PCT:
                violations[col_name] = round(null_pct, 4)

        passed = len(violations) == 0
        if not passed:
            logger.warning("null_rate_violations", domain=domain, violations=violations)
        return QualityResult("null_rates", passed=passed, metrics={"violations": violations})

    def _check_min_rows(self, df: DataFrame, domain: str) -> QualityResult:
        count = df.count()
        passed = count >= _MIN_BATCH_ROWS
        if not passed:
            logger.warning("suspiciously_small_batch", domain=domain, row_count=count)
        return QualityResult(
            "min_row_count",
            passed=passed,
            metrics={"row_count": count, "threshold": _MIN_BATCH_ROWS},
        )
