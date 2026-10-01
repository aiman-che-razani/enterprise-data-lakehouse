"""
Business-rule validation for the Silver layer.

Each domain has a set of constraints that must hold after cleansing.
Rows that violate constraints are routed to a per-domain Silver DLQ
rather than failing the entire batch.

Rules are expressed as Spark Column expressions (boolean predicates).
A row is VALID if ALL rules evaluate to True.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationRule:
    name: str
    # A Spark Column expression that returns True for VALID rows
    predicate: Any  # pyspark.sql.Column
    description: str = ""


# ─── Domain validation rules ─────────────────────────────────────────────────


def _build_rules() -> dict[str, list[ValidationRule]]:
    """Build the rule table on demand.

    ``F.col`` needs an active SparkContext, so the rules cannot be constructed at
    import time (that broke importing ``src.processing.silver`` without a session).
    """
    return {
        "orders": [
            ValidationRule(
                "total_positive", F.col("total_amount") > 0, "Total amount must be positive"
            ),
            ValidationRule(
                "status_valid",
                F.col("status").isin(
                    "confirmed", "processing", "shipped", "delivered", "cancelled", "returned"
                ),
                "Status must be a valid enum value",
            ),
            ValidationRule(
                "order_id_not_null", F.col("order_id").isNotNull(), "order_id is required"
            ),
            ValidationRule(
                "customer_id_not_null", F.col("customer_id").isNotNull(), "customer_id is required"
            ),
            ValidationRule(
                "subtotal_positive", F.col("subtotal") >= 0, "Subtotal must be non-negative"
            ),
        ],
        "order_lines": [
            ValidationRule("qty_positive", F.col("quantity") > 0, "Quantity must be positive"),
            ValidationRule(
                "unit_price_positive", F.col("unit_price") > 0, "Unit price must be positive"
            ),
            ValidationRule(
                "line_total_positive", F.col("line_total") >= 0, "Line total must be non-negative"
            ),
            ValidationRule(
                "order_id_not_null", F.col("order_id").isNotNull(), "order_id is required"
            ),
            ValidationRule("sku_not_null", F.col("sku").isNotNull(), "SKU is required"),
        ],
        "payments": [
            ValidationRule("amount_positive", F.col("amount") > 0, "Amount must be positive"),
            ValidationRule(
                "status_valid",
                F.col("status").isin("paid", "failed", "refunded", "partially_refunded", "pending"),
                "Payment status must be valid",
            ),
            ValidationRule(
                "refund_not_gt_amount",
                (F.col("refund_amount").isNull()) | (F.col("refund_amount") <= F.col("amount")),
                "Refund amount cannot exceed original amount",
            ),
        ],
        "customers": [
            ValidationRule(
                "segment_valid",
                F.col("segment").isin("platinum", "gold", "silver", "bronze"),
                "Segment must be valid",
            ),
            ValidationRule(
                "loyalty_pts_non_negative",
                F.col("loyalty_points") >= 0,
                "Loyalty points must be non-negative",
            ),
            ValidationRule(
                "customer_id_not_null", F.col("customer_id").isNotNull(), "customer_id required"
            ),
        ],
        "products": [
            ValidationRule(
                "price_positive", F.col("list_price") > 0, "List price must be positive"
            ),
            ValidationRule(
                "cost_non_negative", F.col("cost_price") >= 0, "Cost must be non-negative"
            ),
            ValidationRule("sku_not_null", F.col("sku").isNotNull(), "SKU required"),
        ],
        "inventory": [
            ValidationRule(
                "qty_on_hand_non_negative",
                F.col("quantity_on_hand") >= 0,
                "On-hand quantity must be non-negative",
            ),
            ValidationRule(
                "qty_available_leq_on_hand",
                F.col("quantity_available") <= F.col("quantity_on_hand"),
                "Available cannot exceed on-hand",
            ),
            ValidationRule(
                "abc_class_valid",
                F.col("abc_class").isin("A", "B", "C"),
                "ABC class must be A, B, or C",
            ),
        ],
        "reviews": [
            ValidationRule("rating_in_range", F.col("rating").between(1, 5), "Rating must be 1–5"),
            ValidationRule(
                "sentiment_valid",
                F.col("sentiment").isin("positive", "neutral", "negative"),
                "Sentiment must be positive/neutral/negative",
            ),
        ],
        "iot_sensors": [
            ValidationRule(
                "sensor_type_valid",
                F.col("sensor_type").isin(
                    "temperature_celsius",
                    "humidity_pct",
                    "co2_ppm",
                    "foot_traffic_count",
                    "energy_kwh",
                    "refrigeration_temp_celsius",
                    "noise_db",
                    "air_quality_index",
                ),
                "Sensor type must be known",
            ),
            ValidationRule(
                "battery_in_range",
                (F.col("battery_pct").isNull()) | F.col("battery_pct").between(0, 100),
                "Battery % must be 0-100",
            ),
        ],
        "finance": [
            ValidationRule(
                "debit_positive", F.col("debit_amount") > 0, "Debit amount must be positive"
            ),
            ValidationRule(
                "credit_positive", F.col("credit_amount") > 0, "Credit amount must be positive"
            ),
            ValidationRule(
                "balanced",
                F.abs(F.col("debit_amount") - F.col("credit_amount")) < 0.01,
                "Journal entry must balance (debit = credit)",
            ),
        ],
        "shipments": [
            ValidationRule(
                "carrier_valid",
                F.col("carrier").isin("fedex", "ups", "usps", "dhl", "ontrac"),
                "Carrier must be known",
            ),
        ],
    }


class SilverValidator:
    """
    Validates a cleansed DataFrame against domain-specific business rules.

    Returns (valid_df, invalid_df). Invalid rows include a _failed_rules
    column listing which rules they violated.
    """

    def validate(
        self,
        df: DataFrame,
        domain: str,
    ) -> tuple[DataFrame, DataFrame | None]:
        rules = _build_rules().get(domain)
        if not rules:
            return df, None

        # Build a combined validity flag and a list of violated rule names
        valid_expr = F.lit(True)
        failed_rules_expr = F.array()

        for rule in rules:
            if not self._rule_applies(df, rule):
                continue
            # A NULL predicate result (e.g. NULL amount) must count as a failure;
            # otherwise the row matches neither the valid nor the invalid filter.
            predicate = F.coalesce(rule.predicate, F.lit(False))
            valid_expr = valid_expr & predicate
            failed_rules_expr = F.when(
                ~predicate,
                F.array_union(failed_rules_expr, F.array(F.lit(rule.name))),
            ).otherwise(failed_rules_expr)

        flagged = df.withColumn("_is_valid", valid_expr).withColumn(
            "_failed_rules", failed_rules_expr
        )

        valid_df = flagged.filter(F.col("_is_valid")).drop("_is_valid", "_failed_rules")
        invalid_df = flagged.filter(~F.col("_is_valid")).drop("_is_valid")

        invalid_count = invalid_df.count()
        if invalid_count > 0:
            logger.warning(
                "validation_failures",
                domain=domain,
                failed_rows=invalid_count,
            )

        return valid_df, (invalid_df if invalid_count > 0 else None)

    def _rule_applies(self, df: DataFrame, rule: ValidationRule) -> bool:
        """Check that all columns referenced by the rule exist in the DataFrame."""
        try:
            # Attempt to resolve the column — errors if unknown column name
            df.select(rule.predicate).limit(0)
            return True
        except Exception:
            return False
