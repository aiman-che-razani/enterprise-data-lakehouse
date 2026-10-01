"""
Silver processor for the clickstream fact table (high-volume).

Clickstream Bronze is partitioned by event_date (added by streaming consumer).
Silver deduplicates on event_id; no SCD2 (events are immutable).

Derived Silver columns:
  is_conversion     — event_type == "purchase"
  is_cart_event     — event_type in ("add_to_cart", "abandoned_cart")
  event_hour        — hour of created_at (for hourly traffic analysis)
  device_category   — "mobile" / "desktop" / "tablet" grouped from device_type
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class ClickstreamSilverProcessor(BaseSilverProcessor):
    domain = "clickstream"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        if "event_type" in df.columns:
            df = df.withColumn(
                "is_conversion",
                F.col("event_type") == "purchase",
            ).withColumn(
                "is_cart_event",
                F.col("event_type").isin("add_to_cart", "abandoned_cart"),
            )

        if "created_at" in df.columns:
            df = df.withColumn(
                "event_hour",
                F.hour(F.col("created_at")),
            )

        if "device_type" in df.columns:
            df = df.withColumn(
                "device_category",
                F.when(
                    F.col("device_type").isin("mobile", "smartphone"),
                    F.lit("mobile"),
                )
                .when(F.col("device_type") == "tablet", F.lit("tablet"))
                .otherwise(F.lit("desktop")),
            )

        return df
