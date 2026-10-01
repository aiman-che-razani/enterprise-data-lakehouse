"""
Silver processor for the shipments fact table.

Derived Silver columns:
  days_in_transit      — actual_delivery_date - ship_date (null if not delivered)
  is_on_time           — actual_delivery_date <= estimated_delivery_date
  delivery_variance_days — actual_delivery_date - estimated_delivery_date
                           (negative = early, positive = late)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class ShipmentsSilverProcessor(BaseSilverProcessor):
    domain = "shipments"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        has_ship = "ship_date" in df.columns
        has_actual = "actual_delivery_date" in df.columns
        has_est = "estimated_delivery_date" in df.columns

        if has_ship and has_actual:
            df = df.withColumn(
                "days_in_transit",
                F.datediff(
                    F.col("actual_delivery_date").cast("date"),
                    F.col("ship_date").cast("date"),
                ),
            )

        if has_actual and has_est:
            df = df.withColumn(
                "is_on_time",
                F.when(
                    F.col("actual_delivery_date").isNotNull()
                    & F.col("estimated_delivery_date").isNotNull(),
                    F.col("actual_delivery_date") <= F.col("estimated_delivery_date"),
                ).otherwise(F.lit(None).cast("boolean")),
            ).withColumn(
                "delivery_variance_days",
                F.when(
                    F.col("actual_delivery_date").isNotNull()
                    & F.col("estimated_delivery_date").isNotNull(),
                    F.datediff(
                        F.col("actual_delivery_date").cast("date"),
                        F.col("estimated_delivery_date").cast("date"),
                    ),
                ).otherwise(F.lit(None)),
            )

        return df
