"""
Silver processor for the inventory fact table.

Derived Silver columns:
  stock_health      — "out_of_stock" / "critical" / "low" / "adequate" / "excess"
                      based on quantity_available vs reorder_point / max_stock
  days_of_supply    — quantity_available / avg_daily_units (rough supply days)
  needs_reorder     — quantity_available <= reorder_point
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class InventorySilverProcessor(BaseSilverProcessor):
    domain = "inventory"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        has_qty = "quantity_available" in df.columns
        has_reorder = "reorder_point" in df.columns
        has_max = "max_stock_level" in df.columns
        has_avg = "avg_daily_units" in df.columns

        if has_qty and has_reorder:
            df = df.withColumn(
                "needs_reorder",
                F.col("quantity_available") <= F.col("reorder_point"),
            )

        if has_qty and has_reorder and has_max:
            df = df.withColumn(
                "stock_health",
                F.when(F.col("quantity_available") <= 0, "out_of_stock")
                .when(F.col("quantity_available") < F.col("reorder_point") * 0.5, "critical")
                .when(F.col("quantity_available") <= F.col("reorder_point"), "low")
                .when(F.col("quantity_available") <= F.col("max_stock_level") * 0.8, "adequate")
                .otherwise("excess"),
            )

        if has_qty and has_avg:
            df = df.withColumn(
                "days_of_supply",
                F.when(
                    F.col("avg_daily_units") > 0,
                    F.round(F.col("quantity_available") / F.col("avg_daily_units"), 1),
                ).otherwise(F.lit(None)),
            )

        return df
