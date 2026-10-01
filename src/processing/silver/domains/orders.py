"""
Silver processor for the orders fact table.

Derived Silver columns:
  order_value_band   — "low" / "mid" / "high" / "vip" based on total_amount
  has_discount       — discount_amount > 0
  days_to_update     — updated_at - created_at in days (fulfillment speed proxy)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class OrdersSilverProcessor(BaseSilverProcessor):
    domain = "orders"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        if "total_amount" in df.columns:
            df = df.withColumn(
                "order_value_band",
                F.when(F.col("total_amount") < 50, "low")
                .when(F.col("total_amount") < 200, "mid")
                .when(F.col("total_amount") < 1000, "high")
                .otherwise("vip"),
            )

        if "discount_amount" in df.columns:
            df = df.withColumn(
                "has_discount",
                F.coalesce(F.col("discount_amount"), F.lit(0)) > 0,
            )

        if "created_at" in df.columns and "updated_at" in df.columns:
            df = df.withColumn(
                "days_to_update",
                F.datediff(
                    F.col("updated_at").cast("date"),
                    F.col("created_at").cast("date"),
                ),
            )

        return df
