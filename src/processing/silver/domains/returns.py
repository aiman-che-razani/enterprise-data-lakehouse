"""
Silver processor for the returns fact table.

Derived Silver columns:
  days_to_process   — processed_at - initiated_at
  days_to_refund    — refunded_at - processed_at (null if not yet refunded)
  is_fully_refunded — refund_amount >= return_amount (within 1 cent tolerance)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class ReturnsSilverProcessor(BaseSilverProcessor):
    domain = "returns"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        if "initiated_at" in df.columns and "processed_at" in df.columns:
            df = df.withColumn(
                "days_to_process",
                F.datediff(
                    F.col("processed_at").cast("date"),
                    F.col("initiated_at").cast("date"),
                ),
            )

        if "processed_at" in df.columns and "refunded_at" in df.columns:
            df = df.withColumn(
                "days_to_refund",
                F.when(
                    F.col("refunded_at").isNotNull(),
                    F.datediff(
                        F.col("refunded_at").cast("date"),
                        F.col("processed_at").cast("date"),
                    ),
                ).otherwise(F.lit(None)),
            )

        if "refund_amount" in df.columns and "return_amount" in df.columns:
            df = df.withColumn(
                "is_fully_refunded",
                F.abs(F.col("refund_amount") - F.col("return_amount")) < 0.01,
            )

        return df
