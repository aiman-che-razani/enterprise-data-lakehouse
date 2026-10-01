"""
Silver processor for the payments fact table.

Note: card_last4 is already masked to "****" in the Kafka producer before
Bronze ingestion, so no PII action is needed here.

Derived Silver columns:
  is_refunded       — status in ("refunded", "partially_refunded")
  net_amount        — amount - COALESCE(refund_amount, 0)
  days_to_settle    — updated_at - created_at (settlement latency proxy)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class PaymentsSilverProcessor(BaseSilverProcessor):
    domain = "payments"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        if "status" in df.columns:
            df = df.withColumn(
                "is_refunded",
                F.col("status").isin("refunded", "partially_refunded"),
            )

        if "amount" in df.columns:
            refund_col = F.coalesce(F.col("refund_amount"), F.lit(0.0))
            df = df.withColumn(
                "net_amount",
                F.round(F.col("amount") - refund_col, 2),
            )

        if "created_at" in df.columns and "updated_at" in df.columns:
            df = df.withColumn(
                "days_to_settle",
                F.datediff(
                    F.col("updated_at").cast("date"),
                    F.col("created_at").cast("date"),
                ),
            )

        return df
