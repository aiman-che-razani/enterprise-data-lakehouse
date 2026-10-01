"""
Silver processor for the suppliers dimension (SCD Type 2).

SCD2 columns: tier, on_time_rate, quality_score, payment_terms, is_active

Derived Silver columns:
  supplier_score — composite score combining on_time_rate (60%) + quality_score/10 (40%)
  is_preferred   — score >= 0.80
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class SuppliersSilverProcessor(BaseSilverProcessor):
    domain = "suppliers"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        has_scores = "on_time_rate" in df.columns and "quality_score" in df.columns

        if has_scores:
            df = df.withColumn(
                "supplier_score",
                F.round(
                    F.col("on_time_rate") * 0.6 + (F.col("quality_score") / 10.0) * 0.4,
                    4,
                ),
            ).withColumn(
                "is_preferred",
                F.col("supplier_score") >= 0.80,
            )

        return df
