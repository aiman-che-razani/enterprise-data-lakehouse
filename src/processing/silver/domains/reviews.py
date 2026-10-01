"""
Silver processor for the reviews fact table.

Derived Silver columns:
  is_verified_purchase  — passed through from Bronze (bool)
  sentiment_score       — numeric encoding: positive=1, neutral=0, negative=-1
  body_length           — character count of review_body (null if no body)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class ReviewsSilverProcessor(BaseSilverProcessor):
    domain = "reviews"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        if "sentiment" in df.columns:
            df = df.withColumn(
                "sentiment_score",
                F.when(F.col("sentiment") == "positive", F.lit(1))
                .when(F.col("sentiment") == "negative", F.lit(-1))
                .otherwise(F.lit(0)),
            )

        if "review_body" in df.columns:
            df = df.withColumn(
                "body_length",
                F.when(
                    F.col("review_body").isNotNull(),
                    F.length(F.col("review_body")),
                ).otherwise(F.lit(None)),
            )

        return df
