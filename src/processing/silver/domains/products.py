"""
Silver processor for the products dimension (SCD Type 2).

SCD2 columns: name, category_l1, category_l2, list_price, cost_price,
              is_active, is_discontinued, supplier_id

Derived Silver columns:
  margin_amount  — list_price - cost_price
  margin_pct     — margin_amount / list_price (clipped 0-1 by cleanser if unrealistic)
  price_tier     — "budget" / "mid" / "premium" / "luxury" based on list_price
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class ProductsSilverProcessor(BaseSilverProcessor):
    domain = "products"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        has_price = "list_price" in df.columns and "cost_price" in df.columns

        if has_price:
            df = df.withColumn(
                "margin_amount",
                F.col("list_price") - F.col("cost_price"),
            ).withColumn(
                "margin_pct",
                F.when(
                    F.col("list_price") > 0,
                    (F.col("list_price") - F.col("cost_price")) / F.col("list_price"),
                ).otherwise(F.lit(None)),
            )

        if "list_price" in df.columns:
            df = df.withColumn(
                "price_tier",
                F.when(F.col("list_price") < 25, "budget")
                .when(F.col("list_price") < 100, "mid")
                .when(F.col("list_price") < 500, "premium")
                .otherwise("luxury"),
            )

        return df
