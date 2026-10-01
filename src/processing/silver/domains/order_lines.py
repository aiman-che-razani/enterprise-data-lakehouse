"""
Silver processor for the order_lines fact table.

Derived Silver columns:
  effective_unit_price — line_total / quantity (post-discount per-unit price)
  is_discounted        — effective_unit_price < unit_price
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class OrderLinesSilverProcessor(BaseSilverProcessor):
    domain = "order_lines"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        has_cols = "line_total" in df.columns and "quantity" in df.columns

        if has_cols:
            df = df.withColumn(
                "effective_unit_price",
                F.when(
                    F.col("quantity") > 0,
                    F.round(F.col("line_total") / F.col("quantity"), 4),
                ).otherwise(F.lit(None)),
            )

        if "unit_price" in df.columns and "effective_unit_price" in df.columns:
            df = df.withColumn(
                "is_discounted",
                F.col("effective_unit_price") < F.col("unit_price"),
            )

        return df
