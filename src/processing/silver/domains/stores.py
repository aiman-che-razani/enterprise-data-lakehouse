"""
Silver processor for the stores dimension (SCD Type 2).

SCD2 columns: store_type, sq_footage, num_employees, is_active

Derived Silver columns:
  sq_ft_per_employee — sq_footage / num_employees (productivity indicator)
  store_size_band    — "small" / "medium" / "large" / "flagship"
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class StoresSilverProcessor(BaseSilverProcessor):
    domain = "stores"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        if "sq_footage" in df.columns and "num_employees" in df.columns:
            df = df.withColumn(
                "sq_ft_per_employee",
                F.when(
                    F.col("num_employees") > 0,
                    F.round(F.col("sq_footage") / F.col("num_employees"), 1),
                ).otherwise(F.lit(None)),
            )

        if "sq_footage" in df.columns:
            df = df.withColumn(
                "store_size_band",
                F.when(F.col("sq_footage") < 5_000, "small")
                .when(F.col("sq_footage") < 15_000, "medium")
                .when(F.col("sq_footage") < 40_000, "large")
                .otherwise("flagship"),
            )

        return df
