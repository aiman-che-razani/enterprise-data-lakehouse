"""
dim_store — Gold store dimension.

Source: Silver stores (is_current = True).
Grain: one row per store (current version).

Columns:
  sk_store_id      — surrogate key
  nk_store_id
  store_name, store_type
  city, state, country
  latitude, longitude
  sq_footage, num_employees
  store_size_band  — small / medium / large / flagship (derived in Silver)
  is_active, opened_date, closed_date
  effective_from, effective_to
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import add_sk

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DimStoreBuilder(BaseGoldBuilder):
    table_name = "dim_store"
    is_dim = True

    def build_dim(self) -> DataFrame:
        silver = self.read_silver_current("stores").withColumnRenamed("name", "store_name")

        select_cols = [
            "store_id",
            "store_name",
            "store_type",
            "city",
            "state",
            "country",
            "sq_footage",
            "num_employees",
            "store_size_band",
            "sq_ft_per_employee",
            "is_active",
            "opened_date",
            "effective_from",
            "effective_to",
        ]
        # The warehouse schema always has latitude / longitude; sources without coordinates yield NULLs.
        for coord in ("latitude", "longitude"):
            if coord not in silver.columns:
                silver = silver.withColumn(coord, F.lit(None).cast("double"))
            select_cols.append(coord)
        if "closed_date" in silver.columns:
            select_cols.append("closed_date")

        dim = silver.select(*select_cols).withColumnRenamed("store_id", "nk_store_id")

        return add_sk(dim, "sk_store_id", "nk_store_id")
