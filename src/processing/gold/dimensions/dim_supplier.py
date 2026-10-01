"""
dim_supplier — Gold supplier dimension.

Source: Silver suppliers (is_current = True).
Grain: one row per supplier (current version).

Columns:
  sk_supplier_id  — surrogate key
  nk_supplier_id
  supplier_name
  tier            — gold / silver / bronze
  on_time_rate, quality_score, supplier_score
  is_preferred    — score >= 0.80
  payment_terms
  country
  is_active
  onboarded_date
  effective_from, effective_to
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import add_sk

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DimSupplierBuilder(BaseGoldBuilder):
    table_name = "dim_supplier"
    is_dim = True

    def build_dim(self) -> DataFrame:
        silver = self.read_silver_current("suppliers").withColumnRenamed("name", "supplier_name")

        select_cols = [
            "supplier_id",
            "supplier_name",
            "tier",
            "on_time_rate",
            "quality_score",
            "supplier_score",
            "is_preferred",
            "payment_terms",
            "is_active",
            "effective_from",
            "effective_to",
        ]
        for optional in ("country",):
            if optional in silver.columns:
                select_cols.append(optional)

        dim = silver.select(*select_cols).withColumnRenamed("supplier_id", "nk_supplier_id")

        return add_sk(dim, "sk_supplier_id", "nk_supplier_id")
