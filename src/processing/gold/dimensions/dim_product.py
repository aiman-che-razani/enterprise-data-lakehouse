"""
dim_product — Gold product dimension.

Source: Silver products (is_current = True).
Grain: one row per SKU (current version).

Columns:
  sk_product_id  — surrogate key (xxhash64 of sku)
  sk_supplier_id — FK to dim_supplier
  nk_sku
  name, category_l1, category_l2
  list_price, cost_price, margin_amount, margin_pct
  price_tier     — budget / mid / premium / luxury (derived in Silver)
  is_active, is_discontinued
  effective_from, effective_to
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import add_sk

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DimProductBuilder(BaseGoldBuilder):
    table_name = "dim_product"
    is_dim = True

    def build_dim(self) -> DataFrame:
        silver = self.read_silver_current("products")

        dim = (
            silver.select(
                "sku",
                "name",
                "category_l1",
                "category_l2",
                "list_price",
                "cost_price",
                "margin_amount",
                "margin_pct",
                "price_tier",
                "is_active",
                "is_discontinued",
                "supplier_id",
                "effective_from",
                "effective_to",
                "created_at",
            )
            .withColumnRenamed("sku", "nk_sku")
            .withColumnRenamed("supplier_id", "_raw_supplier_id")
        )

        dim = add_sk(dim, "sk_product_id", "nk_sku")
        dim = add_sk(dim, "sk_supplier_id", "_raw_supplier_id")
        return dim.drop("_raw_supplier_id")
