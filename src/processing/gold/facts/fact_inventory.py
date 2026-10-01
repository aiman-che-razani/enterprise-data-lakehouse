"""
fact_inventory — Gold inventory snapshot fact table.

Grain: one row per (store_id, sku, snapshot_date).

Sources: Silver inventory

Dimension FKs:
  sk_store_id   ← xxhash64(store_id)
  sk_product_id ← xxhash64(sku)
  sk_date_id    ← YYYYMMDD(snapshot_date)

Measures:
  quantity_on_hand, quantity_available, quantity_reserved
  reorder_point, max_stock_level
  avg_daily_units
  days_of_supply

Degenerate dims / flags:
  abc_class, bin_location, stock_health, needs_reorder
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import date_to_sk


class FactInventoryBuilder(BaseGoldBuilder):
    table_name = "fact_inventory"
    is_dim = False

    def build_fact(self) -> DataFrame:
        silver = self._with_derived_measures(self.read_silver("inventory"))

        fact = (
            silver.withColumn(
                "sk_store_id", F.xxhash64(F.col("store_id").cast("string")).cast(LongType())
            )
            .withColumn("sk_product_id", F.xxhash64(F.col("sku").cast("string")).cast(LongType()))
            .withColumn("sk_date_id", date_to_sk("snapshot_date"))
            .select(
                "sk_store_id",
                "sk_product_id",
                "sk_date_id",
                # Natural keys
                F.col("inventory_id").alias("nk_inventory_id"),
                F.col("store_id"),
                F.col("sku"),
                F.col("snapshot_date"),
                # Measures
                "quantity_on_hand",
                "quantity_available",
                "quantity_reserved",
                "reorder_point",
                "max_stock_level",
                "avg_daily_units",
                "days_of_supply",
                # Flags and classification
                "abc_class",
                "bin_location",
                "stock_health",
                "needs_reorder",
                "updated_at",
            )
        )

        return fact

    @staticmethod
    def _with_derived_measures(silver: DataFrame) -> DataFrame:
        """Measures Silver does not carry, derived with the usual definitions (kept when already present)."""
        if "max_stock_level" not in silver.columns:
            # replenishment ceiling: stock on hand right after a reorder arrives
            silver = silver.withColumn(
                "max_stock_level", F.col("reorder_point") + F.col("reorder_quantity")
            )
        if "avg_daily_units" not in silver.columns:
            # days_of_supply = on_hand / daily demand  =>  daily demand = on_hand / days_of_supply
            silver = silver.withColumn(
                "avg_daily_units",
                F.when(
                    F.col("days_of_supply") > 0,
                    F.round(F.col("quantity_on_hand") / F.col("days_of_supply"), 2),
                ),
            )
        if "stock_health" not in silver.columns:
            silver = silver.withColumn(
                "stock_health",
                F.when(F.col("is_out_of_stock"), "out_of_stock")
                .when(F.col("is_below_reorder_point"), "below_reorder_point")
                .when(F.col("quantity_on_hand") > F.col("max_stock_level"), "overstock")
                .otherwise("healthy"),
            )
        return silver
