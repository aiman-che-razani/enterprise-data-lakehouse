"""
fact_returns — Gold returns fact table.

Grain: one row per return (nk_return_id).

Sources: Silver returns (joined to Silver orders for customer/store context).

Dimension FKs:
  sk_customer_id ← xxhash64(customer_id) via order join
  sk_product_id  ← xxhash64(sku) — returns have item-level detail
  sk_store_id    ← xxhash64(store_id) via order join
  sk_date_id     ← YYYYMMDD(initiated_at date)

Measures:
  return_amount, refund_amount
  quantity_returned

Degenerate dims / derived:
  reason, status
  days_to_process, days_to_refund, is_fully_refunded
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import date_to_sk


class FactReturnsBuilder(BaseGoldBuilder):
    table_name = "fact_returns"
    is_dim = False

    def build_fact(self) -> DataFrame:
        returns = self.read_silver("returns")
        orders = self.read_silver("orders").select("order_id", "customer_id", "store_id")

        fact = (
            returns.alias("r")
            .join(orders.alias("o"), on="order_id", how="left")
            .withColumn(
                "sk_customer_id", F.xxhash64(F.col("o.customer_id").cast("string")).cast(LongType())
            )
            .withColumn(
                "sk_store_id", F.xxhash64(F.col("o.store_id").cast("string")).cast(LongType())
            )
            .withColumn(
                "sk_product_id",
                F.when(
                    F.col("r.sku").isNotNull(),
                    F.xxhash64(F.col("r.sku").cast("string")).cast(LongType()),
                ).otherwise(F.lit(None).cast(LongType())),
            )
            .withColumn("sk_date_id", date_to_sk("r.initiated_at"))
            # Silver carries quantity / unit_price / refund_amount; the fact's measures derive from them
            .withColumn("quantity_returned", F.col("r.quantity"))
            .withColumn("return_amount", F.round(F.col("r.quantity") * F.col("r.unit_price"), 2))
            .withColumn(
                "is_fully_refunded",
                F.coalesce(F.col("r.refund_amount") >= F.col("return_amount"), F.lit(False)),
            )
            .select(
                "sk_customer_id",
                "sk_product_id",
                "sk_store_id",
                "sk_date_id",
                F.col("r.return_id").alias("nk_return_id"),
                F.col("r.order_id").alias("nk_order_id"),
                F.col("r.sku"),
                "reason",
                "status",
                "return_amount",
                "refund_amount",
                "quantity_returned",
                "days_to_process",
                "days_to_refund",
                "is_fully_refunded",
                F.col("r.initiated_at"),
            )
        )

        return fact
