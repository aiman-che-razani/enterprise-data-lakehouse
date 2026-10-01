"""
fact_sales — Gold sales fact table.

Grain: one row per order line (nk_order_line_id).

Sources:
  Silver orders      → order-level attributes (customer, store, date, channel, status)
  Silver order_lines → line-level attributes (sku, qty, price, total)

Dimension FKs resolved via xxhash64 surrogate keys:
  sk_customer_id  ← xxhash64(customer_id)
  sk_product_id   ← xxhash64(sku)
  sk_store_id     ← xxhash64(store_id)
  sk_date_id      ← YYYYMMDD(order_date)
  sk_promotion_id ← xxhash64(campaign_id) if not null, else null

Measures:
  quantity, unit_price, effective_unit_price, line_total
  discount_amount (from order header, apportioned: line_total / order subtotal * discount)
  is_discounted

Degenerate dims (no FK, stored inline):
  nk_order_id, nk_order_line_id, order_status, channel
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import date_to_sk, null_sk


class FactSalesBuilder(BaseGoldBuilder):
    table_name = "fact_sales"
    is_dim = False

    def build_fact(self) -> DataFrame:
        orders = self.read_silver("orders").filter(F.col("status").isNotNull())
        lines = self.read_silver("order_lines")

        # Join order lines to order header
        joined = lines.alias("l").join(
            orders.select(
                "order_id",
                "customer_id",
                "store_id",
                F.col("order_date").alias("order_date"),
                "status",
                "channel",
                "subtotal",
                "discount_amount",
                # Orders carry a free-text promo_code, not a campaign reference: until a source links an
                # order to a campaign, every sale maps to the "no promotion" key.
                (F.col("campaign_id") if "campaign_id" in orders.columns else F.lit(None))
                .cast("string")
                .alias("campaign_id"),
                "created_at",
            ).alias("o"),
            on="order_id",
            how="inner",
        )

        # Apportion order-level discount to line level by weight
        discount_weight = F.when(
            (F.col("o.subtotal").isNotNull()) & (F.col("o.subtotal") > 0),
            F.col("l.line_total") / F.col("o.subtotal"),
        ).otherwise(F.lit(0.0))

        apportioned_discount = F.round(
            F.coalesce(F.col("o.discount_amount"), F.lit(0.0)) * discount_weight,
            2,
        )

        fact = (
            joined.withColumn("sk_customer_id", F.xxhash64(F.col("o.customer_id").cast("string")))
            .withColumn("sk_product_id", F.xxhash64(F.col("l.sku").cast("string")))
            .withColumn("sk_store_id", F.xxhash64(F.col("o.store_id").cast("string")))
            .withColumn("sk_date_id", date_to_sk("o.order_date"))
            .withColumn(
                "sk_promotion_id",
                F.when(
                    F.col("o.campaign_id").isNotNull(),
                    F.xxhash64(F.col("o.campaign_id").cast("string")),
                ).otherwise(null_sk()),
            )
            .withColumn("line_discount_amount", apportioned_discount)
            .select(
                # Surrogate keys
                F.col("sk_customer_id").cast(LongType()),
                F.col("sk_product_id").cast(LongType()),
                F.col("sk_store_id").cast(LongType()),
                "sk_date_id",
                F.col("sk_promotion_id").cast(LongType()),
                # Natural / degenerate dims
                F.col("l.order_id").alias("nk_order_id"),
                F.col("l.order_line_id").alias("nk_order_line_id"),
                F.col("o.status").alias("order_status"),
                F.col("o.channel"),
                F.col("l.sku"),
                # Measures
                F.col("l.quantity"),
                F.col("l.unit_price"),
                F.col("l.effective_unit_price"),
                F.col("l.line_total"),
                F.col("line_discount_amount"),
                F.col("l.is_discounted"),
                # Audit
                F.col("o.created_at").alias("order_created_at"),
            )
        )

        return fact
