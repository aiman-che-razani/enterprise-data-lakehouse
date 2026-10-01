"""
fact_shipments — Gold shipments fact table.

Grain: one row per shipment (nk_shipment_id).

Sources: Silver shipments (joined to Silver orders for store context).

Dimension FKs:
  sk_store_id ← xxhash64(store_id) via order join
  sk_date_id  ← YYYYMMDD(ship_date), or the record-created date while still unshipped

Measures:
  shipping_cost, days_in_transit, delivery_variance_days

Degenerate dims:
  carrier, service_level, tracking_number, is_on_time
  nk_order_id, nk_shipment_id
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import date_to_sk, null_sk


class FactShipmentsBuilder(BaseGoldBuilder):
    table_name = "fact_shipments"
    is_dim = False

    def build_fact(self) -> DataFrame:
        shipments = self.read_silver("shipments")
        orders = self.read_silver("orders").select("order_id", "store_id", "customer_id")

        fact = (
            shipments.alias("s")
            .join(orders.alias("o"), on="order_id", how="left")
            .withColumn(
                "sk_store_id",
                F.when(
                    F.col("o.store_id").isNotNull(),
                    F.xxhash64(F.col("o.store_id").cast("string")).cast(LongType()),
                ).otherwise(null_sk()),
            )
            .withColumn(
                "sk_customer_id",
                F.when(
                    F.col("o.customer_id").isNotNull(),
                    F.xxhash64(F.col("o.customer_id").cast("string")).cast(LongType()),
                ).otherwise(null_sk()),
            )
            # sk_date_id is NOT NULL in every warehouse. Shipments not yet shipped have no
            # ship_date, so they are dated by when the shipment record was created.
            .withColumn(
                "_fact_date", F.coalesce(F.col("s.ship_date"), F.to_date(F.col("s.created_at")))
            )
            .withColumn("sk_date_id", date_to_sk("_fact_date"))
            .select(
                "sk_store_id",
                "sk_customer_id",
                "sk_date_id",
                F.col("s.shipment_id").alias("nk_shipment_id"),
                F.col("s.order_id").alias("nk_order_id"),
                "carrier",
                F.col("s.service").alias("service_level"),
                "tracking_number",
                "shipping_cost",
                F.col("s.ship_date"),
                F.col("s.estimated_delivery_date"),
                F.col("s.actual_delivery_date"),
                "days_in_transit",
                "is_on_time",
                "delivery_variance_days",
                "status",
            )
        )

        return fact
