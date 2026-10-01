"""
fact_marketing — Gold marketing performance fact table.

Grain: one row per (campaign_id, event_date).

Sources:
  Silver campaigns  → budget, spend, revenue_attributed
  Silver clickstream → aggregated by campaign_id (from referrer_campaign_id) + event_date

Dimension FKs:
  sk_promotion_id ← xxhash64(campaign_id)
  sk_date_id      ← YYYYMMDD(event_date)

Measures (clickstream-aggregated):
  sessions, page_views, add_to_carts, checkouts, conversions
  revenue_attributed (from campaigns)
  spend             (budget * days_elapsed / campaign_duration proportional estimate)
  ctr, conversion_rate (from campaigns Silver)

The clickstream aggregate uses event_type counts per campaign per day.
Sessions = distinct session_ids linked to a campaign.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import date_to_sk


class FactMarketingBuilder(BaseGoldBuilder):
    table_name = "fact_marketing"
    is_dim = False

    def build_fact(self) -> DataFrame:
        campaigns = self.read_silver("campaigns")
        clickstream = self.read_silver("clickstream")

        # Aggregate clickstream events by campaign and date
        cs_agg = (
            clickstream.filter(F.col("campaign_id").isNotNull())
            .withColumn("event_date", F.to_date(F.col("created_at")))
            .groupBy("campaign_id", "event_date")
            .agg(
                F.countDistinct("session_id").alias("sessions"),
                F.count("*").alias("page_views"),
                F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                    "add_to_carts"
                ),
                F.sum(F.when(F.col("is_conversion"), 1).otherwise(0)).alias("conversions"),
            )
        )

        # Join campaigns to clickstream aggregates
        camp_cols = campaigns.select(
            "campaign_id",
            "campaign_type",
            F.col("platform").alias("channel"),  # the ad platform is the channel
            "budget",
            F.col("total_revenue").alias("revenue_attributed"),
            "ctr",
            "conversion_rate",
            "roi",
            "performance_band",
            "start_date",
            "end_date",
        )

        fact = (
            cs_agg.alias("cs")
            .join(camp_cols.alias("c"), on="campaign_id", how="left")
            .withColumn(
                "sk_promotion_id", F.xxhash64(F.col("campaign_id").cast("string")).cast(LongType())
            )
            .withColumn("sk_date_id", date_to_sk("event_date"))
            # Composite natural key: campaign_id + date
            .withColumn(
                "nk_marketing_id",
                F.concat_ws("|", F.col("campaign_id"), F.col("event_date").cast("string")),
            )
            .select(
                "sk_promotion_id",
                "sk_date_id",
                "nk_marketing_id",
                F.col("campaign_id"),
                "event_date",
                "campaign_type",
                "channel",
                "sessions",
                "page_views",
                "add_to_carts",
                "conversions",
                "budget",
                "revenue_attributed",
                "ctr",
                "conversion_rate",
                "roi",
                "performance_band",
            )
        )

        return fact
