"""
dim_promotion — Gold promotion / campaign dimension.

Source: Silver campaigns (not SCD2 — campaigns are created once and closed-dated).
Grain: one row per campaign.

Columns:
  sk_promotion_id     — surrogate key (xxhash64 of campaign_id)
  nk_campaign_id
  campaign_name, campaign_type
  channel             — email / sms / push / display / social / search
  objective           — awareness / conversion / retention / reactivation
  start_date, end_date
  budget
  campaign_duration_days
  is_active
  roi
  performance_band    — underperform / on_track / overperform
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import add_sk

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DimPromotionBuilder(BaseGoldBuilder):
    table_name = "dim_promotion"
    is_dim = True

    def build_dim(self) -> DataFrame:
        silver = self.read_silver("campaigns")
        if "channel" not in silver.columns and "platform" in silver.columns:
            silver = silver.withColumn(
                "channel", F.col("platform")
            )  # the ad platform is the channel

        select_cols = [
            "campaign_id",
            "campaign_name",
            "campaign_type",
            "channel",
            "objective",
            "start_date",
            "end_date",
            "budget",
            "campaign_duration_days",
            "is_active",
            "roi",
            "performance_band",
        ]
        available = [c for c in select_cols if c in silver.columns]

        dim = silver.select(*available).withColumnRenamed("campaign_id", "nk_campaign_id")

        return add_sk(dim, "sk_promotion_id", "nk_campaign_id")
