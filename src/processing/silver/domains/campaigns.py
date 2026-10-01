"""
Silver processor for the campaigns dimension-like table.

Campaigns are not SCD2 (they are created-once, close-dated), but they
receive standard Silver enrichment.

Derived Silver columns:
  campaign_duration_days — end_date - start_date
  is_active              — today is between start_date and end_date
  roi                    — (total_revenue - budget) / budget (null if budget=0)
  performance_band       — "underperform" / "on_track" / "overperform"
                           based on ctr vs campaign_type benchmarks
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor

# CTR benchmarks by campaign type
_CTR_BENCHMARK: dict[str, float] = {
    "email": 0.021,
    "sms": 0.030,
    "push": 0.015,
    "display": 0.005,
    "social": 0.010,
    "search": 0.035,
    # campaign_type values as the source systems (and the generator) spell them
    "push_notification": 0.015,
    "social_media": 0.010,
    "paid_search": 0.035,
    "affiliate": 0.020,
}


class CampaignsSilverProcessor(BaseSilverProcessor):
    domain = "campaigns"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        if "start_date" in df.columns and "end_date" in df.columns:
            df = df.withColumn(
                "campaign_duration_days",
                F.datediff(
                    F.col("end_date").cast("date"),
                    F.col("start_date").cast("date"),
                ),
            ).withColumn(
                "is_active",
                (F.col("start_date").cast("date") <= F.current_date())
                & (
                    F.col("end_date").isNull()
                    | (F.col("end_date").cast("date") >= F.current_date())
                ),
            )

        if "total_revenue" in df.columns and "budget" in df.columns:
            df = df.withColumn(
                "roi",
                F.when(
                    F.col("budget") > 0,
                    F.round(
                        (F.col("total_revenue") - F.col("budget")) / F.col("budget"),
                        4,
                    ),
                ).otherwise(F.lit(None)),
            )

        if "ctr" in df.columns and "campaign_type" in df.columns:
            # Build a benchmark lookup using a literal map
            benchmark_map = F.create_map(
                *[v for kv in [(F.lit(k), F.lit(v)) for k, v in _CTR_BENCHMARK.items()] for v in kv]
            )
            benchmark = benchmark_map[F.col("campaign_type")]
            df = df.withColumn(
                "performance_band",
                F.when(benchmark.isNull(), F.lit(None))
                .when(F.col("ctr") < benchmark * 0.8, "underperform")
                .when(F.col("ctr") > benchmark * 1.2, "overperform")
                .otherwise("on_track"),
            )

        return df
