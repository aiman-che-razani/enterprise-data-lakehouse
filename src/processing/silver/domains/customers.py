"""
Silver processor for the customers dimension (SCD Type 2 + PII tokenisation).

Dimension columns tracked for SCD2:
  segment, loyalty_tier, loyalty_points, country, state,
  channel_preference, is_email_verified, is_sms_opted_in, is_active

PII handling (from SILVER_PII_HANDLING):
  email        → email_hash (SHA-256, applied by PiiTokenizer)
  phone        → dropped
  address_line1→ dropped
  date_of_birth→ age_bucket ("25-34")

Derived Silver columns:
  years_as_customer  — derived from first_order_at / created_at
  lifetime_orders    — passed through from Bronze (denormalised from order counts)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.silver.base_processor import BaseSilverProcessor


class CustomersSilverProcessor(BaseSilverProcessor):
    domain = "customers"

    def domain_transform(self, df: DataFrame) -> DataFrame:
        # Derive customer age in years from created_at (time as customer)
        if "created_at" in df.columns:
            df = df.withColumn(
                "years_as_customer",
                F.round(
                    F.datediff(F.current_date(), F.col("created_at").cast("date")) / 365.25,
                    1,
                ),
            )
        # Normalise loyalty_tier ordering for downstream Gold joins
        df = df.withColumn(
            "loyalty_tier_rank",
            F.when(F.col("loyalty_tier") == "platinum", F.lit(4))
            .when(F.col("loyalty_tier") == "gold", F.lit(3))
            .when(F.col("loyalty_tier") == "silver", F.lit(2))
            .otherwise(F.lit(1)),
        )
        return df
