"""
dim_customer — Gold customer dimension.

Source: Silver customers (is_current = True only — current SCD2 snapshot).
Grain: one row per customer (current version).

Columns:
  sk_customer_id     — xxhash64 surrogate key
  nk_customer_id     — natural key (UUID from Silver)
  email_hash         — SHA-256 of email (PII-tokenised in Silver)
  age_bucket         — "25-34" style bucket (PII-tokenised in Silver)
  segment            — platinum / gold / silver / bronze
  loyalty_tier       — same as segment (kept separate for forward compat)
  loyalty_tier_rank  — 1–4 numeric rank (derived in Silver)
  loyalty_points     — current points balance
  country, state
  channel_preference — email / sms / push / none
  is_email_verified, is_sms_opted_in, is_active
  years_as_customer
  effective_from, effective_to  — SCD2 passthrough (current record: effective_to IS NULL)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import add_sk

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DimCustomerBuilder(BaseGoldBuilder):
    table_name = "dim_customer"
    is_dim = True

    def build_dim(self) -> DataFrame:
        silver = self.read_silver_current("customers")

        dim = silver.select(
            "customer_id",
            "email_hash",
            "age_bucket",
            "segment",
            "loyalty_tier",
            "loyalty_tier_rank",
            "loyalty_points",
            "country",
            "state",
            "channel_preference",
            "is_email_verified",
            "is_sms_opted_in",
            "is_active",
            "years_as_customer",
            "effective_from",
            "effective_to",
            "created_at",
        ).withColumnRenamed("customer_id", "nk_customer_id")

        return add_sk(dim, "sk_customer_id", "nk_customer_id")
