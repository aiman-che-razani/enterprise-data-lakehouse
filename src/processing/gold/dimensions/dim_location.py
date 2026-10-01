"""
dim_location — Gold geography dimension.

Derived from the union of distinct city/state/country combinations found in:
  - Silver stores (includes latitude, longitude)
  - Silver customers (city, state, country only)

Grain: one row per unique (city, state, country) combination.

Columns:
  sk_location_id  — xxhash64 of "city|state|country"
  nk_location_key — "city|state|country" composite natural key
  city, state, country
  region          — "North America" / "Europe" / "Asia Pacific" / "Other"
                    derived from country ISO code
  latitude, longitude   — from stores if available, else null
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.gold.surrogate_keys import add_sk

# Country → region mapping (covers common countries in RetailNexus synthetic data)
_NORTH_AMERICA = ["US", "CA", "MX"]
_EUROPE = ["GB", "DE", "FR", "IT", "ES", "NL", "SE", "NO", "DK", "FI", "PL"]
_ASIA_PACIFIC = ["CN", "JP", "KR", "AU", "IN", "SG", "HK", "TW", "NZ"]


class DimLocationBuilder(BaseGoldBuilder):
    table_name = "dim_location"
    is_dim = True

    def build_dim(self) -> DataFrame:
        # Store locations (have lat/lon when the source provides them)
        stores = self.read_silver_current("stores")

        def _coord(name: str) -> Column:
            col = F.col(name) if name in stores.columns else F.lit(None)
            return col.cast(DoubleType()).alias(name)

        stores_locs = stores.select(
            F.col("city"),
            F.col("state"),
            F.col("country"),
            _coord("latitude"),
            _coord("longitude"),
        )

        # Customer locations (no lat/lon)
        customer_locs = (
            self.read_silver_current("customers")
            .select(
                F.col("country"),
                F.col("state"),
                F.lit(None).cast("string").alias("city"),
                F.lit(None).cast(DoubleType()).alias("latitude"),
                F.lit(None).cast(DoubleType()).alias("longitude"),
            )
            .filter(F.col("country").isNotNull())
        )

        # Union and deduplicate — prefer store rows (have lat/lon) over customer rows
        all_locs = stores_locs.unionByName(customer_locs).distinct()

        dim = all_locs.withColumn(
            "nk_location_key",
            F.concat_ws(
                "|",
                F.coalesce(F.col("city"), F.lit("")),
                F.coalesce(F.col("state"), F.lit("")),
                F.coalesce(F.col("country"), F.lit("")),
            ),
        ).withColumn(
            "region",
            F.when(F.col("country").isin(_NORTH_AMERICA), "North America")
            .when(F.col("country").isin(_EUROPE), "Europe")
            .when(F.col("country").isin(_ASIA_PACIFIC), "Asia Pacific")
            .otherwise("Other"),
        )

        return add_sk(dim, "sk_location_id", "nk_location_key")
