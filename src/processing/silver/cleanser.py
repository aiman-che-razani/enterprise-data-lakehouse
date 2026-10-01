"""
Data cleansing for the Silver layer.

Responsibilities:
  1. Cast timestamp / date string columns to the correct Spark types
  2. Trim leading/trailing whitespace from string columns
  3. Lowercase enum-like columns (status, channel, segment, etc.)
  4. Replace empty strings with nulls
  5. Clip numeric columns to valid ranges (e.g., rating 1-5, pct 0-1)
  6. Standardise boolean columns from string representations

This runs AFTER deduplication and BEFORE domain-specific validation,
so validators can assume correct types.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from src.common.logging import get_logger
from src.processing.silver.domain_meta import DOMAIN_DATE_COLS, DOMAIN_TIMESTAMP_COLS

logger = get_logger(__name__)

_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
_DATE_FORMAT = "yyyy-MM-dd"

# Enum-like columns that should be lowercased in Silver (abc_class stays A / B / C by convention)
_LOWERCASE_COLS: set[str] = {
    "status",
    "payment_status",
    "channel",
    "segment",
    "loyalty_tier",
    "tier",
    "event_type",
    "device_type",
    "sentiment",
    "condition",
    "reason",
    "carrier",  # the shipment validator expects fedex / ups / usps / dhl / ontrac
    "sensor_type",
    "campaign_type",
    "objective",
    "store_type",
    "source_system",
    "reading_quality",
    "transaction_type",
}

# Numeric columns with valid ranges [min, max] — values outside are set to null
_RANGE_CLIPS: dict[str, tuple[float, float]] = {
    "rating": (1.0, 5.0),
    "on_time_rate": (0.0, 1.0),
    "quality_score": (0.0, 10.0),
    "discount_pct": (0.0, 1.0),
    "battery_pct": (0.0, 100.0),
    "ctr": (0.0, 1.0),
    "conversion_rate": (0.0, 1.0),
    "margin_pct": (-1.0, 10.0),
}


class SilverCleanser:
    """Applies a standard set of cleansing transforms to a Bronze DataFrame."""

    def cleanse(self, df: DataFrame, domain: str) -> DataFrame:
        df = self._cast_timestamps(df, domain)
        df = self._cast_dates(df, domain)
        df = self._trim_strings(df)
        df = self._lowercase_enums(df)
        df = self._nullify_empty_strings(df)
        df = self._clip_ranges(df)
        df = self._cast_booleans(df)
        return df

    # ─── Individual transforms ────────────────────────────────────────────────

    def _cast_timestamps(self, df: DataFrame, domain: str) -> DataFrame:
        ts_cols = DOMAIN_TIMESTAMP_COLS.get(domain, [])
        for col in ts_cols:
            if col in df.columns:
                df = df.withColumn(
                    col,
                    F.to_timestamp(F.col(col), _TS_FORMAT),
                )
        return df

    def _cast_dates(self, df: DataFrame, domain: str) -> DataFrame:
        date_cols = DOMAIN_DATE_COLS.get(domain, [])
        for col in date_cols:
            if col in df.columns:
                df = df.withColumn(
                    col,
                    F.to_date(F.col(col), _DATE_FORMAT),
                )
        return df

    def _trim_strings(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.types import StringType

        str_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]
        for col in str_cols:
            df = df.withColumn(col, F.trim(F.col(col)))
        return df

    def _lowercase_enums(self, df: DataFrame) -> DataFrame:
        for col in _LOWERCASE_COLS:
            if col in df.columns:
                df = df.withColumn(col, F.lower(F.col(col)))
        return df

    def _nullify_empty_strings(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.types import StringType

        str_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]
        for col in str_cols:
            df = df.withColumn(
                col,
                F.when(F.col(col) == "", F.lit(None)).otherwise(F.col(col)),
            )
        return df

    def _clip_ranges(self, df: DataFrame) -> DataFrame:
        for col_name, (lo, hi) in _RANGE_CLIPS.items():
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.when(
                        (F.col(col_name) >= lo) & (F.col(col_name) <= hi),
                        F.col(col_name),
                    ).otherwise(F.lit(None)),
                )
        return df

    def _cast_booleans(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.types import StringType

        bool_string_cols = [
            f.name
            for f in df.schema.fields
            if f.dataType == StringType() and f.name.startswith("is_")
        ]
        for col in bool_string_cols:
            df = df.withColumn(
                col,
                F.when(F.lower(F.col(col)).isin("true", "1", "yes"), F.lit(True))
                .when(F.lower(F.col(col)).isin("false", "0", "no"), F.lit(False))
                .otherwise(F.lit(None).cast(BooleanType())),
            )
        return df
