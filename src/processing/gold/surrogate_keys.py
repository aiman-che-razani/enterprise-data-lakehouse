"""
Surrogate key generation for Gold dimension and fact tables.

Strategy: deterministic xxhash64 of the natural key.

  sk = F.xxhash64(F.col("nk_customer_id"))

Benefits:
  - No lookup table needed (stateless, distributable)
  - Deterministic across runs — same natural key always produces the same SK
  - 64-bit integers — compatible with all warehouse FK columns
  - xxhash64 is a Spark built-in (PySpark 3.x), zero dependency

For composite natural keys (e.g. dim_location: city + state + country):
  sk = F.xxhash64(F.concat_ws("|", F.col("city"), F.col("state"), F.col("country")))

For dim_date the SK is the YYYYMMDD integer (e.g. 20250601), which is both
human-readable and joins cleanly to standard date int columns in warehouses.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType


def add_sk(df: DataFrame, sk_col: str, *nk_cols: str) -> DataFrame:
    """
    Append a surrogate key column derived from one or more natural key columns.

    Single column:  add_sk(df, "sk_customer_id", "nk_customer_id")
    Composite key:  add_sk(df, "sk_location_id", "city", "state", "country")
    """
    if len(nk_cols) == 1:
        key_expr = F.col(nk_cols[0]).cast("string")
    else:
        key_expr = F.concat_ws("|", *[F.col(c).cast("string") for c in nk_cols])

    return df.withColumn(sk_col, F.xxhash64(key_expr).cast(LongType()))


def date_to_sk(date_col: str) -> Column:
    """
    Convert a DateType column to a YYYYMMDD integer surrogate key.

    Usage:
        df = df.withColumn("sk_date_id", date_to_sk("order_date"))
    """
    return (
        F.year(F.col(date_col)) * 10000
        + F.month(F.col(date_col)) * 100
        + F.dayofmonth(F.col(date_col))
    ).cast(IntegerType())


def null_sk() -> Column:
    """Return a null SK (long) for optional FK columns (e.g. campaign on non-promoted orders)."""
    return F.lit(None).cast(LongType())
