"""
PII tokenisation for the Silver layer.

All tokenisation is deterministic and one-way:
  - hash strategy → keyed SHA-256: sha256("<key>|<lowercased, stripped value>").
    The key (``Settings.pii_hash_key``, from Secrets Manager in prod) stops a dictionary
    attack against known emails / IPv4 addresses: without it the hash cannot be recomputed.
    Deterministic, so cross-domain joins on ``<col>_hash`` still work.
  - drop strategy → column removed
  - date_of_birth → 10-year age bucket string (e.g., "25-34")

Hashing uses Spark's built-in ``sha2`` (no Python worker), so it is fast and JVM-only.
Rotating the key changes every hash; treat it like any other re-tokenisation migration.
"""

from __future__ import annotations

import hashlib
from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.common.logging import get_logger

logger = get_logger(__name__)

_TS_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

# ─── Spark UDFs ──────────────────────────────────────────────────────────────

# Development-only fallback; Settings rejects it in production.
DEV_HASH_KEY = "dev-only-pii-hash-key"


def keyed_hash_column(col: Column, key: str) -> Column:
    """Spark expression for the keyed SHA-256 of *col* (NULL stays NULL)."""
    normalised = F.lower(F.trim(col.cast("string")))
    return F.when(col.isNull(), F.lit(None).cast("string")).otherwise(
        F.sha2(F.concat_ws("|", F.lit(key), normalised), 256)
    )


@F.udf(StringType())
def _age_bucket_udf(dob_str: str | None) -> str | None:
    """Convert ISO date-of-birth string to a 10-year age bucket."""
    if dob_str is None:
        return "Unknown"
    try:
        dob = date.fromisoformat(str(dob_str)[:10])
        age = (date.today() - dob).days // 365
        for lo, hi in [(0, 18), (18, 25), (25, 35), (35, 45), (45, 55), (55, 65), (65, 75)]:
            if lo <= age < hi:
                return f"{lo}-{hi - 1}"
        return "75+"
    except (ValueError, TypeError):
        return "Unknown"


# ─── Public API ───────────────────────────────────────────────────────────────


class PiiTokenizer:
    """
    Applies PII tokenisation rules to a DataFrame.

    Each domain has its own rule set defined in SILVER_PII_HANDLING.
    The tokenizer reads those rules and applies UDFs / column drops accordingly.
    """

    def __init__(self, hash_key: str = DEV_HASH_KEY) -> None:
        if not hash_key:
            raise ValueError("PII hash key must be non-empty")
        self._hash_key = hash_key

    def tokenize(self, df: DataFrame, domain: str) -> DataFrame:
        from src.processing.silver.domain_meta import SILVER_PII_HANDLING

        rules = SILVER_PII_HANDLING.get(domain)
        if not rules:
            return df

        for col_name, strategy in rules.items():
            if col_name not in df.columns:
                continue

            if strategy == "hash":
                df = df.withColumn(
                    f"{col_name}_hash", keyed_hash_column(F.col(col_name), self._hash_key)
                ).drop(col_name)
                logger.debug("pii_hashed", domain=domain, column=col_name)

            elif strategy == "drop":
                df = df.drop(col_name)
                logger.debug("pii_dropped", domain=domain, column=col_name)

            elif strategy == "age_bucket":
                df = df.withColumn("age_bucket", _age_bucket_udf(F.col(col_name))).drop(col_name)
                logger.debug("pii_age_bucketed", domain=domain, column=col_name)

        return df


# ─── Pure-Python helpers (used in unit tests without PySpark) ─────────────────


def sha256_email(email: str) -> str:
    """UNKEYED SHA-256 of a normalised value. Kept for tests and one-off migrations only —
    Silver uses :func:`keyed_hash` so hashes cannot be reversed by dictionary attack."""
    return hashlib.sha256(email.lower().strip().encode("utf-8")).hexdigest()


def keyed_hash(value: str | None, key: str) -> str | None:
    """Pure-Python twin of :func:`keyed_hash_column` (same output for the same input)."""
    if value is None:
        return None
    return hashlib.sha256(f"{key}|{value.lower().strip()}".encode()).hexdigest()


def age_bucket(dob: date | str | None) -> str:
    """Return 10-year age bucket for a date-of-birth."""
    if dob is None:
        return "Unknown"
    if isinstance(dob, str):
        try:
            dob = date.fromisoformat(dob[:10])
        except ValueError:
            return "Unknown"
    age = (date.today() - dob).days // 365
    for lo, hi in [(0, 18), (18, 25), (25, 35), (35, 45), (45, 55), (55, 65), (65, 75)]:
        if lo <= age < hi:
            return f"{lo}-{hi - 1}"
    return "75+"
