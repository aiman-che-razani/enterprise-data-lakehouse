"""
Unit tests for SilverDeduplicator (requires PySpark local mode).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.processing.silver.deduplicator import SilverDeduplicator


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_silver_deduplicator")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


class TestSilverDeduplicator:
    def _df(self, spark, rows):
        schema = StructType(
            [
                StructField("customer_id", StringType()),
                StructField("segment", StringType()),
                StructField("_ingested_at", StringType()),  # string simulating timestamp
            ]
        )
        return spark.createDataFrame(rows, schema)

    def test_single_row_unchanged(self, spark):
        df = self._df(spark, [("c1", "gold", "2025-01-02T10:00:00Z")])
        deduped = SilverDeduplicator().deduplicate(df, "customer_id")
        assert deduped.count() == 1

    def test_keeps_latest_ingestion(self, spark):
        df = self._df(
            spark,
            [
                ("c1", "bronze", "2025-01-01T00:00:00Z"),
                ("c1", "gold", "2025-01-02T00:00:00Z"),
            ],
        )
        deduped = SilverDeduplicator().deduplicate(df, "customer_id")
        assert deduped.count() == 1
        row = deduped.first()
        assert row["segment"] == "gold"

    def test_multiple_distinct_pks_all_kept(self, spark):
        df = self._df(
            spark,
            [
                ("c1", "gold", "2025-01-01T00:00:00Z"),
                ("c2", "silver", "2025-01-01T00:00:00Z"),
                ("c3", "bronze", "2025-01-01T00:00:00Z"),
            ],
        )
        deduped = SilverDeduplicator().deduplicate(df, "customer_id")
        assert deduped.count() == 3

    def test_missing_pk_col_returns_unchanged(self, spark):
        df = self._df(spark, [("c1", "gold", "2025-01-01T00:00:00Z")])
        deduped = SilverDeduplicator().deduplicate(df, "nonexistent_id")
        assert deduped.count() == 1

    def test_count_duplicates(self, spark):
        df = self._df(
            spark,
            [
                ("c1", "bronze", "2025-01-01T00:00:00Z"),
                ("c1", "gold", "2025-01-02T00:00:00Z"),
                ("c2", "silver", "2025-01-01T00:00:00Z"),
            ],
        )
        dupes = SilverDeduplicator().count_duplicates(df, "customer_id")
        assert dupes == 1
