"""
Pipeline integration tests for the Gold layer.

Uses PySpark local mode + Delta Lake (in-process).
Writes synthetic Silver Delta data and exercises the full dim_date,
dim_customer, and fact_sales builders end-to-end.

Requires: pyspark==3.5.3, delta-spark==3.2.0
"""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pathlib import Path


def _mock_settings(tmp_path: Path) -> MagicMock:
    settings = MagicMock()
    settings.pipeline_version = "test-gold-1.0"
    settings.postgres_dsn = "postgresql://localhost/test"

    def s3_path(layer: str, table: str) -> str:
        return str(tmp_path / layer / table)

    settings.s3_path = s3_path
    return settings


# ─── Silver fixture data ──────────────────────────────────────────────────────


def _write_silver_customers(spark, path: str) -> None:
    schema = StructType(
        [
            StructField("customer_id", StringType()),
            StructField("email_hash", StringType()),
            StructField("age_bucket", StringType()),
            StructField("segment", StringType()),
            StructField("loyalty_tier", StringType()),
            StructField("loyalty_tier_rank", IntegerType()),
            StructField("loyalty_points", IntegerType()),
            StructField("country", StringType()),
            StructField("state", StringType()),
            StructField("channel_preference", StringType()),
            StructField("is_email_verified", BooleanType()),
            StructField("is_sms_opted_in", BooleanType()),
            StructField("is_active", BooleanType()),
            StructField("years_as_customer", DoubleType()),
            StructField("effective_from", TimestampType()),
            StructField("effective_to", TimestampType()),
            StructField("is_current", BooleanType()),
            StructField("created_at", TimestampType()),
        ]
    )
    now = datetime.datetime(2025, 6, 1, 0, 0, 0)
    rows = [
        (
            "c1",
            "abc123",
            "25-34",
            "gold",
            "gold",
            3,
            500,
            "US",
            "CA",
            "email",
            True,
            False,
            True,
            2.5,
            now,
            None,
            True,
            now,
        ),
        (
            "c2",
            "def456",
            "35-44",
            "silver",
            "silver",
            2,
            200,
            "US",
            "NY",
            "sms",
            True,
            True,
            True,
            1.2,
            now,
            None,
            True,
            now,
        ),
        # Historical (not current) — must be excluded
        (
            "c1",
            "abc123",
            "25-34",
            "bronze",
            "bronze",
            1,
            100,
            "US",
            "CA",
            "email",
            True,
            False,
            False,
            1.5,
            datetime.datetime(2024, 1, 1),
            now,
            False,
            datetime.datetime(2024, 1, 1),
        ),
    ]
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(path)


def _write_silver_orders(spark, path: str) -> None:
    schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("store_id", StringType()),
            StructField("order_date", DateType()),
            StructField("status", StringType()),
            StructField("channel", StringType()),
            StructField("subtotal", DoubleType()),
            StructField("discount_amount", DoubleType()),
            StructField("campaign_id", StringType()),
            StructField("created_at", TimestampType()),
        ]
    )
    now = datetime.datetime(2025, 6, 1, 0, 0, 0)
    rows = [
        (
            "ord-1",
            "c1",
            "st-1",
            datetime.date(2025, 6, 1),
            "confirmed",
            "web",
            150.0,
            10.0,
            None,
            now,
        ),
        (
            "ord-2",
            "c2",
            "st-1",
            datetime.date(2025, 6, 2),
            "delivered",
            "mobile",
            80.0,
            0.0,
            "camp-1",
            now,
        ),
    ]
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(path)


def _write_silver_order_lines(spark, path: str) -> None:
    schema = StructType(
        [
            StructField("order_line_id", StringType()),
            StructField("order_id", StringType()),
            StructField("sku", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("effective_unit_price", DoubleType()),
            StructField("line_total", DoubleType()),
            StructField("is_discounted", BooleanType()),
        ]
    )
    rows = [
        ("ol-1", "ord-1", "SKU-A", 2, 50.0, 47.5, 95.0, True),
        ("ol-2", "ord-1", "SKU-B", 1, 55.0, 55.0, 55.0, False),
        ("ol-3", "ord-2", "SKU-A", 1, 80.0, 80.0, 80.0, False),
    ]
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(path)


# ─── Tests ────────────────────────────────────────────────────────────────────


class TestDimDateBuilder:
    def test_generates_date_range(self, spark, tmp_path):
        from src.processing.gold.dimensions.dim_date import DimDateBuilder

        settings = _mock_settings(tmp_path)
        builder = DimDateBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            result = builder.build()

        assert result["status"] == "success"
        gold_path = str(tmp_path / "gold" / "dim_date")

        dim = spark.read.format("delta").load(gold_path)
        assert dim.count() > 3650  # at least 10 years of days

    def test_sk_date_id_is_yyyymmdd(self, spark, tmp_path):
        from src.processing.gold.dimensions.dim_date import DimDateBuilder

        settings = _mock_settings(tmp_path)
        builder = DimDateBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            builder.build()

        dim = spark.read.format("delta").load(str(tmp_path / "gold" / "dim_date"))
        sample = dim.filter(F.col("full_date") == "2025-06-01").first()
        assert sample["sk_date_id"] == 20250601

    def test_weekend_flag(self, spark, tmp_path):
        from src.processing.gold.dimensions.dim_date import DimDateBuilder

        settings = _mock_settings(tmp_path)
        builder = DimDateBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            builder.build()

        dim = spark.read.format("delta").load(str(tmp_path / "gold" / "dim_date"))
        # 2025-06-01 is a Sunday
        sunday = dim.filter(F.col("full_date") == "2025-06-01").first()
        assert sunday["is_weekend"] == True  # noqa: E712


class TestDimCustomerBuilder:
    def test_only_current_customers(self, spark, tmp_path):
        from src.processing.gold.dimensions.dim_customer import DimCustomerBuilder

        settings = _mock_settings(tmp_path)
        _write_silver_customers(spark, str(tmp_path / "silver" / "customers"))
        builder = DimCustomerBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            builder.build()

        dim = spark.read.format("delta").load(str(tmp_path / "gold" / "dim_customer"))
        # Only 2 current customers (the historical c1 row must be excluded)
        assert dim.count() == 2

    def test_sk_is_long_type(self, spark, tmp_path):
        from src.processing.gold.dimensions.dim_customer import DimCustomerBuilder

        settings = _mock_settings(tmp_path)
        _write_silver_customers(spark, str(tmp_path / "silver" / "customers"))
        builder = DimCustomerBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            builder.build()

        dim = spark.read.format("delta").load(str(tmp_path / "gold" / "dim_customer"))
        assert dim.schema["sk_customer_id"].dataType == LongType()

    def test_nk_customer_id_present(self, spark, tmp_path):
        from src.processing.gold.dimensions.dim_customer import DimCustomerBuilder

        settings = _mock_settings(tmp_path)
        _write_silver_customers(spark, str(tmp_path / "silver" / "customers"))
        builder = DimCustomerBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            builder.build()

        dim = spark.read.format("delta").load(str(tmp_path / "gold" / "dim_customer"))
        assert "nk_customer_id" in dim.columns
        assert "sk_customer_id" in dim.columns


class TestFactSalesBuilder:
    def _setup_silver(self, spark, tmp_path):
        _write_silver_orders(spark, str(tmp_path / "silver" / "orders"))
        _write_silver_order_lines(spark, str(tmp_path / "silver" / "order_lines"))

    def test_fact_rows_equal_order_lines(self, spark, tmp_path):
        from src.processing.gold.facts.fact_sales import FactSalesBuilder

        self._setup_silver(spark, tmp_path)
        settings = _mock_settings(tmp_path)
        builder = FactSalesBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            builder.build()

        fact = spark.read.format("delta").load(str(tmp_path / "gold" / "fact_sales"))
        assert fact.count() == 3  # 3 order lines

    def test_sk_date_id_correct(self, spark, tmp_path):
        from src.processing.gold.facts.fact_sales import FactSalesBuilder

        self._setup_silver(spark, tmp_path)
        settings = _mock_settings(tmp_path)
        builder = FactSalesBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            builder.build()

        fact = spark.read.format("delta").load(str(tmp_path / "gold" / "fact_sales"))
        date_ids = {r["sk_date_id"] for r in fact.select("sk_date_id").collect()}
        assert 20250601 in date_ids
        assert 20250602 in date_ids

    def test_sk_promotion_null_when_no_campaign(self, spark, tmp_path):
        from src.processing.gold.facts.fact_sales import FactSalesBuilder

        self._setup_silver(spark, tmp_path)
        settings = _mock_settings(tmp_path)
        builder = FactSalesBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            builder.build()

        fact = spark.read.format("delta").load(str(tmp_path / "gold" / "fact_sales"))
        # ord-1 has no campaign — its lines should have null sk_promotion_id
        no_promo = fact.filter(F.col("nk_order_id") == "ord-1")
        assert no_promo.filter(F.col("sk_promotion_id").isNull()).count() == 2

    def test_gold_metadata_columns(self, spark, tmp_path):
        from src.processing.gold.facts.fact_sales import FactSalesBuilder

        self._setup_silver(spark, tmp_path)
        settings = _mock_settings(tmp_path)
        builder = FactSalesBuilder(spark=spark, settings=settings)

        with _patch_watermark(builder):
            builder.build()

        fact = spark.read.format("delta").load(str(tmp_path / "gold" / "fact_sales"))
        for col in ("_gold_processed_at", "_gold_date", "_batch_id", "_pipeline_version"):
            assert col in fact.columns


# ─── Helper ───────────────────────────────────────────────────────────────────

from contextlib import contextmanager
from unittest.mock import patch


@contextmanager
def _patch_watermark(builder):
    with patch.object(builder, "_update_watermark", return_value=None):
        yield


class TestFactPartitioning:
    """Facts are partitioned by business month, merge without duplicates, and old layouts migrate."""

    @staticmethod
    def _partition_columns(spark, path: str) -> list[str]:
        from delta import DeltaTable

        return list(DeltaTable.forPath(spark, path).detail().first()["partitionColumns"])

    def _builder(self, spark, tmp_path):
        from src.processing.gold.facts.fact_sales import FactSalesBuilder

        _write_silver_orders(spark, str(tmp_path / "silver" / "orders"))
        _write_silver_order_lines(spark, str(tmp_path / "silver" / "order_lines"))
        return FactSalesBuilder(spark=spark, settings=_mock_settings(tmp_path))

    def test_partitioned_by_business_month(self, spark, tmp_path):
        builder = self._builder(spark, tmp_path)
        with _patch_watermark(builder):
            builder.build()
        path = str(tmp_path / "gold" / "fact_sales")
        assert self._partition_columns(spark, path) == ["_date_month"]
        months = {
            r[0] for r in spark.read.format("delta").load(path).select("_date_month").collect()
        }
        assert months == {202506}

    def test_rebuilding_does_not_duplicate_rows(self, spark, tmp_path):
        builder = self._builder(spark, tmp_path)
        with _patch_watermark(builder):
            builder.build()
            builder.build()
        path = str(tmp_path / "gold" / "fact_sales")
        assert spark.read.format("delta").load(path).count() == 3

    def test_a_table_partitioned_by_processing_day_is_migrated_once(self, spark, tmp_path):
        builder = self._builder(spark, tmp_path)
        with _patch_watermark(builder):
            builder.build()
        path = str(tmp_path / "gold" / "fact_sales")
        legacy = spark.read.format("delta").load(path).drop("_date_month")
        legacy.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).partitionBy("_gold_date").save(path)
        assert self._partition_columns(spark, path) == ["_gold_date"]

        with _patch_watermark(builder):
            builder.build()

        assert self._partition_columns(spark, path) == ["_date_month"]
        assert spark.read.format("delta").load(path).count() == 3
