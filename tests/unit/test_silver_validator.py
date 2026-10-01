"""
Unit tests for SilverValidator business rules (requires PySpark local mode).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.processing.silver.validator import SilverValidator


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_silver_validator")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


class TestOrdersValidation:
    def _orders_df(self, spark, rows):
        schema = StructType(
            [
                StructField("order_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("total_amount", DoubleType()),
                StructField("subtotal", DoubleType()),
                StructField("status", StringType()),
            ]
        )
        return spark.createDataFrame(rows, schema)

    def test_valid_order_passes(self, spark):
        df = self._orders_df(spark, [("o1", "c1", 100.0, 90.0, "confirmed")])
        validator = SilverValidator()
        valid_df, invalid_df = validator.validate(df, "orders")
        assert valid_df.count() == 1
        assert invalid_df is None

    def test_negative_total_fails(self, spark):
        df = self._orders_df(spark, [("o1", "c1", -5.0, 0.0, "confirmed")])
        validator = SilverValidator()
        valid_df, invalid_df = validator.validate(df, "orders")
        assert valid_df.count() == 0
        assert invalid_df is not None
        assert invalid_df.count() == 1

    def test_invalid_status_fails(self, spark):
        df = self._orders_df(spark, [("o1", "c1", 50.0, 50.0, "INVALID_STATUS")])
        validator = SilverValidator()
        valid_df, invalid_df = validator.validate(df, "orders")
        assert valid_df.count() == 0

    def test_mixed_valid_invalid(self, spark):
        df = self._orders_df(
            spark,
            [
                ("o1", "c1", 100.0, 90.0, "confirmed"),  # valid
                ("o2", "c2", -1.0, 0.0, "cancelled"),  # invalid total
            ],
        )
        validator = SilverValidator()
        valid_df, invalid_df = validator.validate(df, "orders")
        assert valid_df.count() == 1
        assert invalid_df is not None
        assert invalid_df.count() == 1


class TestFinanceValidation:
    def _finance_df(self, spark, rows):
        schema = StructType(
            [
                StructField("transaction_id", StringType()),
                StructField("debit_amount", DoubleType()),
                StructField("credit_amount", DoubleType()),
            ]
        )
        return spark.createDataFrame(rows, schema)

    def test_balanced_entry_passes(self, spark):
        df = self._finance_df(spark, [("t1", 100.0, 100.0)])
        validator = SilverValidator()
        valid_df, invalid_df = validator.validate(df, "finance")
        assert valid_df.count() == 1
        assert invalid_df is None

    def test_unbalanced_entry_fails(self, spark):
        df = self._finance_df(spark, [("t1", 100.0, 99.0)])  # 1.0 gap > 0.01 tolerance
        validator = SilverValidator()
        valid_df, invalid_df = validator.validate(df, "finance")
        assert valid_df.count() == 0
        assert invalid_df is not None


class TestReviewsValidation:
    def _reviews_df(self, spark, rows):
        schema = StructType(
            [
                StructField("review_id", StringType()),
                StructField("rating", IntegerType()),
                StructField("sentiment", StringType()),
            ]
        )
        return spark.createDataFrame(rows, schema)

    def test_valid_review_passes(self, spark):
        df = self._reviews_df(spark, [("r1", 5, "positive")])
        validator = SilverValidator()
        valid_df, invalid_df = validator.validate(df, "reviews")
        assert valid_df.count() == 1

    def test_rating_out_of_range_fails(self, spark):
        df = self._reviews_df(spark, [("r1", 6, "positive")])
        validator = SilverValidator()
        valid_df, invalid_df = validator.validate(df, "reviews")
        assert valid_df.count() == 0

    def test_unknown_domain_passes_all(self, spark):
        schema = StructType([StructField("id", StringType())])
        df = spark.createDataFrame([("x",)], schema)
        validator = SilverValidator()
        valid_df, invalid_df = validator.validate(df, "unknown_domain")
        assert valid_df.count() == 1
        assert invalid_df is None
