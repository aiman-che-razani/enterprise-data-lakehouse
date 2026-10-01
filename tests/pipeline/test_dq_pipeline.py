"""
PySpark integration tests for individual DQ check classes.

These tests create real (in-process) DataFrames and verify that each check
class returns correctly-typed CheckResult objects.  No external Delta tables
or PostgreSQL connections are needed.

Requires PySpark 3.5 + Delta Lake 3.2 on the PYTHONPATH.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.quality.checks.base_check import CheckStatus
from src.quality.checks.completeness import CompletenessCheck
from src.quality.checks.freshness import FreshnessCheck
from src.quality.checks.row_count import RowCountCheck
from src.quality.checks.schema_check import SchemaCheck
from src.quality.checks.uniqueness import UniquenessCheck
from src.quality.checks.value_range import ValueRangeCheck


# ─────────────────────────────────────────────────────────────────────────────
# CompletenessCheck
# ─────────────────────────────────────────────────────────────────────────────
class TestCompletenessCheck:
    def test_passed_when_no_nulls(self, spark):
        df = spark.createDataFrame([Row(order_id="A"), Row(order_id="B"), Row(order_id="C")])
        results = CompletenessCheck(["order_id"], threshold=0.0).run(df, "orders", "bronze")
        assert len(results) == 1
        assert results[0].status == CheckStatus.PASSED
        assert results[0].metric_value == pytest.approx(0.0)

    def test_failed_when_null_rate_exceeds_threshold(self, spark):
        schema = StructType([StructField("email", StringType(), True)])
        df = spark.createDataFrame(
            [Row(email="a@b.com"), Row(email=None), Row(email=None)], schema=schema
        )
        results = CompletenessCheck(["email"], threshold=0.1).run(df, "customers", "bronze")
        assert len(results) == 1
        r = results[0]
        assert r.status == CheckStatus.FAILED
        assert r.column_name == "email"
        assert r.metric_value == pytest.approx(2 / 3, rel=1e-3)

    def test_passed_within_threshold(self, spark):
        schema = StructType([StructField("phone", StringType(), True)])
        df = spark.createDataFrame([Row(phone="123"), Row(phone=None)], schema=schema)
        # 50 % nulls — allowed when threshold=0.6
        results = CompletenessCheck(["phone"], threshold=0.6).run(df, "customers", "bronze")
        assert results[0].status == CheckStatus.PASSED

    def test_absent_column_silently_skipped(self, spark):
        df = spark.createDataFrame([Row(x=1)])
        results = CompletenessCheck(["nonexistent_col"], threshold=0.0).run(df, "orders", "bronze")
        assert results == []

    def test_empty_dataframe_returns_empty(self, spark):
        schema = StructType([StructField("order_id", StringType(), True)])
        df = spark.createDataFrame([], schema=schema)
        results = CompletenessCheck(["order_id"], threshold=0.0).run(df, "orders", "bronze")
        assert results == []


# ─────────────────────────────────────────────────────────────────────────────
# UniquenessCheck
# ─────────────────────────────────────────────────────────────────────────────
class TestUniquenessCheck:
    def test_passed_for_unique_pk(self, spark):
        df = spark.createDataFrame([Row(order_id="A"), Row(order_id="B"), Row(order_id="C")])
        results = UniquenessCheck("order_id").run(df, "orders", "bronze")
        assert len(results) == 1
        r = results[0]
        assert r.status == CheckStatus.PASSED
        assert r.metric_value == pytest.approx(0.0)

    def test_failed_when_duplicates_present(self, spark):
        df = spark.createDataFrame([Row(order_id="A"), Row(order_id="A"), Row(order_id="B")])
        results = UniquenessCheck("order_id").run(df, "orders", "bronze")
        r = results[0]
        assert r.status == CheckStatus.FAILED
        assert r.metric_value == pytest.approx(1.0)  # 1 duplicate
        assert "duplicate" in r.message.lower()

    def test_error_when_column_missing(self, spark):
        df = spark.createDataFrame([Row(other="X")])
        results = UniquenessCheck("order_id").run(df, "orders", "bronze")
        assert results[0].status == CheckStatus.ERROR


# ─────────────────────────────────────────────────────────────────────────────
# FreshnessCheck
# ─────────────────────────────────────────────────────────────────────────────
class TestFreshnessCheck:
    def _df_with_ts(self, spark, age_hours: float):
        ts = datetime.now(UTC) - timedelta(hours=age_hours)
        schema = StructType([StructField("created_at", TimestampType(), True)])
        return spark.createDataFrame([Row(created_at=ts.replace(tzinfo=None))], schema=schema)

    def test_passed_within_sla(self, spark):
        df = self._df_with_ts(spark, age_hours=10.0)
        results = FreshnessCheck("created_at", max_age_hours=26.0).run(df, "orders", "bronze")
        assert results[0].status == CheckStatus.PASSED

    def test_failed_when_exceeds_sla(self, spark):
        df = self._df_with_ts(spark, age_hours=30.0)
        results = FreshnessCheck("created_at", max_age_hours=26.0).run(df, "orders", "bronze")
        r = results[0]
        assert r.status == CheckStatus.FAILED
        assert r.metric_value > 26.0

    def test_failed_on_empty_table(self, spark):
        schema = StructType([StructField("created_at", TimestampType(), True)])
        df = spark.createDataFrame([], schema=schema)
        results = FreshnessCheck("created_at", max_age_hours=26.0).run(df, "orders", "bronze")
        assert results[0].status == CheckStatus.FAILED
        assert results[0].rows_checked == 0

    def test_error_when_column_missing(self, spark):
        df = spark.createDataFrame([Row(other="X")])
        results = FreshnessCheck("created_at", max_age_hours=26.0).run(df, "orders", "bronze")
        assert results[0].status == CheckStatus.ERROR


# ─────────────────────────────────────────────────────────────────────────────
# RowCountCheck
# ─────────────────────────────────────────────────────────────────────────────
class TestRowCountCheck:
    def test_passed_when_above_minimum(self, spark):
        df = spark.createDataFrame([Row(x=i) for i in range(100)])
        results = RowCountCheck(min_count=1).run(df, "orders", "bronze")
        min_result = next(r for r in results if "minimum" in r.check_name)
        assert min_result.status == CheckStatus.PASSED

    def test_failed_when_empty(self, spark):
        schema = StructType([StructField("x", IntegerType(), True)])
        df = spark.createDataFrame([], schema=schema)
        results = RowCountCheck(min_count=1).run(df, "orders", "bronze")
        min_result = next(r for r in results if "minimum" in r.check_name)
        assert min_result.status == CheckStatus.FAILED
        assert min_result.metric_value == pytest.approx(0.0)

    def test_no_anomaly_check_without_connection(self, spark):
        df = spark.createDataFrame([Row(x=1)])
        results = RowCountCheck(min_count=1, history_conn=None).run(df, "orders", "bronze")
        # Only minimum check is returned when no history connection
        assert all("anomaly" not in r.check_name for r in results)


# ─────────────────────────────────────────────────────────────────────────────
# SchemaCheck
# ─────────────────────────────────────────────────────────────────────────────
class TestSchemaCheck:
    def test_passed_when_all_required_present(self, spark):
        df = spark.createDataFrame([Row(order_id="A", customer_id="C1")])
        results = SchemaCheck(required_columns=["order_id", "customer_id"]).run(
            df, "orders", "bronze"
        )
        assert any(r.status == CheckStatus.PASSED for r in results)
        assert all(r.status != CheckStatus.FAILED for r in results)

    def test_failed_when_required_column_missing(self, spark):
        df = spark.createDataFrame([Row(order_id="A")])
        results = SchemaCheck(required_columns=["order_id", "customer_id"]).run(
            df, "orders", "bronze"
        )
        failed = [r for r in results if r.status == CheckStatus.FAILED]
        assert any(r.column_name == "customer_id" for r in failed)

    def test_extra_column_is_warning_not_failed(self, spark):
        expected = StructType(
            [
                StructField("order_id", StringType(), True),
            ]
        )
        df = spark.createDataFrame([Row(order_id="A", extra_col="X")])
        results = SchemaCheck(expected=expected).run(df, "orders", "bronze")
        statuses = {r.check_name: r.status for r in results}
        assert statuses.get("schema.extra_column") == CheckStatus.WARNING

    def test_type_mismatch_is_failed(self, spark):
        expected = StructType(
            [
                StructField("quantity", StringType(), True),  # expect string
            ]
        )
        df = spark.createDataFrame([Row(quantity=5)])  # actual: integer
        results = SchemaCheck(expected=expected).run(df, "orders", "bronze")
        failed = [r for r in results if r.status == CheckStatus.FAILED]
        assert any("type_mismatch" in r.check_name for r in failed)


# ─────────────────────────────────────────────────────────────────────────────
# ValueRangeCheck
# ─────────────────────────────────────────────────────────────────────────────
class TestValueRangeCheck:
    def test_passed_when_all_in_range(self, spark):
        schema = StructType([StructField("unit_price", DoubleType(), True)])
        df = spark.createDataFrame([Row(unit_price=10.0), Row(unit_price=50.0)], schema=schema)
        results = ValueRangeCheck({"unit_price": (0.01, 1000.0)}).run(df, "products", "bronze")
        assert results[0].status == CheckStatus.PASSED

    def test_failed_when_below_lower_bound(self, spark):
        schema = StructType([StructField("rating", DoubleType(), True)])
        df = spark.createDataFrame(
            [Row(rating=1.0), Row(rating=0.0)], schema=schema  # 0.0 < 1.0 lower bound
        )
        results = ValueRangeCheck({"rating": (1.0, 5.0)}).run(df, "reviews", "bronze")
        assert results[0].status == CheckStatus.FAILED
        assert "min" in results[0].message

    def test_failed_when_above_upper_bound(self, spark):
        schema = StructType([StructField("rating", DoubleType(), True)])
        df = spark.createDataFrame([Row(rating=5.0), Row(rating=6.0)], schema=schema)
        results = ValueRangeCheck({"rating": (1.0, 5.0)}).run(df, "reviews", "bronze")
        assert results[0].status == CheckStatus.FAILED
        assert "max" in results[0].message

    def test_absent_column_skipped(self, spark):
        df = spark.createDataFrame([Row(other=1.0)])
        results = ValueRangeCheck({"nonexistent": (0.0, 100.0)}).run(df, "orders", "bronze")
        assert results == []

    def test_none_bound_skips_that_side(self, spark):
        schema = StructType([StructField("total", DoubleType(), True)])
        df = spark.createDataFrame([Row(total=9_999_999.0)], schema=schema)
        # Only lower bound — no upper bound check
        results = ValueRangeCheck({"total": (0.0, None)}).run(df, "orders", "bronze")
        assert results[0].status == CheckStatus.PASSED


class TestReferentialCheck:
    @staticmethod
    def _parent(spark, tmp_path, keys):
        path = str(tmp_path / "parent")
        spark.createDataFrame([Row(store_id=k) for k in keys]).write.format("delta").save(path)
        return path

    def test_many_child_rows_sharing_a_valid_key_are_not_orphans(self, spark, tmp_path):
        """Regression: exceptAll reported 19,990 'orphans' for 20,000 rows over 10 valid keys."""
        from src.quality.checks.referential import ReferentialCheck

        parent = self._parent(spark, tmp_path, ["S1", "S2"])
        child = spark.createDataFrame([Row(store_id="S1")] * 500 + [Row(store_id="S2")] * 500)
        [result] = ReferentialCheck("store_id", parent, "store_id", spark).run(
            child, "iot", "silver"
        )
        assert result.status == CheckStatus.PASSED
        assert result.metric_value == 0.0

    def test_rows_with_an_unknown_key_are_counted_as_orphans(self, spark, tmp_path):
        from src.quality.checks.referential import ReferentialCheck

        parent = self._parent(spark, tmp_path, ["S1"])
        child = spark.createDataFrame(
            [Row(store_id="S1")] * 3 + [Row(store_id="GHOST")] * 4 + [Row(store_id=None)]
        )
        [result] = ReferentialCheck("store_id", parent, "store_id", spark).run(
            child, "iot", "silver"
        )
        assert result.status == CheckStatus.FAILED
        assert result.metric_value == 4.0  # the NULL is not a foreign-key violation


class TestFreshnessOnStringTimestamps:
    def test_bronze_style_iso_string_timestamps_are_evaluated(self, spark):
        """Regression: Bronze stores timestamps as strings, and max() of a string crashed the whole run
        ("unsupported operand type(s) for -: 'datetime.datetime' and 'str'")."""
        recent = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        df = spark.createDataFrame([Row(created_at="2020-01-01T00:00:00Z"), Row(created_at=recent)])
        [result] = FreshnessCheck("created_at", max_age_hours=24).run(df, "orders", "bronze")
        assert result.status == CheckStatus.PASSED
        assert result.metric_value < 24

    def test_a_stale_string_timestamp_fails(self, spark):
        df = spark.createDataFrame([Row(created_at="2020-01-01T00:00:00Z")])
        [result] = FreshnessCheck("created_at", max_age_hours=24).run(df, "orders", "bronze")
        assert result.status == CheckStatus.FAILED
