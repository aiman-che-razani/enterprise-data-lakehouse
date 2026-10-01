"""
Data-contract enforcement on a real Spark session, using the Silver-shaped fixtures in
tests/fixtures/data. DataFrames come from spark.read.json / SQL (JVM-only paths).

    pytest tests/integration/spark -m "integration and spark"
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.spark]

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "data"


def _silver_orders(spark, filename: str):
    """Fixture rows cast to Silver types and decorated with the Silver metadata columns."""
    from pyspark.sql import functions as F

    df = (
        spark.read.json(str((_FIXTURES / filename).resolve()))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    now = F.current_timestamp()
    return (
        df.withColumn("_bronze_version", F.lit(3).cast("bigint"))
        .withColumn("_silver_processed_at", now)
        .withColumn("_silver_date", F.to_date(now))
        .withColumn("_batch_id", F.lit("itest"))
        .withColumn("_pipeline_version", F.lit("1.0.0"))
    )


def test_clean_silver_orders_satisfy_the_published_contract(local_spark):
    from src.quality.contracts import ContractEnforcer

    report = ContractEnforcer(mode="enforce").check(
        _silver_orders(local_spark, "silver_orders_sample.json"), "orders"
    )
    assert report.passed and report.rows_checked == 3 and report.violations == []


def test_violations_are_reported_with_row_counts(local_spark):
    from src.quality.contracts import ContractEnforcer

    report = ContractEnforcer(mode="warn").check(
        _silver_orders(local_spark, "silver_orders_violations.json"), "orders"
    )
    found = {(v.rule, v.column): v.offending_rows for v in report.violations}
    assert found[("null_in_required", "customer_id")] == 1
    assert found[("invalid_value", "status")] == 1
    assert found[("below_minimum", "total_amount")] == 1
    assert found[("duplicate_primary_key", "order_id")] == 1
    assert not report.passed


def test_enforce_mode_blocks_the_run(local_spark):
    from src.quality.contracts import ContractEnforcer, ContractViolationError

    with pytest.raises(ContractViolationError, match="orders@"):
        ContractEnforcer(mode="enforce").check(
            _silver_orders(local_spark, "silver_orders_violations.json"), "orders"
        )


def test_pii_columns_left_in_silver_are_caught(local_spark):
    from pyspark.sql import functions as F

    from src.quality.contracts import ContractEnforcer

    df = _silver_orders(local_spark, "silver_orders_sample.json")
    # Build a customers-shaped frame that (wrongly) still carries raw PII.
    leaky = (
        df.select(
            F.col("customer_id"),
            F.col("created_at"),
            F.col("updated_at"),
            F.col("_bronze_version"),
            F.col("_silver_processed_at"),
            F.col("_silver_date"),
            F.col("_batch_id"),
            F.col("_pipeline_version"),
        )
        .withColumn("email", F.lit("a@b.c"))
        .withColumn("phone", F.lit("555"))
    )
    report = ContractEnforcer(mode="warn").check(leaky, "customers")
    forbidden = {v.column for v in report.violations if v.rule == "forbidden_column"}
    assert {"email", "phone"} <= forbidden
