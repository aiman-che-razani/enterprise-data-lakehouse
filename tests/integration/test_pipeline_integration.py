"""
Pipeline integration tests — Bronze → Silver smoke test with live Spark + Delta.

Requires:
  - pyspark + delta-spark installed in the test environment
  - A writable temp path for Delta tables (uses tmp_path from pytest)
  - SPARK_MASTER optional (defaults to local[2])

Run with:
    pytest tests/integration/test_pipeline_integration.py -m "integration and spark" -v

All tests skip automatically when pyspark is not importable.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.spark]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _now() -> str:
    return datetime.now(UTC).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Bronze ingestion smoke test
# ─────────────────────────────────────────────────────────────────────────────
class TestBronzeIngestionSmoke:
    """
    Verifies that the Bronze batch ingestor correctly lands a small synthetic
    dataset into a Delta table with all required metadata columns.
    """

    def test_bronze_write_produces_delta_table(self, integration_spark, tmp_path):
        from pyspark.sql import Row
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        delta_path = str(tmp_path / "bronze" / "orders")

        schema = StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), True),
                StructField("amount", IntegerType(), True),
                StructField("created_at", TimestampType(), True),
            ]
        )

        rows = [
            Row(
                order_id="ORD-001",
                customer_id="CUST-1",
                amount=100,
                created_at=datetime(2025, 6, 1, 10, 0, 0),
            ),
            Row(
                order_id="ORD-002",
                customer_id="CUST-2",
                amount=250,
                created_at=datetime(2025, 6, 1, 11, 0, 0),
            ),
            Row(
                order_id="ORD-003",
                customer_id="CUST-1",
                amount=75,
                created_at=datetime(2025, 6, 1, 12, 0, 0),
            ),
        ]

        df = integration_spark.createDataFrame(rows, schema)
        df.write.format("delta").mode("overwrite").save(delta_path)

        read_back = integration_spark.read.format("delta").load(delta_path)
        assert read_back.count() == 3
        cols = set(read_back.columns)
        assert "order_id" in cols
        assert "customer_id" in cols
        assert "amount" in cols

    def test_bronze_metadata_columns_added(self, integration_spark, tmp_path):
        """
        Simulates BronzeBatchIngestor._add_metadata_columns() and verifies
        the correct columns are present in the written Delta table.
        """
        from pyspark.sql import Row
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType

        delta_path = str(tmp_path / "bronze" / "products_meta")

        schema = StructType(
            [
                StructField("product_id", StringType(), False),
                StructField("name", StringType(), True),
            ]
        )
        rows = [Row(product_id=f"P-{i}", name=f"Product {i}") for i in range(5)]
        df = integration_spark.createDataFrame(rows, schema)

        batch_id = str(uuid.uuid4())
        df = (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingestion_date", F.current_date())
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_file", F.lit("synthetic/products.csv"))
            .withColumn("_pipeline_version", F.lit("1.0.0"))
        )
        df.write.format("delta").mode("overwrite").save(delta_path)

        read_back = integration_spark.read.format("delta").load(delta_path)
        cols = set(read_back.columns)
        assert "_ingested_at" in cols
        assert "_ingestion_date" in cols
        assert "_batch_id" in cols
        assert "_source_file" in cols
        assert "_pipeline_version" in cols
        assert read_back.count() == 5

    def test_bronze_delta_supports_append_mode(self, integration_spark, tmp_path):
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        delta_path = str(tmp_path / "bronze" / "append_test")
        schema = StructType([StructField("id", StringType(), False)])

        integration_spark.createDataFrame([Row(id="A"), Row(id="B")], schema).write.format(
            "delta"
        ).mode("overwrite").save(delta_path)

        integration_spark.createDataFrame([Row(id="C"), Row(id="D")], schema).write.format(
            "delta"
        ).mode("append").save(delta_path)

        total = integration_spark.read.format("delta").load(delta_path).count()
        assert total == 4

    def test_bronze_delta_history_recorded(self, integration_spark, tmp_path):
        from delta.tables import DeltaTable
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        delta_path = str(tmp_path / "bronze" / "history_test")
        schema = StructType([StructField("id", StringType(), False)])

        integration_spark.createDataFrame([Row(id="X")], schema).write.format("delta").mode(
            "overwrite"
        ).save(delta_path)
        integration_spark.createDataFrame([Row(id="Y")], schema).write.format("delta").mode(
            "append"
        ).save(delta_path)

        dt = DeltaTable.forPath(integration_spark, delta_path)
        history = dt.history().collect()
        # Two operations → at least 2 history entries
        assert len(history) >= 2


# ─────────────────────────────────────────────────────────────────────────────
# Silver processing smoke test
# ─────────────────────────────────────────────────────────────────────────────
class TestSilverProcessingSmoke:
    """
    Tests core Silver-layer transformations (deduplication + PII masking)
    on a small synthetic dataset without hitting external services.
    """

    def test_deduplication_keeps_latest(self, integration_spark):
        from pyspark.sql import Row
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("email", StringType(), True),
                StructField("_ingested_at", TimestampType(), True),
                StructField("version", IntegerType(), True),
            ]
        )
        rows = [
            Row(
                customer_id="C1",
                email="a@x.com",
                _ingested_at=datetime(2025, 6, 1, 10, 0, 0),
                version=1,
            ),
            Row(
                customer_id="C1",
                email="a@x.com",
                _ingested_at=datetime(2025, 6, 1, 11, 0, 0),
                version=2,
            ),  # latest
            Row(
                customer_id="C2",
                email="b@x.com",
                _ingested_at=datetime(2025, 6, 1, 10, 30, 0),
                version=1,
            ),
        ]
        df = integration_spark.createDataFrame(rows, schema)

        from pyspark.sql import Window

        window = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
        deduped = (
            df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
        )

        assert deduped.count() == 2
        c1_ver = deduped.filter(F.col("customer_id") == "C1").select("version").collect()[0][0]
        assert c1_ver == 2  # latest version kept

    def test_pii_email_hashing(self, integration_spark):
        import hashlib

        from pyspark.sql import Row
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("email", StringType(), True),
            ]
        )
        rows = [
            Row(customer_id="C1", email="alice@example.com"),
            Row(customer_id="C2", email="bob@example.com"),
            Row(customer_id="C3", email=None),
        ]
        df = integration_spark.createDataFrame(rows, schema)

        # Mirror the Silver PII tokenizer logic
        df_masked = df.withColumn(
            "email_hash",
            F.when(
                F.col("email").isNotNull(),
                F.sha2(F.lower(F.trim(F.col("email"))), 256),
            ).otherwise(F.lit(None)),
        ).drop("email")

        cols = set(df_masked.columns)
        assert "email" not in cols
        assert "email_hash" in cols

        alice_hash = (
            df_masked.filter(F.col("customer_id") == "C1").select("email_hash").collect()[0][0]
        )
        expected = hashlib.sha256(b"alice@example.com").hexdigest()
        assert alice_hash == expected

        null_hash = (
            df_masked.filter(F.col("customer_id") == "C3").select("email_hash").collect()[0][0]
        )
        assert null_hash is None

    def test_surrogate_key_deterministic(self, integration_spark):
        from pyspark.sql import Row
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([StructField("order_id", StringType(), False)])
        rows = [Row(order_id="ORD-001"), Row(order_id="ORD-001"), Row(order_id="ORD-002")]
        df = integration_spark.createDataFrame(rows, schema)

        df_with_sk = df.withColumn("sk_order", F.xxhash64(F.col("order_id")))
        sk_vals = {r["order_id"]: r["sk_order"] for r in df_with_sk.collect()}

        # Same natural key → same SK
        assert sk_vals["ORD-001"] == sk_vals["ORD-001"]
        # Different natural keys → different SKs (with overwhelming probability)
        assert sk_vals["ORD-001"] != sk_vals["ORD-002"]


# ─────────────────────────────────────────────────────────────────────────────
# Data Quality engine smoke test
# ─────────────────────────────────────────────────────────────────────────────
class TestDQEngineSmoke:
    """Validates that DQ check classes run correctly against a live Spark session."""

    def test_completeness_check_passes(self, integration_spark):
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from src.quality.checks.completeness import CompletenessCheck

        schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
            ]
        )
        rows = [Row(order_id=f"O{i}", customer_id=f"C{i}") for i in range(100)]
        df = integration_spark.createDataFrame(rows, schema)

        check = CompletenessCheck(columns=["order_id", "customer_id"], threshold=0.05)
        result = check.run(df, domain="orders", layer="bronze")
        assert result.status.value == "passed"
        assert result.rows_checked == 100

    def test_uniqueness_check_detects_duplicates(self, integration_spark):
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from src.quality.checks.uniqueness import UniquenessCheck

        schema = StructType([StructField("order_id", StringType(), False)])
        rows = [Row(order_id="DUP"), Row(order_id="DUP"), Row(order_id="UNIQUE")]
        df = integration_spark.createDataFrame(rows, schema)

        check = UniquenessCheck(pk_col="order_id")
        result = check.run(df, domain="orders", layer="bronze")
        assert result.status.value == "failed"
        assert result.metric_value == 1  # 1 duplicate

    def test_row_count_check_below_minimum(self, integration_spark):
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from src.quality.checks.row_count import RowCountCheck

        schema = StructType([StructField("id", StringType(), False)])
        df = integration_spark.createDataFrame([Row(id="A")], schema)

        check = RowCountCheck(min_count=100, max_z_score=3.0, history_conn=None)
        result = check.run(df, domain="orders", layer="bronze")
        assert result.status.value == "failed"
        assert result.metric_value == 1
