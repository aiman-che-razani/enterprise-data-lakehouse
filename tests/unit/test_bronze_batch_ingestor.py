"""
Unit tests for the Bronze batch ingestor and supporting components.

Tests use PySpark in local mode — no MinIO or PostgreSQL required.
All S3 writes are redirected to a temporary local directory.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.processing.bronze.domain_meta import (
    BATCH_INGESTION_ORDER,
    DOMAIN_FORMATS,
    DOMAIN_PKS,
    DOMAIN_ZORDER,
)

# ─── Domain metadata ──────────────────────────────────────────────────────────


class TestDomainMeta:
    def test_all_domains_have_pk(self):
        for domain in BATCH_INGESTION_ORDER:
            assert domain in DOMAIN_PKS, f"Missing PK for {domain}"

    def test_all_domains_have_format(self):
        for domain in BATCH_INGESTION_ORDER:
            assert domain in DOMAIN_FORMATS, f"Missing format for {domain}"

    def test_all_domains_have_zorder(self):
        for domain in BATCH_INGESTION_ORDER:
            assert domain in DOMAIN_ZORDER, f"Missing ZORDER for {domain}"

    def test_zorder_first_col_is_pk_for_most_domains(self):
        exceptions = {"products"}  # products PK = "sku", same as ZORDER first col
        for domain in BATCH_INGESTION_ORDER:
            pk = DOMAIN_PKS[domain]
            zorder = DOMAIN_ZORDER[domain]
            assert len(zorder) >= 1
            # First Z-ORDER column should match or relate to PK
            assert zorder[0] == pk or domain in exceptions or pk in zorder

    def test_formats_are_valid(self):
        valid = {"json", "csv", "parquet"}
        for domain, fmt in DOMAIN_FORMATS.items():
            assert fmt in valid, f"Invalid format '{fmt}' for {domain}"

    def test_batch_ingestion_order_contains_all_domains(self):
        assert set(BATCH_INGESTION_ORDER) == set(DOMAIN_PKS.keys())

    def test_orders_before_order_lines(self):
        assert BATCH_INGESTION_ORDER.index("orders") < BATCH_INGESTION_ORDER.index("order_lines")

    def test_products_before_inventory(self):
        assert BATCH_INGESTION_ORDER.index("products") < BATCH_INGESTION_ORDER.index("inventory")

    def test_finance_is_parquet(self):
        assert DOMAIN_FORMATS["finance"] == "parquet"

    def test_clickstream_is_json(self):
        assert DOMAIN_FORMATS["clickstream"] == "json"


# ─── Quality gates (unit) ─────────────────────────────────────────────────────


class TestQualityResult:
    def test_to_dict_includes_check_name(self):
        from src.processing.bronze.quality_gates import QualityResult

        result = QualityResult("pk_not_null", passed=True, metrics={"null_count": 0})
        d = result.to_dict()
        assert d["check"] == "pk_not_null"
        assert d["passed"] is True
        assert d["null_count"] == 0

    def test_failed_result(self):
        from src.processing.bronze.quality_gates import QualityResult

        result = QualityResult("null_rates", passed=False, metrics={"violations": {"email": 0.10}})
        assert result.passed is False
        assert "violations" in result.to_dict()


class TestBronzeQualityGates:
    """Tests that require PySpark — skipped if pyspark not installed."""

    @pytest.fixture(autouse=True)
    def _require_pyspark(self):
        pytest.importorskip("pyspark")

    @pytest.fixture()
    def spark(self):
        from pyspark.sql import SparkSession

        return (
            SparkSession.builder.master("local[1]")
            .appName("test-quality-gates")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    def test_split_valid_records_all_good(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType

        from src.processing.bronze.quality_gates import BronzeQualityGates

        schema = StructType(
            [
                StructField("supplier_id", StringType()),
                StructField("name", StringType()),
            ]
        )
        data = [("SUP-000001", "Acme"), ("SUP-000002", "Beta")]
        df = spark.createDataFrame(data, schema)

        gates = BronzeQualityGates()
        good, bad = gates.split_valid_records(df, schema, "suppliers")
        assert good.count() == 2
        assert bad is None

    def test_split_valid_records_with_corrupt(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType

        from src.processing.bronze.quality_gates import BronzeQualityGates

        schema_with_corrupt = StructType(
            [
                StructField("supplier_id", StringType()),
                StructField("name", StringType()),
                StructField("_corrupt_record", StringType()),
            ]
        )
        data = [
            ("SUP-000001", "Acme", None),
            (None, None, '{"bad": "json"'),  # corrupt row
        ]
        df = spark.createDataFrame(data, schema_with_corrupt)

        gates = BronzeQualityGates()
        good, bad = gates.split_valid_records(df, schema_with_corrupt, "suppliers")
        assert good.count() == 1
        assert bad is not None
        assert bad.count() == 1

    def test_pk_null_check_fails_when_pk_is_null(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType

        from src.processing.bronze.quality_gates import BronzeQualityGates

        schema = StructType([StructField("order_id", StringType())])
        df = spark.createDataFrame([(None,), ("ORD-0000000001",)], schema)

        gates = BronzeQualityGates()
        result = gates._check_pk_nulls(df, "orders")
        assert result.passed is False
        assert result.metrics["null_count"] == 1

    def test_min_row_check_fails_on_tiny_batch(self, spark):
        from pyspark.sql.types import StringType, StructField, StructType

        from src.processing.bronze.quality_gates import BronzeQualityGates

        schema = StructType([StructField("order_id", StringType())])
        df = spark.createDataFrame([("ORD-0000000001",)], schema)  # 1 row < 10

        gates = BronzeQualityGates()
        result = gates._check_min_rows(df, "orders")
        assert result.passed is False

    def test_all_checks_pass_on_healthy_data(self, spark):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        from src.processing.bronze.quality_gates import BronzeQualityGates

        schema = StructType(
            [
                StructField("order_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("order_date", StringType()),
                StructField("total_amount", IntegerType()),
            ]
        )
        data = [(f"ORD-{i:010d}", f"CUST-{i:08d}", "2024-01-01", i * 10) for i in range(1, 20)]
        df = spark.createDataFrame(data, schema)

        gates = BronzeQualityGates()
        results = gates.run_all_checks(df, "orders")
        assert all(r.passed for r in results), [r.to_dict() for r in results if not r.passed]


# ─── SparkSession factory (unit) ──────────────────────────────────────────────


class TestCreateBronzeSparkSession:
    @pytest.fixture(autouse=True)
    def _require_pyspark(self):
        pyspark_sql = pytest.importorskip("pyspark.sql")
        # getOrCreate() would hand back whatever session an earlier test left behind
        active = pyspark_sql.SparkSession.getActiveSession()
        if active is not None:
            active.stop()
        yield
        active = pyspark_sql.SparkSession.getActiveSession()
        if active is not None:
            active.stop()

    def test_session_is_created(self):
        from src.processing.bronze.spark_session import create_bronze_spark_session

        spark = create_bronze_spark_session(
            app_name="test-session",
            extra_conf={"spark.master": "local[1]"},
        )
        assert spark is not None
        assert spark.sparkContext.appName == "test-session"
        spark.stop()

    def test_delta_extension_configured(self):
        from src.processing.bronze.spark_session import create_bronze_spark_session

        spark = create_bronze_spark_session(
            extra_conf={"spark.master": "local[1]"},
        )
        ext = spark.conf.get("spark.sql.extensions")
        assert "DeltaSparkSessionExtension" in ext
        spark.stop()


# ─── BronzeBatchIngestor (integration-lite, no S3) ────────────────────────────


class TestBronzeBatchIngestorUnit:
    """Unit tests that mock Spark and PostgreSQL — no real cluster required."""

    def test_init_generates_batch_id(self):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        mock_spark = MagicMock()
        ingestor = BronzeBatchIngestor(spark=mock_spark, raw_data_dir="/tmp")
        assert ingestor.batch_id is not None
        assert len(ingestor.batch_id) == 36  # UUID4 length

    def test_custom_batch_id_preserved(self):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        mock_spark = MagicMock()
        ingestor = BronzeBatchIngestor(
            spark=mock_spark, raw_data_dir="/tmp", batch_id="my-custom-id"
        )
        assert ingestor.batch_id == "my-custom-id"

    def test_source_path_json(self, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        mock_spark = MagicMock()
        ingestor = BronzeBatchIngestor(spark=mock_spark, raw_data_dir=str(tmp_path))
        path = ingestor._source_path("orders", "json")
        assert Path(path) == tmp_path / "json" / "orders"

    def test_source_path_csv(self, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        mock_spark = MagicMock()
        ingestor = BronzeBatchIngestor(spark=mock_spark, raw_data_dir=str(tmp_path))
        path = ingestor._source_path("suppliers", "csv")
        assert Path(path) == tmp_path / "csv" / "suppliers"

    def test_source_path_parquet(self, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        mock_spark = MagicMock()
        ingestor = BronzeBatchIngestor(spark=mock_spark, raw_data_dir=str(tmp_path))
        path = ingestor._source_path("finance", "parquet")
        assert Path(path) == tmp_path / "parquet" / "finance"

    def test_ingest_domain_skips_missing_source(self, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        mock_spark = MagicMock()
        # _read_raw returns None when path doesn't exist
        ingestor = BronzeBatchIngestor(spark=mock_spark, raw_data_dir=str(tmp_path))
        with patch.object(ingestor, "_read_raw", return_value=None):
            result = ingestor.ingest_domain("orders")
        assert result["status"] == "skipped"
        assert result["rows_written"] == 0
