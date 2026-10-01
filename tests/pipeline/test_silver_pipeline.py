"""
Pipeline integration tests for the Silver layer.

Uses PySpark local mode + Delta Lake (in-process).
Writes real Bronze Delta data to tmp_path and exercises
the full Silver processor pipeline end-to-end.

Requires:
    pyspark==3.5.3
    delta-spark==3.2.0
    pytest-tmp-path (built into pytest)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

# ─── Settings mock ────────────────────────────────────────────────────────────


def _mock_settings(tmp_path: Path) -> MagicMock:
    settings = MagicMock()
    settings.pipeline_version = "test-1.0"
    settings.postgres_dsn = "postgresql://localhost/test"

    def s3_path(layer: str, domain: str) -> str:
        return str(tmp_path / layer / domain)

    settings.s3_path = s3_path
    return settings


# ─── Bronze fixture writer ────────────────────────────────────────────────────


def _write_bronze_customers(spark: SparkSession, bronze_path: str) -> None:
    schema = StructType(
        [
            StructField("customer_id", StringType()),
            StructField("segment", StringType()),
            StructField("loyalty_tier", StringType()),
            StructField("loyalty_points", IntegerType()),
            StructField("email", StringType()),
            StructField("phone", StringType()),
            StructField("address_line1", StringType()),
            StructField("date_of_birth", StringType()),
            StructField("country", StringType()),
            StructField("state", StringType()),
            StructField("channel_preference", StringType()),
            StructField("is_email_verified", StringType()),
            StructField("is_sms_opted_in", StringType()),
            StructField("is_active", StringType()),
            StructField("created_at", StringType()),
            StructField("updated_at", StringType()),
            StructField("last_order_at", StringType()),
            StructField("_ingested_at", StringType()),
            StructField("_ingestion_date", StringType()),
            StructField("_batch_id", StringType()),
            StructField("_source_file", StringType()),
            StructField("_pipeline_version", StringType()),
        ]
    )
    rows = [
        (
            "c1",
            "gold",
            "gold",
            500,
            "alice@test.com",
            "555-1234",
            "123 Main St",
            "1990-05-15",
            "US",
            "CA",
            "email",
            "true",
            "false",
            "true",
            "2025-01-01T00:00:00Z",
            "2025-06-01T00:00:00Z",
            "2025-05-01T00:00:00Z",
            "2025-06-01T01:00:00Z",
            "2025-06-01",
            "batch-1",
            "customers.csv",
            "1.0",
        ),
        (
            "c2",
            "silver",
            "silver",
            200,
            "bob@test.com",
            "555-5678",
            "456 Oak Ave",
            "1985-03-20",
            "US",
            "NY",
            "sms",
            "true",
            "true",
            "true",
            "2025-01-02T00:00:00Z",
            "2025-06-01T00:00:00Z",
            None,
            "2025-06-01T01:00:00Z",
            "2025-06-01",
            "batch-1",
            "customers.csv",
            "1.0",
        ),
    ]
    df = spark.createDataFrame(rows, schema)
    df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").save(
        bronze_path
    )


# ─── Tests ────────────────────────────────────────────────────────────────────


class TestCustomersSilverProcessor:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path, spark):
        self.spark = spark
        self.tmp_path = tmp_path
        self.settings = _mock_settings(tmp_path)
        self.bronze_path = str(tmp_path / "bronze" / "customers")
        self.silver_path = str(tmp_path / "silver" / "customers")
        _write_bronze_customers(spark, self.bronze_path)

    def _run(self):
        from src.processing.silver.domains.customers import CustomersSilverProcessor

        processor = CustomersSilverProcessor(spark=self.spark, settings=self.settings)

        with (
            patch.object(processor, "_get_last_bronze_version", return_value=None),
            patch.object(processor, "_update_watermark", return_value=None),
            patch.object(processor, "_get_bronze_latest_version", return_value=0),
        ):
            # Read Bronze directly (no CDF needed for version 0 from scratch)
            bronze_df = self.spark.read.format("delta").load(self.bronze_path)
            # Inject directly into the pipeline stages
            pk_col = "customer_id"
            deduped = processor._deduplicator.deduplicate(bronze_df, pk_col)
            cleansed = processor._cleanser.cleanse(deduped, "customers")
            valid_df, invalid_df = processor._validator.validate(cleansed, "customers")
            tokenised = processor._tokenizer.tokenize(valid_df, "customers")
            transformed = processor.domain_transform(tokenised)
            final_df = processor._add_silver_metadata(transformed, 0, "test-batch")
            final_df.write.format("delta").mode("overwrite").save(self.silver_path)
            return final_df

    def test_pii_email_hashed(self):
        final_df = self._run()
        cols = final_df.columns
        assert "email_hash" in cols, "email_hash must exist after tokenisation"
        assert "email" not in cols, "raw email must be dropped"

    def test_pii_phone_dropped(self):
        final_df = self._run()
        assert "phone" not in final_df.columns

    def test_pii_address_dropped(self):
        final_df = self._run()
        assert "address_line1" not in final_df.columns

    def test_pii_dob_bucketed(self):
        final_df = self._run()
        assert "date_of_birth" not in final_df.columns
        assert "age_bucket" in final_df.columns
        buckets = {r["age_bucket"] for r in final_df.select("age_bucket").collect()}
        valid_buckets = {
            "0-17",
            "18-24",
            "25-34",
            "35-44",
            "45-54",
            "55-64",
            "65-74",
            "75+",
            "Unknown",
        }
        assert buckets.issubset(valid_buckets)

    def test_silver_metadata_columns(self):
        final_df = self._run()
        for col in ("_bronze_version", "_silver_processed_at", "_silver_date", "_batch_id"):
            assert col in final_df.columns, f"{col} missing from Silver output"

    def test_loyalty_tier_rank_derived(self):
        final_df = self._run()
        assert "loyalty_tier_rank" in final_df.columns
        gold_rank = (
            final_df.filter(F.col("loyalty_tier") == "gold")
            .select("loyalty_tier_rank")
            .first()["loyalty_tier_rank"]
        )
        assert gold_rank == 3

    def test_two_customers_written(self):
        final_df = self._run()
        assert final_df.count() == 2

    def test_delta_written(self):
        self._run()
        from delta import DeltaTable

        assert DeltaTable.isDeltaTable(self.spark, self.silver_path)


class TestSilverCleanser:
    def test_timestamp_cast(self, spark, tmp_path):
        from src.processing.silver.cleanser import SilverCleanser

        schema = StructType(
            [
                StructField("order_id", StringType()),
                StructField("created_at", StringType()),
            ]
        )
        df = spark.createDataFrame([("o1", "2025-06-01T12:00:00Z")], schema)
        cleansed = SilverCleanser().cleanse(df, "orders")
        ts_type = cleansed.schema["created_at"].dataType
        assert ts_type == TimestampType()

    def test_empty_string_nullified(self, spark, tmp_path):
        from src.processing.silver.cleanser import SilverCleanser

        schema = StructType([StructField("status", StringType())])
        df = spark.createDataFrame([("",)], schema)
        cleansed = SilverCleanser().cleanse(df, "orders")
        row = cleansed.first()
        assert row["status"] is None

    def test_enum_lowercased(self, spark, tmp_path):
        from src.processing.silver.cleanser import SilverCleanser

        schema = StructType([StructField("status", StringType())])
        df = spark.createDataFrame([("CONFIRMED",)], schema)
        cleansed = SilverCleanser().cleanse(df, "orders")
        assert cleansed.first()["status"] == "confirmed"


class TestCampaignsDomainTransform:
    def test_is_active_combines_start_and_end_date_without_a_type_error(self, spark):
        """`start <= today & (end is null | end >= today)` parsed as `start <= (today & ...)` once."""
        from src.processing.silver.domains.campaigns import CampaignsSilverProcessor

        df = spark.sql(
            """
            SELECT * FROM VALUES
              ('open-ended', '2000-01-01', CAST(NULL AS STRING)),
              ('finished',   '2000-01-01', '2000-02-01'),
              ('future',     '2999-01-01', '2999-02-01'),
              ('running',    '2000-01-01', '2999-01-01')
            AS t(campaign_id, start_date, end_date)
            """
        )
        out = CampaignsSilverProcessor.domain_transform(None, df)
        active = {r["campaign_id"]: r["is_active"] for r in out.collect()}
        assert active == {"open-ended": True, "finished": False, "future": False, "running": True}


class TestCleanserAgreesWithValidator:
    """The generator's enum spellings must survive cleansing and pass validation (an e2e run once sent
    every shipment and inventory row to the dead-letter table: 'UPS' was never lowercased, 'B' was).
    """

    @staticmethod
    def _run(spark, domain, rows_sql):
        from src.processing.silver.cleanser import SilverCleanser
        from src.processing.silver.validator import SilverValidator

        df = spark.sql(rows_sql)
        valid, invalid = SilverValidator().validate(SilverCleanser().cleanse(df, domain), domain)
        return valid.count(), 0 if invalid is None else invalid.count()

    def test_shipment_carriers_as_generated_are_valid(self, spark):
        valid, invalid = self._run(
            spark,
            "shipments",
            "SELECT * FROM VALUES ('FedEx'), ('UPS'), ('USPS'), ('DHL'), ('OnTrac') AS t(carrier)",
        )
        assert (valid, invalid) == (5, 0)

    def test_inventory_abc_classes_as_generated_are_valid(self, spark):
        valid, invalid = self._run(
            spark, "inventory", "SELECT * FROM VALUES ('A'), ('B'), ('C') AS t(abc_class)"
        )
        assert (valid, invalid) == (3, 0)
