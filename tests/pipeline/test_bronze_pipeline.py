"""
Pipeline integration tests for the Bronze ingestion layer.

These tests use PySpark in local mode with Delta Lake and write to a
temporary directory (no MinIO or PostgreSQL required).

Marked with @pytest.mark.pipeline — excluded from the fast unit test suite.
Run with: pytest tests/pipeline/ -m pipeline
"""

from __future__ import annotations

import csv
import json
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.pipeline


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module", autouse=True)
def _require_pyspark():
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")


@pytest.fixture()
def raw_data_dir(tmp_path: Path) -> Path:
    """Create a minimal set of raw source files for two domains."""
    _write_orders_json(tmp_path)
    _write_suppliers_csv(tmp_path)
    return tmp_path


def _write_orders_json(base: Path) -> None:
    orders_dir = base / "json" / "orders"
    orders_dir.mkdir(parents=True)
    records = [
        {
            "order_id": f"ORD-{i:010d}",
            "customer_id": f"CUST-{i:08d}",
            "order_date": "2024-06-01",
            "channel": "web",
            "store_id": None,
            "status": "delivered",
            "payment_status": "paid",
            "subtotal": 99.99,
            "discount_amount": 0.0,
            "tax_amount": 8.50,
            "shipping_amount": 5.99,
            "total_amount": 114.48,
            "currency": "USD",
            "promo_code": None,
            "is_gift": False,
            "created_at": "2024-06-01T10:00:00Z",
            "updated_at": "2024-06-01T12:00:00Z",
            "source_system": "ecommerce",
        }
        for i in range(1, 26)  # 25 records
    ]
    path = orders_dir / "orders_part_0000.json"
    path.write_text("\n".join(json.dumps(r) for r in records))


def _write_suppliers_csv(base: Path) -> None:
    suppliers_dir = base / "csv" / "suppliers"
    suppliers_dir.mkdir(parents=True)
    fieldnames = [
        "supplier_id",
        "name",
        "tier",
        "country",
        "city",
        "contact_email",
        "contact_phone",
        "lead_time_days",
        "on_time_rate",
        "quality_score",
        "payment_terms",
        "certifications",
        "is_active",
        "onboarded_date",
        "created_at",
        "updated_at",
    ]
    rows = [
        {
            "supplier_id": f"SUP-{i:06d}",
            "name": f"Supplier {i}",
            "tier": ["A", "B", "C"][i % 3],
            "country": "United States",
            "city": "Chicago",
            "contact_email": f"contact{i}@supplier.com",
            "contact_phone": "555-0100",
            "lead_time_days": 14,
            "on_time_rate": 0.95,
            "quality_score": 8.5,
            "payment_terms": "NET30",
            "certifications": "[]",
            "is_active": True,
            "onboarded_date": "2022-01-01",
            "created_at": "2022-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }
        for i in range(1, 26)
    ]
    path = suppliers_dir / "suppliers.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ─── Tests ────────────────────────────────────────────────────────────────────


class TestBronzeOrdersIngestion:
    def test_ingest_orders_writes_delta(self, spark, raw_data_dir, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze"
        settings = _mock_settings(bronze_dir)

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect") as mock_pg:
            mock_pg.return_value.__enter__ = lambda s: s
            mock_pg.return_value.__exit__ = lambda *a: None
            mock_pg.return_value.cursor.return_value.__enter__ = lambda s: s
            mock_pg.return_value.cursor.return_value.__exit__ = lambda *a: None

            ingestor = BronzeBatchIngestor(
                spark=spark,
                settings=settings,
                raw_data_dir=str(raw_data_dir),
            )
            with patch.object(ingestor, "_optimize"):  # skip OPTIMIZE in tests
                result = ingestor.ingest_domain("orders")

        assert result["status"] == "success"
        assert result["rows_written"] == 25

    def test_bronze_delta_has_metadata_columns(self, spark, raw_data_dir, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze2"
        settings = _mock_settings(bronze_dir)

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect"):
            ingestor = BronzeBatchIngestor(
                spark=spark, settings=settings, raw_data_dir=str(raw_data_dir)
            )
            with patch.object(ingestor, "_optimize"):
                ingestor.ingest_domain("orders")

        bronze_path = str(bronze_dir / "orders")
        df = spark.read.format("delta").load(bronze_path)
        cols = df.columns
        assert "_ingested_at" in cols
        assert "_ingestion_date" in cols
        assert "_batch_id" in cols
        assert "_pipeline_version" in cols

    def test_bronze_delta_has_correct_row_count(self, spark, raw_data_dir, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze3"
        settings = _mock_settings(bronze_dir)

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect"):
            ingestor = BronzeBatchIngestor(
                spark=spark, settings=settings, raw_data_dir=str(raw_data_dir)
            )
            with patch.object(ingestor, "_optimize"):
                ingestor.ingest_domain("orders")

        bronze_path = str(bronze_dir / "orders")
        count = spark.read.format("delta").load(bronze_path).count()
        assert count == 25

    def test_a_second_run_does_not_re_ingest_files_already_in_bronze(
        self, spark, raw_data_dir, tmp_path
    ):
        """Every run reads the whole raw folder; without file-level idempotency each daily run would
        append all of history again."""
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze_idempotent"
        settings = _mock_settings(bronze_dir)

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect"):
            for run in range(2):
                ingestor = BronzeBatchIngestor(
                    spark=spark,
                    settings=settings,
                    raw_data_dir=str(raw_data_dir),
                    batch_id=f"run-{run}",
                )
                with patch.object(ingestor, "_optimize"):
                    result = ingestor.ingest_domain("orders")
                assert result["rows_written"] == (25 if run == 0 else 0)

        bronze_path = str(bronze_dir / "orders")
        assert spark.read.format("delta").load(bronze_path).count() == 25

    def test_a_new_file_is_ingested_alongside_already_ingested_ones(
        self, spark, raw_data_dir, tmp_path
    ):
        import shutil

        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze_incremental"
        settings = _mock_settings(bronze_dir)
        orders_dir = raw_data_dir / "json" / "orders"

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect"):
            first = BronzeBatchIngestor(
                spark=spark, settings=settings, raw_data_dir=str(raw_data_dir), batch_id="b1"
            )
            with patch.object(first, "_optimize"):
                first.ingest_domain("orders")

            shutil.copy(orders_dir / "orders_part_0000.json", orders_dir / "orders_part_0001.json")
            second = BronzeBatchIngestor(
                spark=spark, settings=settings, raw_data_dir=str(raw_data_dir), batch_id="b2"
            )
            with patch.object(second, "_optimize"):
                result = second.ingest_domain("orders")

        assert result["rows_written"] == 25  # only the new file
        df = spark.read.format("delta").load(str(bronze_dir / "orders"))
        assert df.count() == 50
        assert df.select("_source_file").distinct().count() == 2
        assert df.filter("_source_file = '' OR _source_file IS NULL").count() == 0

    def test_bronze_partitioned_by_ingestion_date(self, spark, raw_data_dir, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze_partitioned"
        settings = _mock_settings(bronze_dir)

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect"):
            ingestor = BronzeBatchIngestor(
                spark=spark, settings=settings, raw_data_dir=str(raw_data_dir)
            )
            with patch.object(ingestor, "_optimize"):
                ingestor.ingest_domain("orders")

        bronze_path = str(bronze_dir / "orders")
        df = spark.read.format("delta").load(bronze_path)
        assert "_ingestion_date" in df.columns


class TestBronzeSuppliersIngestion:
    def test_ingest_csv_domain(self, spark, raw_data_dir, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze_suppliers"
        settings = _mock_settings(bronze_dir)

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect"):
            ingestor = BronzeBatchIngestor(
                spark=spark, settings=settings, raw_data_dir=str(raw_data_dir)
            )
            with patch.object(ingestor, "_optimize"):
                result = ingestor.ingest_domain("suppliers")

        assert result["status"] == "success"
        assert result["rows_written"] == 25

    def test_supplier_tier_values_preserved(self, spark, raw_data_dir, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze_suppliers2"
        settings = _mock_settings(bronze_dir)

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect"):
            ingestor = BronzeBatchIngestor(
                spark=spark, settings=settings, raw_data_dir=str(raw_data_dir)
            )
            with patch.object(ingestor, "_optimize"):
                ingestor.ingest_domain("suppliers")

        bronze_path = str(bronze_dir / "suppliers")
        tiers = {
            row["tier"]
            for row in spark.read.format("delta").load(bronze_path).select("tier").collect()
        }
        assert tiers.issubset({"A", "B", "C"})


class TestBronzeIngestAll:
    def test_ingest_all_returns_results_per_domain(self, spark, raw_data_dir, tmp_path):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze_all"
        settings = _mock_settings(bronze_dir)

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect"):
            ingestor = BronzeBatchIngestor(
                spark=spark, settings=settings, raw_data_dir=str(raw_data_dir)
            )
            with patch.object(ingestor, "_optimize"):
                results = ingestor.ingest_all(domains=["orders", "suppliers"])

        assert "orders" in results
        assert "suppliers" in results
        assert results["orders"]["status"] == "success"
        assert results["suppliers"]["status"] == "success"

    def test_ingest_all_skips_missing_domains(self, spark, raw_data_dir, tmp_path):
        """Domains with no source files are skipped, not failed."""
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        bronze_dir = tmp_path / "bronze_skip"
        settings = _mock_settings(bronze_dir)

        with patch("src.processing.bronze.batch_ingestor.psycopg2.connect"):
            ingestor = BronzeBatchIngestor(
                spark=spark, settings=settings, raw_data_dir=str(raw_data_dir)
            )
            with patch.object(ingestor, "_optimize"):
                results = ingestor.ingest_all(domains=["orders", "customers"])  # no customers data

        assert results["orders"]["status"] == "success"
        assert results["customers"]["status"] == "skipped"


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _mock_settings(bronze_base: Path):
    """Return a Settings-like mock that writes Delta tables to a local temp dir."""
    from unittest.mock import MagicMock

    from pydantic import SecretStr

    settings = MagicMock()
    settings.environment = "dev"
    settings.aws_access_key_id = "test"
    settings.aws_secret_access_key = SecretStr("test")
    settings.s3_endpoint_url = None
    settings.postgres_host = "localhost"
    settings.postgres_port = 5432
    settings.postgres_db = "lakehouse"
    settings.postgres_user = "lakehouse"
    settings.postgres_password = SecretStr("lakehouse")

    # Redirect all Delta writes to a local temp dir instead of S3
    def _s3_path(layer, domain):
        return str(bronze_base / domain)

    settings.s3_path = _s3_path
    settings.checkpoint_path = lambda name: str(bronze_base / ".checkpoints" / name)
    return settings
