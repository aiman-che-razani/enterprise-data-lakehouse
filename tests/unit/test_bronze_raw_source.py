"""Where Bronze reads raw files from, and that an ingest which reads nothing is not reported as success."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.common.config.settings import Settings, get_settings
from src.processing.bronze.batch_ingestor import BronzeBatchIngestor


@pytest.fixture(autouse=True)
def _clean_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _settings(monkeypatch, *, endpoint: str | None, environment: str = "dev") -> Settings:
    monkeypatch.setenv("ENVIRONMENT", environment)
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lake")
    if endpoint is None:
        monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
        return Settings(s3_endpoint_url=None)
    monkeypatch.setenv("S3_ENDPOINT_URL", endpoint)
    return Settings()


class TestRawPath:
    def test_object_storage_when_an_endpoint_is_configured(self, monkeypatch):
        assert _settings(monkeypatch, endpoint="http://minio:9000").raw_path == "s3a://lake/raw"

    def test_local_folder_without_object_storage(self, monkeypatch):
        assert _settings(monkeypatch, endpoint=None).raw_path == "datasets/raw"


class TestIngestorSource:
    def test_defaults_to_the_settings_raw_path(self, monkeypatch):
        s = _settings(monkeypatch, endpoint="http://minio:9000")
        ing = BronzeBatchIngestor(spark=MagicMock(), settings=s)
        assert ing._source_path("orders", "json") == "s3a://lake/raw/json/orders"

    def test_an_explicit_uri_keeps_its_double_slash(self, monkeypatch):
        s = _settings(monkeypatch, endpoint="http://minio:9000")
        ing = BronzeBatchIngestor(spark=MagicMock(), settings=s, raw_data_dir="s3a://other/raw/")
        assert ing._source_path("stores", "csv") == "s3a://other/raw/csv/stores"

    def test_a_missing_local_folder_is_skipped(self, monkeypatch, tmp_path):
        s = _settings(monkeypatch, endpoint=None)
        ing = BronzeBatchIngestor(spark=MagicMock(), settings=s, raw_data_dir=str(tmp_path))
        assert (
            ing._read_raw(ing._source_path("orders", "json"), "json", MagicMock(), "orders") is None
        )


class TestJobExitCode:
    @staticmethod
    def _run(results: dict, argv: list[str]) -> int:
        from spark.jobs.bronze import ingest_bronze

        ingestor = MagicMock()
        ingestor.ingest_domain.side_effect = lambda d: results[d]
        with (
            patch.object(ingest_bronze, "create_bronze_spark_session", return_value=MagicMock()),
            patch.object(ingest_bronze, "BronzeBatchIngestor", return_value=ingestor),
            patch.object(ingest_bronze, "setup_logging"),
        ):
            return ingest_bronze.main(argv)

    def test_fails_when_nothing_was_ingested(self):
        assert (
            self._run({"orders": {"status": "skipped", "rows_written": 0}}, ["--domain", "orders"])
            == 1
        )

    def test_allow_empty_opts_out(self):
        argv = ["--domain", "orders", "--allow-empty"]
        assert self._run({"orders": {"status": "skipped", "rows_written": 0}}, argv) == 0

    def test_succeeds_when_rows_were_written(self):
        assert (
            self._run({"orders": {"status": "ok", "rows_written": 5}}, ["--domain", "orders"]) == 0
        )

    def test_a_failed_domain_still_fails(self):
        assert (
            self._run({"orders": {"status": "failed"}}, ["--domain", "orders", "--allow-empty"])
            == 1
        )


class TestSharedSchemaIsNotMutated:
    def test_reading_twice_leaves_the_registered_schema_untouched(self, monkeypatch):
        from spark.schemas.bronze_schemas import get_bronze_schema

        s = _settings(monkeypatch, endpoint="http://minio:9000")
        ing = BronzeBatchIngestor(spark=MagicMock(), settings=s)
        schema = get_bronze_schema("orders")
        before = [f.name for f in schema.fields]

        for _ in range(2):
            ing._read_raw("s3a://lake/raw/json/orders", "json", schema, "orders")

        assert [f.name for f in schema.fields] == before
        assert "_corrupt_record" not in schema.fieldNames()
