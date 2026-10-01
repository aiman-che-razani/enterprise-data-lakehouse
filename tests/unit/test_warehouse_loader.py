"""
Unit tests for warehouse loader base class and config.
No warehouse connections are needed — all I/O is mocked.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.loading.warehouse_config import WarehouseSettings


class TestWarehouseConfig:
    def test_redshift_jdbc_url_constructed(self, monkeypatch):
        monkeypatch.setenv("REDSHIFT_HOST", "cluster.abcdef.us-east-1.redshift.amazonaws.com")
        monkeypatch.setenv("REDSHIFT_PORT", "5439")
        monkeypatch.setenv("REDSHIFT_DB", "retailnexus")
        cfg = WarehouseSettings()
        assert "redshift.amazonaws.com" in cfg.redshift_jdbc_url
        assert "5439" in cfg.redshift_jdbc_url
        assert "retailnexus" in cfg.redshift_jdbc_url

    def test_redshift_jdbc_url_none_when_no_host(self, monkeypatch):
        monkeypatch.delenv("REDSHIFT_HOST", raising=False)
        cfg = WarehouseSettings()
        assert cfg.redshift_jdbc_url is None

    def test_snowflake_url_constructed(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "xy12345.us-east-1")
        cfg = WarehouseSettings()
        assert "snowflakecomputing.com" in cfg.snowflake_url

    def test_snowflake_url_none_when_no_account(self, monkeypatch):
        monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
        cfg = WarehouseSettings()
        assert cfg.snowflake_url is None

    def test_bq_defaults(self):
        cfg = WarehouseSettings()
        assert cfg.bq_project == "retailnexus-prod"
        assert cfg.bq_dataset == "gold"
        assert "gs://" in cfg.bq_temp_gcs_uri

    def test_gcs_temp_uri_format(self, monkeypatch):
        monkeypatch.setenv("GCS_TEMP_BUCKET", "my-temp-bucket")
        cfg = WarehouseSettings()
        assert cfg.bq_temp_gcs_uri == "gs://my-temp-bucket/bq-temp/"


class TestBaseWarehouseLoaderDropCols:
    """Test internal column dropping without needing Spark."""

    def test_drop_cols_constant_contains_delta_internals(self):
        from src.loading.base_loader import _DROP_COLS

        assert "_change_type" in _DROP_COLS
        assert "_scd_hash" in _DROP_COLS
        assert "_bronze_version" in _DROP_COLS
        assert "_silver_processed_at" in _DROP_COLS

    def test_drop_cols_does_not_include_gold_metadata(self):
        from src.loading.base_loader import _DROP_COLS

        assert "_gold_processed_at" not in _DROP_COLS
        assert "_gold_date" not in _DROP_COLS
        assert "_batch_id" not in _DROP_COLS
        assert "_pipeline_version" not in _DROP_COLS


class TestRedshiftLoaderTarget:
    def test_target_is_redshift(self):
        from src.loading.redshift.redshift_loader import RedshiftLoader

        assert RedshiftLoader.target == "redshift"


class TestSnowflakeLoaderTarget:
    def test_target_is_snowflake(self):
        from src.loading.snowflake.snowflake_loader import SnowflakeLoader

        assert SnowflakeLoader.target == "snowflake"

    def test_sf_options_keys(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "xy12345")
        monkeypatch.setenv("SNOWFLAKE_USER", "svc_etl")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "s3cr3t")
        from src.loading.snowflake.snowflake_loader import SnowflakeLoader

        spark = MagicMock()
        settings = MagicMock()
        loader = SnowflakeLoader(spark=spark, settings=settings)

        opts = loader._sf_options
        assert "sfURL" in opts
        assert "sfDatabase" in opts
        assert "sfSchema" in opts
        assert "sfWarehouse" in opts
        assert opts["sfColumnNameCaseSensitive"] == "false"


class TestBigQueryLoaderTarget:
    def test_target_is_bigquery(self):
        from src.loading.bigquery.bigquery_loader import BigQueryLoader

        assert BigQueryLoader.target == "bigquery"


class TestWarehouseLoadModeMapping:
    """Verify that dim tables use overwrite and fact tables use append."""

    def test_dim_tables_use_overwrite_mode(self):
        from src.processing.gold.domain_meta import FULL_REFRESH_DIMS, GOLD_DIM_ORDER

        for dim in GOLD_DIM_ORDER:
            assert (
                dim in FULL_REFRESH_DIMS
            ), f"{dim} should be in FULL_REFRESH_DIMS (overwrite mode)"

    def test_fact_tables_not_in_full_refresh(self):
        from src.processing.gold.domain_meta import FULL_REFRESH_DIMS, GOLD_FACT_ORDER

        for fact in GOLD_FACT_ORDER:
            assert (
                fact not in FULL_REFRESH_DIMS
            ), f"{fact} should NOT be in FULL_REFRESH_DIMS (facts use append)"
