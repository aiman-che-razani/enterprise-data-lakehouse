"""
Unit tests for src.common.config.settings.

Tests cover:
- Default values and type coercion
- Computed properties (S3 paths, DB URLs)
- Environment override via env vars
- Production validation guards
- Singleton behaviour of get_settings()
"""

from __future__ import annotations

import pytest

from src.common.config.settings import Settings, get_settings

pytestmark = pytest.mark.unit


class TestSettingsDefaults:
    def test_default_environment_is_dev(self, dev_settings: Settings) -> None:
        assert dev_settings.environment == "dev"

    def test_default_lakehouse_bucket(self, dev_settings: Settings) -> None:
        assert dev_settings.lakehouse_bucket == "retailnexus-lakehouse"

    def test_default_postgres_port(self, dev_settings: Settings) -> None:
        assert dev_settings.postgres_port == 5432

    def test_default_api_port(self, dev_settings: Settings) -> None:
        assert dev_settings.api_port == 8000

    def test_default_kafka_topic_orders(self, dev_settings: Settings) -> None:
        assert dev_settings.kafka_topic_orders == "retailnexus.orders"

    def test_dq_halt_on_critical_is_true_by_default(self, dev_settings: Settings) -> None:
        assert dev_settings.dq_halt_on_critical is True


class TestComputedProperties:
    def test_bronze_s3_path(self, dev_settings: Settings) -> None:
        assert dev_settings.bronze_s3_path == "s3a://retailnexus-lakehouse/bronze"

    def test_silver_s3_path(self, dev_settings: Settings) -> None:
        assert dev_settings.silver_s3_path == "s3a://retailnexus-lakehouse/silver"

    def test_gold_s3_path(self, dev_settings: Settings) -> None:
        assert dev_settings.gold_s3_path == "s3a://retailnexus-lakehouse/gold"

    def test_is_production_false_in_dev(self, dev_settings: Settings) -> None:
        assert dev_settings.is_production is False

    def test_log_level_is_info_in_dev(self, dev_settings: Settings) -> None:
        assert dev_settings.log_level == "INFO"

    def test_postgres_sync_url_format(self, dev_settings: Settings) -> None:
        url = dev_settings.postgres_sync_url
        assert url.startswith("postgresql+psycopg2://")
        assert "lakehouse" in url
        assert "5432" in url

    def test_postgres_async_url_format(self, dev_settings: Settings) -> None:
        url = dev_settings.postgres_async_url
        assert url.startswith("postgresql+asyncpg://")


class TestS3PathHelper:
    def test_s3_path_bronze_domain(self, dev_settings: Settings) -> None:
        path = dev_settings.s3_path("bronze", "orders")
        assert path == "s3a://retailnexus-lakehouse/bronze/orders"

    def test_s3_path_silver_domain(self, dev_settings: Settings) -> None:
        path = dev_settings.s3_path("silver", "customers")
        assert path == "s3a://retailnexus-lakehouse/silver/customers"

    def test_s3_path_gold_domain(self, dev_settings: Settings) -> None:
        path = dev_settings.s3_path("gold", "fact_sales")
        assert path == "s3a://retailnexus-lakehouse/gold/fact_sales"


class TestKafkaHelpers:
    def test_kafka_consumer_group(self, dev_settings: Settings) -> None:
        group = dev_settings.kafka_consumer_group("orders")
        assert group == "lakehouse-orders"

    def test_checkpoint_path(self, dev_settings: Settings) -> None:
        path = dev_settings.checkpoint_path("orders_stream")
        assert "orders_stream" in path
        assert path.startswith("s3a://")


class TestEnvironmentOverride:
    def test_override_lakehouse_bucket(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("LAKEHOUSE_BUCKET", "my-custom-bucket")
        get_settings.cache_clear()
        s = Settings()
        assert s.lakehouse_bucket == "my-custom-bucket"
        get_settings.cache_clear()

    def test_override_kafka_bootstrap_servers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
        get_settings.cache_clear()
        s = Settings()
        assert s.kafka_bootstrap_servers == "broker1:9092,broker2:9092"
        get_settings.cache_clear()


class TestProductionValidation:
    def test_prod_requires_non_default_api_secret(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENVIRONMENT", "prod")
        monkeypatch.setenv("SECRETS_BACKEND", "aws_secrets_manager")
        monkeypatch.setenv("API_SECRET_KEY", "dev-secret-change-in-production")
        get_settings.cache_clear()
        with pytest.raises(ValueError, match="API_SECRET_KEY"):
            Settings()
        get_settings.cache_clear()

    def test_prod_requires_aws_secrets_backend(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENVIRONMENT", "prod")
        monkeypatch.setenv("SECRETS_BACKEND", "env")
        monkeypatch.setenv("API_SECRET_KEY", "super-secret-production-key-xyz")
        get_settings.cache_clear()
        with pytest.raises(ValueError, match="SECRETS_BACKEND"):
            Settings()
        get_settings.cache_clear()


class TestSingleton:
    def test_get_settings_returns_same_instance(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENVIRONMENT", "dev")
        get_settings.cache_clear()
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2
        get_settings.cache_clear()

    def test_secret_not_in_repr(
        self, monkeypatch: pytest.MonkeyPatch, dev_settings: Settings
    ) -> None:
        # Use a value that cannot collide with non-secret fields (postgres_db="lakehouse").
        monkeypatch.setenv("POSTGRES_PASSWORD", "pw-Zx9-not-in-repr")
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minio-Qw7-not-in-repr")
        get_settings.cache_clear()
        repr_str = repr(Settings())
        get_settings.cache_clear()
        assert "pw-Zx9-not-in-repr" not in repr_str
        assert "minio-Qw7-not-in-repr" not in repr_str
