"""
Environment-aware configuration management.

Uses pydantic-settings to load config from environment variables (or .env file).
Provides a singleton `get_settings()` function that's safe to call from
anywhere in the codebase — Spark jobs, Airflow operators, FastAPI endpoints.
"""

from __future__ import annotations

import functools
from typing import Literal

from pydantic import Field, SecretStr, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Central configuration for the RetailNexus Data Lakehouse.

    Priority order (highest → lowest):
        1. Environment variables
        2. .env file in project root
        3. Default values defined here

    Secrets (SecretStr) are never logged or included in str() output.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ─── Environment ───────────────────────────────────────────────────────────
    environment: Literal["dev", "staging", "prod"] = Field(
        default="dev",
        description="Deployment environment. Controls logging verbosity, S3 paths, and secret backend.",
    )

    # ─── AWS / MinIO ───────────────────────────────────────────────────────────
    aws_access_key_id: str = Field(default="minioadmin", description="AWS or MinIO access key")
    aws_secret_access_key: SecretStr = Field(
        default=SecretStr("minioadmin"),
        description="AWS or MinIO secret key",
    )
    aws_default_region: str = Field(default="us-east-1")
    s3_endpoint_url: str | None = Field(
        default="http://minio:9000",
        description="Override S3 endpoint. Set to None in prod to use real AWS.",
    )
    lakehouse_bucket: str = Field(
        default="retailnexus-lakehouse",
        description="Primary S3 bucket for all lakehouse layers.",
    )

    # ─── S3 path prefixes ──────────────────────────────────────────────────────
    bronze_prefix: str = Field(default="bronze")
    silver_prefix: str = Field(default="silver")
    gold_prefix: str = Field(default="gold")
    quality_reports_prefix: str = Field(default="quality-reports")
    checkpoints_prefix: str = Field(default="checkpoints")

    # ─── PostgreSQL ────────────────────────────────────────────────────────────
    postgres_host: str = Field(default="postgres")
    postgres_port: int = Field(default=5432)
    postgres_user: str = Field(default="lakehouse")
    postgres_password: SecretStr = Field(default=SecretStr("lakehouse"))
    postgres_db: str = Field(default="lakehouse")

    # ─── Apache Kafka ──────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Comma-separated list of Kafka broker host:port pairs.",
    )
    kafka_security_protocol: str | None = Field(
        default=None, description="e.g. SASL_SSL; unset = PLAINTEXT"
    )
    kafka_sasl_mechanism: str | None = Field(default=None)
    kafka_sasl_username: str | None = Field(default=None)
    kafka_sasl_password: SecretStr | None = Field(default=None)
    kafka_consumer_group_prefix: str = Field(default="lakehouse")
    kafka_auto_offset_reset: Literal["earliest", "latest"] = Field(default="latest")
    kafka_replication_factor: int = Field(default=1)

    # Kafka topic names
    kafka_topic_orders: str = Field(default="retailnexus.orders")
    kafka_topic_payments: str = Field(default="retailnexus.payments")
    kafka_topic_clickstream: str = Field(default="retailnexus.clickstream")
    kafka_topic_inventory: str = Field(default="retailnexus.inventory")
    kafka_topic_iot_sensors: str = Field(default="retailnexus.iot-sensors")

    # ─── Apache Spark ──────────────────────────────────────────────────────────
    spark_master_url: str = Field(default="spark://spark-master:7077")
    spark_driver_memory: str = Field(default="2g")
    spark_executor_memory: str = Field(default="2g")
    spark_executor_cores: int = Field(default=2)
    spark_max_executors: int = Field(default=4)

    # Delta Lake
    delta_log_retention_days: int = Field(default=30)
    delta_vacuum_retention_hours: int = Field(
        default=72
    )  # Silver/Gold; Bronze keeps 7 years (see delta_maintenance.py)

    # ─── FastAPI ───────────────────────────────────────────────────────────────
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_secret_key: SecretStr = Field(default=SecretStr("dev-secret-change-in-production"))
    api_debug: bool = Field(default=False)
    api_workers: int = Field(default=1)
    api_rate_limit_per_minute: int = Field(
        default=300,
        ge=0,
        description="Sustained requests/minute per API key (or client IP); 0 disables limiting.",
    )
    api_access_token_expire_minutes: int = Field(default=30)

    # ─── Airflow ───────────────────────────────────────────────────────────────
    airflow_postgres_user: str = Field(default="airflow")
    airflow_postgres_password: SecretStr = Field(default=SecretStr("airflow"))
    airflow_postgres_db: str = Field(default="airflow")

    # ─── Monitoring ────────────────────────────────────────────────────────────
    prometheus_port: int = Field(default=9090)
    prometheus_pushgateway_url: str = Field(default="http://pushgateway:9091")
    enable_metrics: bool = Field(default=True)

    # ─── ELK ───────────────────────────────────────────────────────────────────
    elasticsearch_hosts: str = Field(default="http://elasticsearch:9200")
    logstash_host: str = Field(default="logstash")
    logstash_port: int = Field(default=5000)
    enable_log_shipping: bool = Field(default=False)

    # ─── Pipeline ──────────────────────────────────────────────────────────────
    pipeline_version: str = Field(
        default="3.0.0",
        description="Stamped on every Silver/Gold row as _pipeline_version (Bronze uses its own constant).",
    )

    # ─── Privacy / governance ──────────────────────────────────────────────────
    pii_hash_key: SecretStr = Field(
        default=SecretStr("dev-only-pii-hash-key"),
        description="Secret key mixed into PII hashes (email, IP...). Must be set in prod.",
    )
    data_contract_mode: Literal["off", "warn", "enforce"] | None = Field(
        default=None,
        description="Silver data-contract enforcement. Unset: 'enforce' in staging/prod, 'warn' in dev.",
    )

    # ─── Data Quality ──────────────────────────────────────────────────────────
    dq_halt_on_critical: bool = Field(
        default=True,
        description="If True, pipeline halts when a CRITICAL DQ check fails.",
    )
    dq_notification_on_high: bool = Field(
        default=True,
        description="Send Slack/PagerDuty notification on HIGH DQ failures.",
    )

    # ─── Notifications ─────────────────────────────────────────────────────────
    slack_webhook_url: SecretStr | None = Field(default=None)
    pagerduty_routing_key: SecretStr | None = Field(default=None)

    # ─── Secrets backend ───────────────────────────────────────────────────────
    secrets_backend: Literal["env", "aws_secrets_manager"] = Field(
        default="env",
        description="Where to read secrets from. Use aws_secrets_manager in staging/prod.",
    )
    aws_secrets_manager_prefix: str = Field(default="/retailnexus/lakehouse/")

    # ─── Computed properties ───────────────────────────────────────────────────
    # URLs that embed the DB password are excluded from repr() so they never leak
    # into logs, tracebacks or pytest output.
    @computed_field(repr=False)  # type: ignore[misc]
    @property
    def postgres_dsn(self) -> str:
        """libpq keyword/value DSN for ``psycopg2.connect()``."""

        def q(value: str) -> str:
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

        return (
            f"host={q(self.postgres_host)} port={self.postgres_port} "
            f"dbname={q(self.postgres_db)} user={q(self.postgres_user)} "
            f"password={q(self.postgres_password.get_secret_value())}"
        )

    @computed_field(repr=False)  # type: ignore[misc]
    @property
    def postgres_sync_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}"
            f":{self.postgres_password.get_secret_value()}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field(repr=False)  # type: ignore[misc]
    @property
    def postgres_async_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}"
            f":{self.postgres_password.get_secret_value()}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field  # type: ignore[misc]
    @property
    def bronze_s3_path(self) -> str:
        return f"s3a://{self.lakehouse_bucket}/{self.bronze_prefix}"

    @computed_field  # type: ignore[misc]
    @property
    def silver_s3_path(self) -> str:
        return f"s3a://{self.lakehouse_bucket}/{self.silver_prefix}"

    @computed_field  # type: ignore[misc]
    @property
    def gold_s3_path(self) -> str:
        return f"s3a://{self.lakehouse_bucket}/{self.gold_prefix}"

    @property
    def raw_path(self) -> str:
        """Where raw source files live: object storage when there is one (MinIO / S3), else the repo folder.

        Spark workers cannot see the caller's filesystem, so a cluster run must read from object storage
        (``make upload-data`` mirrors ``datasets/raw`` into ``s3a://<bucket>/raw``).
        """
        if self.s3_endpoint_url or self.environment == "prod":
            return f"s3a://{self.lakehouse_bucket}/raw"
        return "datasets/raw"

    @property
    def contract_mode(self) -> str:
        """Effective data-contract mode (explicit setting wins; otherwise by environment)."""
        if self.data_contract_mode is not None:
            return self.data_contract_mode
        return "enforce" if self.environment in ("staging", "prod") else "warn"

    @computed_field  # type: ignore[misc]
    @property
    def is_production(self) -> bool:
        return self.environment == "prod"

    @computed_field  # type: ignore[misc]
    @property
    def log_level(self) -> str:
        return "WARNING" if self.is_production else "INFO"

    # ─── Validation ────────────────────────────────────────────────────────────
    @model_validator(mode="after")
    def validate_production_settings(self) -> Settings:
        if self.is_production:
            if self.api_secret_key.get_secret_value() == "dev-secret-change-in-production":
                raise ValueError("API_SECRET_KEY must be changed for production deployments")
            if self.secrets_backend != "aws_secrets_manager":
                raise ValueError("SECRETS_BACKEND must be 'aws_secrets_manager' in production")
            if self.pii_hash_key.get_secret_value() == "dev-only-pii-hash-key":
                raise ValueError("PII_HASH_KEY must be set to a secret value in production")
            if self.postgres_password.get_secret_value() == "lakehouse":
                raise ValueError(
                    "POSTGRES_PASSWORD must not be the development default in production"
                )
            if (
                self.aws_access_key_id == "minioadmin"
                or self.aws_secret_access_key.get_secret_value() == "minioadmin"
            ):
                raise ValueError("MinIO development credentials are not allowed in production")
            if self.s3_endpoint_url and "minio" in self.s3_endpoint_url:
                raise ValueError(
                    "S3_ENDPOINT_URL points at MinIO; unset it in production to use AWS S3"
                )
            if self.data_contract_mode == "off":
                raise ValueError("DATA_CONTRACT_MODE=off is not allowed in production")
        return self

    def s3_path(self, layer: str, domain: str) -> str:
        """Return the canonical S3 path for a given layer and domain."""
        prefix_map = {
            "bronze": self.bronze_prefix,
            "silver": self.silver_prefix,
            "gold": self.gold_prefix,
        }
        prefix = prefix_map.get(layer, layer)
        return f"s3a://{self.lakehouse_bucket}/{prefix}/{domain}"

    def kafka_consumer_group(self, topic_suffix: str) -> str:
        """Return a namespaced consumer group ID."""
        return f"{self.kafka_consumer_group_prefix}-{topic_suffix}"

    def checkpoint_path(self, stream_name: str) -> str:
        """Return the S3 path for a streaming job's checkpoint directory."""
        return f"s3a://lakehouse-checkpoints/{self.checkpoints_prefix}/{stream_name}"


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the singleton Settings instance.

    Cached after first call — safe to call from anywhere without performance cost.
    Clear the cache in tests with: get_settings.cache_clear()
    """
    return Settings()
