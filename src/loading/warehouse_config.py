"""
Warehouse connection settings — pydantic-settings v2.

Credentials come from environment variables (dev) or AWS Secrets Manager (prod).
All fields are Optional so missing warehouse creds only fail when that
specific warehouse loader is invoked, not on import.
"""

from __future__ import annotations

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WarehouseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Redshift ──────────────────────────────────────────────────────────────
    redshift_host: str | None = Field(None, alias="REDSHIFT_HOST")
    redshift_port: int = Field(5439, alias="REDSHIFT_PORT")
    redshift_db: str = Field("retailnexus", alias="REDSHIFT_DB")
    redshift_user: str | None = Field(None, alias="REDSHIFT_USER")
    redshift_password: str | None = Field(None, alias="REDSHIFT_PASSWORD")
    redshift_iam_role: str | None = Field(None, alias="REDSHIFT_IAM_ROLE")
    redshift_schema: str = Field("gold", alias="REDSHIFT_SCHEMA")
    redshift_temp_s3_prefix: str = Field(
        "s3a://retailnexus-lakehouse/redshift-temp/",
        alias="REDSHIFT_TEMP_S3_PREFIX",
    )

    @computed_field  # type: ignore[misc]
    @property
    def redshift_jdbc_url(self) -> str | None:
        if not self.redshift_host:
            return None
        return f"jdbc:redshift://{self.redshift_host}:{self.redshift_port}" f"/{self.redshift_db}"

    # ── Snowflake ─────────────────────────────────────────────────────────────
    snowflake_account: str | None = Field(None, alias="SNOWFLAKE_ACCOUNT")
    snowflake_user: str | None = Field(None, alias="SNOWFLAKE_USER")
    snowflake_password: str | None = Field(None, alias="SNOWFLAKE_PASSWORD")
    snowflake_database: str = Field("RETAILNEXUS", alias="SNOWFLAKE_DATABASE")
    snowflake_schema: str = Field("GOLD", alias="SNOWFLAKE_SCHEMA")
    snowflake_warehouse: str = Field("RETAILNEXUS_LOAD_WH", alias="SNOWFLAKE_WAREHOUSE")
    snowflake_role: str = Field("RETAILNEXUS_ENGINEER", alias="SNOWFLAKE_ROLE")

    @computed_field  # type: ignore[misc]
    @property
    def snowflake_url(self) -> str | None:
        if not self.snowflake_account:
            return None
        return f"https://{self.snowflake_account}.snowflakecomputing.com"

    # ── BigQuery ──────────────────────────────────────────────────────────────
    bq_project: str = Field("retailnexus-prod", alias="BQ_PROJECT")
    bq_dataset: str = Field("gold", alias="BQ_DATASET")
    gcs_temp_bucket: str = Field("retailnexus-bq-temp", alias="GCS_TEMP_BUCKET")
    google_application_credentials: str | None = Field(None, alias="GOOGLE_APPLICATION_CREDENTIALS")

    @computed_field  # type: ignore[misc]
    @property
    def bq_temp_gcs_uri(self) -> str:
        return f"gs://{self.gcs_temp_bucket}/bq-temp/"
