"""
Secrets management for the RetailNexus Data Lakehouse.

Provides a unified SecretsClient that reads from:
  - AWS Secrets Manager (staging / prod)
  - Environment variables (dev — local Docker Compose)

The active backend is controlled by settings.secrets_backend.

Usage:
    from src.common.security import get_secrets_client

    client = get_secrets_client()
    db_password = client.get_secret("POSTGRES_PASSWORD")
    credentials = client.get_secret_json("retailnexus/kafka-credentials")
"""

from __future__ import annotations

import functools
import json
import os
from abc import ABC, abstractmethod
from typing import Any

from src.common.logging import get_logger

logger = get_logger(__name__)


class SecretsBackend(ABC):
    """Abstract base for secrets backends."""

    @abstractmethod
    def get_secret(self, secret_name: str) -> str:
        """Retrieve a secret value as a string."""

    @abstractmethod
    def get_secret_json(self, secret_name: str) -> dict[str, Any]:
        """Retrieve a secret value and parse it as JSON."""


class EnvSecretsBackend(SecretsBackend):
    """
    Development backend: reads secrets directly from environment variables.

    Secret names are upper-cased and used as env var names.
    This backend is only appropriate for local development.
    """

    def get_secret(self, secret_name: str) -> str:
        env_key = secret_name.upper().replace("/", "_").replace("-", "_")
        value = os.getenv(env_key)
        if value is None:
            raise SecretNotFoundError(
                f"Secret '{secret_name}' not found in environment (looked up as env var '{env_key}')"
            )
        return value

    def get_secret_json(self, secret_name: str) -> dict[str, Any]:
        raw = self.get_secret(secret_name)
        try:
            return dict(json.loads(raw))
        except json.JSONDecodeError as exc:
            raise SecretParseError(f"Secret '{secret_name}' is not valid JSON: {exc}") from exc


class AwsSecretsManagerBackend(SecretsBackend):
    """
    Production backend: reads secrets from AWS Secrets Manager.

    Secrets are cached in-memory for the lifetime of the process to avoid
    redundant API calls. Cache is invalidated when the process restarts
    (i.e., on Airflow task retry or ECS task restart).
    """

    def __init__(self, region_name: str, prefix: str = "") -> None:
        import boto3

        self._client = boto3.client("secretsmanager", region_name=region_name)
        self._prefix = prefix
        self._cache: dict[str, str] = {}

    def _full_name(self, secret_name: str) -> str:
        if self._prefix and not secret_name.startswith(self._prefix):
            return f"{self._prefix}{secret_name}"
        return secret_name

    def get_secret(self, secret_name: str) -> str:
        full_name = self._full_name(secret_name)
        if full_name in self._cache:
            return self._cache[full_name]

        try:

            response = self._client.get_secret_value(SecretId=full_name)
            value: str = response.get("SecretString") or ""
            if not value:
                raise SecretNotFoundError(f"Secret '{full_name}' exists but has no string value")

            self._cache[full_name] = value
            logger.info("secret_retrieved", secret_name=full_name)
            return value

        except Exception as exc:
            # Avoid leaking secret names in logs at DEBUG level
            logger.error("secret_retrieval_failed", secret_name=full_name, error=str(exc))
            raise SecretNotFoundError(f"Failed to retrieve secret '{full_name}': {exc}") from exc

    def get_secret_json(self, secret_name: str) -> dict[str, Any]:
        raw = self.get_secret(secret_name)
        try:
            return dict(json.loads(raw))
        except json.JSONDecodeError as exc:
            raise SecretParseError(f"Secret '{secret_name}' is not valid JSON: {exc}") from exc


class SecretsClient:
    """
    High-level secrets client.

    Wraps a SecretsBackend and adds convenience methods specific to the
    lakehouse domain (e.g., get_kafka_credentials, get_db_credentials).
    """

    def __init__(self, backend: SecretsBackend) -> None:
        self._backend = backend

    def get_secret(self, name: str) -> str:
        return self._backend.get_secret(name)

    def get_secret_json(self, name: str) -> dict[str, Any]:
        return self._backend.get_secret_json(name)

    def get_db_credentials(self) -> dict[str, str]:
        """Return Postgres credentials as a dict."""
        try:
            return self.get_secret_json("postgres-credentials")
        except SecretNotFoundError:
            return {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": os.getenv("POSTGRES_PORT", "5432"),
                "user": os.getenv("POSTGRES_USER", "lakehouse"),
                "password": os.getenv("POSTGRES_PASSWORD", "lakehouse"),
                "dbname": os.getenv("POSTGRES_DB", "lakehouse"),
            }

    def get_kafka_credentials(self) -> dict[str, str]:
        """Return Kafka SASL credentials (if configured)."""
        try:
            return self.get_secret_json("kafka-credentials")
        except SecretNotFoundError:
            return {
                "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            }

    def get_aws_credentials(self) -> dict[str, str]:
        """Return AWS/MinIO credentials."""
        try:
            return self.get_secret_json("aws-credentials")
        except SecretNotFoundError:
            return {
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
                "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            }


# ─── Errors ────────────────────────────────────────────────────────────────────


class SecretNotFoundError(Exception):
    """Raised when a secret cannot be found in the configured backend."""


class SecretParseError(Exception):
    """Raised when a secret value cannot be parsed as expected."""


# ─── Factory ───────────────────────────────────────────────────────────────────


@functools.lru_cache(maxsize=1)
def get_secrets_client() -> SecretsClient:
    """
    Return the singleton SecretsClient.

    Backend is selected based on the SECRETS_BACKEND environment variable:
        - "env"                  → EnvSecretsBackend (dev default)
        - "aws_secrets_manager"  → AwsSecretsManagerBackend
    """
    backend_name = os.getenv("SECRETS_BACKEND", "env")

    if backend_name == "aws_secrets_manager":
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        prefix = os.getenv("AWS_SECRETS_MANAGER_PREFIX", "/retailnexus/lakehouse/")
        backend: SecretsBackend = AwsSecretsManagerBackend(region_name=region, prefix=prefix)
        logger.info("secrets_backend_initialized", backend="aws_secrets_manager", region=region)
    else:
        backend = EnvSecretsBackend()
        logger.info("secrets_backend_initialized", backend="env")

    return SecretsClient(backend)
