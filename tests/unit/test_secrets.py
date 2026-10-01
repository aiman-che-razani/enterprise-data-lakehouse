"""
Unit tests for src.common.security.secrets.

Tests cover:
- EnvSecretsBackend: happy path, missing secret, JSON parsing
- SecretsClient: high-level credential getters with env fallback
- get_secrets_client(): singleton + backend selection
"""

from __future__ import annotations

import json

import pytest

from src.common.security.secrets import (
    EnvSecretsBackend,
    SecretNotFoundError,
    SecretParseError,
    SecretsClient,
    get_secrets_client,
)

pytestmark = pytest.mark.unit


class TestEnvSecretsBackend:
    def test_get_secret_reads_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_SECRET_VALUE", "supersecret")
        backend = EnvSecretsBackend()
        assert backend.get_secret("MY_SECRET_VALUE") == "supersecret"

    def test_get_secret_normalises_name_to_upper(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_PASSWORD", "kafka-pass")
        backend = EnvSecretsBackend()
        assert backend.get_secret("kafka_password") == "kafka-pass"

    def test_get_secret_replaces_slashes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RETAILNEXUS_LAKEHOUSE_DB", "value")
        backend = EnvSecretsBackend()
        assert backend.get_secret("retailnexus/lakehouse/db") == "value"

    def test_get_secret_raises_when_not_found(self) -> None:
        backend = EnvSecretsBackend()
        with pytest.raises(SecretNotFoundError, match="NOT_AN_ENV_VAR_12345"):
            backend.get_secret("NOT_AN_ENV_VAR_12345")

    def test_get_secret_json_parses_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        payload = {"host": "db.internal", "port": 5432}
        monkeypatch.setenv("DB_CREDENTIALS", json.dumps(payload))
        backend = EnvSecretsBackend()
        result = backend.get_secret_json("DB_CREDENTIALS")
        assert result == payload

    def test_get_secret_json_raises_on_invalid_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BROKEN_JSON", "not-json")
        backend = EnvSecretsBackend()
        with pytest.raises(SecretParseError):
            backend.get_secret_json("BROKEN_JSON")


class TestSecretsClient:
    def test_get_db_credentials_falls_back_to_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_HOST", "test-host")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("POSTGRES_DB", "testdb")

        backend = EnvSecretsBackend()
        client = SecretsClient(backend)
        creds = client.get_db_credentials()

        assert creds["host"] == "test-host"
        assert creds["port"] == "5433"
        assert creds["user"] == "testuser"
        assert creds["password"] == "testpass"
        assert creds["dbname"] == "testdb"

    def test_get_kafka_credentials_falls_back_to_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092")
        backend = EnvSecretsBackend()
        client = SecretsClient(backend)
        creds = client.get_kafka_credentials()
        assert creds["bootstrap_servers"] == "broker1:9092"

    def test_get_aws_credentials_falls_back_to_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATEST")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
        backend = EnvSecretsBackend()
        client = SecretsClient(backend)
        creds = client.get_aws_credentials()
        assert creds["aws_access_key_id"] == "AKIATEST"
        assert creds["region_name"] == "eu-west-1"


class TestGetSecretsClientFactory:
    def test_returns_client_with_env_backend(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SECRETS_BACKEND", "env")
        get_secrets_client.cache_clear()
        client = get_secrets_client()
        assert isinstance(client, SecretsClient)
        get_secrets_client.cache_clear()

    def test_singleton_returns_same_instance(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SECRETS_BACKEND", "env")
        get_secrets_client.cache_clear()
        c1 = get_secrets_client()
        c2 = get_secrets_client()
        assert c1 is c2
        get_secrets_client.cache_clear()
