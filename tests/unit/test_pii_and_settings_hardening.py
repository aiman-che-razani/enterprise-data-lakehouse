"""Keyed PII hashing, PCI/PII coverage across Silver, and production guard rails in Settings."""

from __future__ import annotations

import hashlib

import pytest

from src.common.config.settings import Settings, get_settings
from src.processing.silver.domain_meta import SILVER_PII_DOMAINS, SILVER_PII_HANDLING
from src.processing.silver.pii_tokenizer import DEV_HASH_KEY, PiiTokenizer, keyed_hash
from src.quality.contracts import CONTRACTS

_PROD_ENV = {
    "ENVIRONMENT": "prod",
    "SECRETS_BACKEND": "aws_secrets_manager",
    "API_SECRET_KEY": "a-real-secret",
    "PII_HASH_KEY": "a-real-pii-key",
    "POSTGRES_PASSWORD": "a-real-db-password",
    "AWS_ACCESS_KEY_ID": "AKIAREALKEYID",
    "AWS_SECRET_ACCESS_KEY": "real-secret",
    "S3_ENDPOINT_URL": "",
}


@pytest.fixture()
def prod(monkeypatch):
    def _make(**overrides: str) -> Settings:
        monkeypatch.delenv("DATA_CONTRACT_MODE", raising=False)
        for k, v in {**_PROD_ENV, **overrides}.items():
            monkeypatch.setenv(k, v)
        get_settings.cache_clear()
        return Settings(_env_file=None)

    yield _make
    get_settings.cache_clear()


class TestKeyedHash:
    def test_deterministic_and_normalised(self):
        assert keyed_hash("A@B.com ", "k") == keyed_hash(" a@b.COM", "k")

    def test_key_changes_the_hash(self):
        assert keyed_hash("a@b.com", "k1") != keyed_hash("a@b.com", "k2")

    def test_not_recomputable_without_the_key(self):
        """The whole point: plain SHA-256 of a known email must NOT match the token."""
        assert keyed_hash("a@b.com", "secret") != hashlib.sha256(b"a@b.com").hexdigest()

    def test_null_stays_null(self):
        assert keyed_hash(None, "k") is None

    def test_empty_key_rejected(self):
        with pytest.raises(ValueError):
            PiiTokenizer("")

    def test_default_key_is_the_dev_key(self):
        assert PiiTokenizer()._hash_key == DEV_HASH_KEY


@pytest.mark.spark
class TestSparkEquivalence:
    def test_spark_expression_matches_python_twin(self, local_spark):
        from pyspark.sql import functions as F

        from src.processing.silver.pii_tokenizer import keyed_hash_column

        df = local_spark.sql(
            "SELECT ' User@Example.COM ' AS v UNION ALL SELECT CAST(NULL AS STRING)"
        )
        rows = {
            r.v: r.h
            for r in df.select("v", keyed_hash_column(F.col("v"), "k").alias("h")).collect()
        }
        assert rows[" User@Example.COM "] == keyed_hash(" User@Example.COM ", "k")
        assert rows[None] is None

    def test_tokenizer_hashes_drops_and_leaves_no_raw_value(self, local_spark):
        df = local_spark.sql(
            "SELECT 's1' AS supplier_id, 'a@b.com' AS contact_email, '555' AS contact_phone, 'x' AS name"
        )
        out = PiiTokenizer("k").tokenize(df, "suppliers")
        assert set(out.columns) == {"supplier_id", "name", "contact_email_hash"}
        assert out.first().contact_email_hash == keyed_hash("a@b.com", "k")


class TestCoverage:
    def test_every_pii_rule_domain_is_processed_as_pii(self):
        assert set(SILVER_PII_HANDLING) == SILVER_PII_DOMAINS

    @pytest.mark.parametrize(
        "domain,column",
        [
            ("payments", "card_exp"),
            ("payments", "card_network"),
            ("suppliers", "contact_email"),
            ("suppliers", "contact_phone"),
            ("stores", "address"),
            ("clickstream", "ip_address"),
            ("customers", "email"),
            ("customers", "phone"),
            ("employees", "email"),
        ],
    )
    def test_sensitive_column_is_handled_and_forbidden_in_the_contract(self, domain, column):
        assert column in SILVER_PII_HANDLING[domain]
        assert column in CONTRACTS[domain].forbidden_columns

    def test_hashed_columns_are_published_in_the_contract(self):
        for domain, rules in SILVER_PII_HANDLING.items():
            for col, strategy in rules.items():
                if strategy == "hash":
                    assert CONTRACTS[domain].column(f"{col}_hash") is not None, (domain, col)

    def test_pci_columns_never_pass_through(self):
        from src.catalog.sync import PCI_COLUMNS

        dropped = {c for r in SILVER_PII_HANDLING.values() for c, s in r.items() if s == "drop"}
        assert {"card_exp", "card_network"} <= dropped
        assert {"card_exp", "card_network"} <= PCI_COLUMNS


class TestProductionGuards:
    def test_valid_prod_settings_load(self, prod):
        assert prod().is_production

    @pytest.mark.parametrize(
        "override,message",
        [
            ({"PII_HASH_KEY": "dev-only-pii-hash-key"}, "PII_HASH_KEY"),
            ({"POSTGRES_PASSWORD": "lakehouse"}, "POSTGRES_PASSWORD"),
            ({"AWS_ACCESS_KEY_ID": "minioadmin"}, "MinIO"),
            ({"AWS_SECRET_ACCESS_KEY": "minioadmin"}, "MinIO"),
            ({"S3_ENDPOINT_URL": "http://minio:9000"}, "S3_ENDPOINT_URL"),
            ({"DATA_CONTRACT_MODE": "off"}, "DATA_CONTRACT_MODE"),
            ({"API_SECRET_KEY": "dev-secret-change-in-production"}, "API_SECRET_KEY"),
            ({"SECRETS_BACKEND": "env"}, "SECRETS_BACKEND"),
        ],
    )
    def test_dev_defaults_are_rejected_in_prod(self, prod, override, message):
        with pytest.raises(ValueError, match=message):
            prod(**override)

    def test_pii_key_never_in_repr(self, prod):
        assert "a-real-pii-key" not in repr(prod())


class TestContractMode:
    @pytest.mark.parametrize(
        "env,expected", [("dev", "warn"), ("staging", "enforce"), ("prod", "enforce")]
    )
    def test_default_by_environment(self, monkeypatch, env, expected):
        monkeypatch.delenv("DATA_CONTRACT_MODE", raising=False)
        monkeypatch.setenv("ENVIRONMENT", env)
        if env == "prod":
            for k, v in _PROD_ENV.items():
                monkeypatch.setenv(k, v)
        assert Settings(_env_file=None).contract_mode == expected

    def test_explicit_setting_wins(self, monkeypatch):
        monkeypatch.setenv("ENVIRONMENT", "staging")
        monkeypatch.setenv("DATA_CONTRACT_MODE", "warn")
        assert Settings(_env_file=None).contract_mode == "warn"

    def test_enforcer_follows_settings(self, monkeypatch):
        from types import SimpleNamespace

        from src.quality.contracts import ContractEnforcer

        monkeypatch.delenv("DATA_CONTRACT_MODE", raising=False)
        assert ContractEnforcer(settings=SimpleNamespace(contract_mode="enforce")).mode == "enforce"
        assert (
            ContractEnforcer(settings=SimpleNamespace(contract_mode="enforce"), mode="warn").mode
            == "warn"
        )
        assert ContractEnforcer(settings=object()).mode == "warn"  # no setting -> env/default
