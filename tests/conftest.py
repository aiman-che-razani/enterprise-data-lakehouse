"""
Global pytest fixtures for the RetailNexus Data Lakehouse test suite.

Fixtures defined here are available to all tests without explicit import.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from src.common.config.settings import Settings, get_settings
from src.common.logging.logger import setup_logging

if TYPE_CHECKING:
    from collections.abc import Generator
    from unittest.mock import MagicMock

_ROOT = Path(__file__).resolve().parents[1]

# The API rate limiter is process-global; hundreds of TestClient requests from one address would trip it.
# tests/unit/api/test_ratelimit.py installs its own limiter explicitly.
os.environ.setdefault("API_RATE_LIMIT_PER_MINUTE", "0")

# Airflow puts $AIRFLOW_HOME/plugins on sys.path, so plugin packages import as top-level
# ``operators`` / ``sensors`` / ``hooks``. Mirror that here so DAG and plugin code imports the
# same way in tests as in the container (the repo-root ``airflow/`` folder is not a package).
_PLUGINS = str(_ROOT / "airflow" / "plugins")
if _PLUGINS not in sys.path:
    sys.path.insert(0, _PLUGINS)

_AIRFLOW_TEST_FILES = {"test_airflow_dags.py", "test_airflow_operators.py"}
_TIER_BY_DIR = {"unit": "unit", "integration": "integration", "pipeline": "pipeline"}


def _airflow_installed() -> bool:
    try:
        return importlib.util.find_spec("airflow.models") is not None
    except (ImportError, ValueError):
        return False


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark every test by its directory (unit/integration/pipeline) and gate Airflow tests.

    CI selects ``-m unit``; only three files carried that marker by hand, so most of the unit
    suite silently never ran there. Directory-based marking makes the selection complete.
    """
    airflow_ok = _airflow_installed()
    skip_airflow = pytest.mark.skip(
        reason="Apache Airflow is not installed (runs in the airflow CI job / container)"
    )
    for item in items:
        path = Path(str(item.fspath))
        parts = path.parts
        if "tests" in parts:
            tier = _TIER_BY_DIR.get(parts[parts.index("tests") + 1])
            if tier and item.get_closest_marker(tier) is None:
                item.add_marker(getattr(pytest.mark, tier))
        if path.name in _AIRFLOW_TEST_FILES:
            item.add_marker(pytest.mark.airflow)
            if not airflow_ok:
                item.add_marker(skip_airflow)


# ─── Logging ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session", autouse=True)
def configure_logging() -> None:
    """Set up structured logging once for the entire test session."""
    setup_logging(level="WARNING", json_output=False)


# ─── Settings ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def dev_settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    """
    Return a Settings instance with known dev values.

    Clears the lru_cache so each test starts fresh.
    Uses monkeypatch to set env vars without leaking into other tests.
    """
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_USER", "lakehouse")
    monkeypatch.setenv("POSTGRES_PASSWORD", "lakehouse")
    monkeypatch.setenv("POSTGRES_DB", "lakehouse")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minioadmin")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("SECRETS_BACKEND", "env")

    get_settings.cache_clear()
    settings = Settings()
    yield settings
    get_settings.cache_clear()


@pytest.fixture()
def mock_aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub out AWS credentials for tests that touch boto3."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:9000")


# ─── Moto S3 ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def moto_s3(mock_aws_credentials: None) -> Generator[MagicMock, None, None]:
    """
    Provide a mocked S3 environment using moto.

    Creates a 'retailnexus-lakehouse' bucket with bronze/silver/gold prefixes.
    """
    try:
        import boto3
        from moto import mock_aws

        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="retailnexus-lakehouse")
            s3.create_bucket(Bucket="lakehouse-checkpoints")
            yield s3
    except ImportError:
        pytest.skip("moto not installed")


# ─── Secrets ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def mock_secrets_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure env-based secrets backend for tests."""
    monkeypatch.setenv("SECRETS_BACKEND", "env")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-db-password")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    from src.common.security.secrets import get_secrets_client

    get_secrets_client.cache_clear()
    yield
    get_secrets_client.cache_clear()


# ─── Spark (local mode, skipped when a JVM/Spark cannot start) ─────────────────


@pytest.fixture()
def local_spark():
    """Local[1] SparkSession; skips dependants if Spark cannot start.

    Function-scoped on purpose: other test modules stop the shared session in their own
    teardown, and ``getOrCreate()`` transparently builds a fresh one afterwards. We never
    stop it here for the same reason. PySpark 3.5 needs JDK 8/11/17 - on an unsupported JDK
    the JVM gateway fails to launch, so skip rather than error.
    """
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    # Another test module may have left a Delta-configured session behind (DeltaCatalog set but the
    # Delta jars not on the classpath); reusing it breaks every query. Start clean in that case.
    stale = SparkSession.getActiveSession()
    if stale is not None and "DeltaCatalog" in stale.conf.get(
        "spark.sql.catalog.spark_catalog", ""
    ):
        stale.stop()

    try:
        return (
            SparkSession.builder.master("local[1]")
            .appName("lakehouse-unit-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
    except Exception as exc:  # py4j raises a variety of errors here
        pytest.skip(f"Spark could not start in this environment: {type(exc).__name__}")
