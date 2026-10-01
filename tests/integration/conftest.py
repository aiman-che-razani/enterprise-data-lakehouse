"""
Integration test fixtures.

These tests require live services. They are skipped automatically when the
required environment variables are absent (CI without docker-compose, local
runs without 'make up').

Required env vars:
  TEST_DATABASE_URL   — psycopg2 DSN for the running PostgreSQL
  TEST_API_URL        — base URL of the running FastAPI service
  TEST_API_KEY        — valid API key for auth (seeded by init_postgres.sql)

Optional:
  SPARK_MASTER        — spark://host:7077  (used by pipeline integration tests)
"""

from __future__ import annotations

import os

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Skip sentinel helpers
# ─────────────────────────────────────────────────────────────────────────────
def _require_env(*names: str) -> None:
    """Skip the calling test if any required env vars are absent."""
    missing = [n for n in names if not os.getenv(n)]
    if missing:
        pytest.skip(f"integration env vars not set: {', '.join(missing)}")


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL connection
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def pg_conn():
    """
    Live psycopg2 connection to the pipeline PostgreSQL database.
    Skips if TEST_DATABASE_URL is not set.
    """
    _require_env("TEST_DATABASE_URL")
    import psycopg2

    conn = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
    conn.autocommit = True
    yield conn
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# API client
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def api_headers():
    """Return HTTP headers containing the integration test API key."""
    _require_env("TEST_API_KEY")
    return {"X-API-Key": os.environ["TEST_API_KEY"]}


@pytest.fixture(scope="session")
def api_base_url() -> str:
    _require_env("TEST_API_URL")
    return os.environ["TEST_API_URL"].rstrip("/")


@pytest.fixture(scope="session")
def api_client(api_base_url, api_headers):
    """
    httpx client pointed at the live API service.
    Skips if TEST_API_URL or TEST_API_KEY are not set.
    """
    try:
        import httpx
    except ImportError:
        pytest.skip("httpx not installed")

    with httpx.Client(base_url=api_base_url, headers=api_headers, timeout=10.0) as client:
        yield client


# ─────────────────────────────────────────────────────────────────────────────
# Spark session (pipeline integration tests only)
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def integration_spark():
    """
    Minimal SparkSession wired for local Delta Lake I/O.
    Skips if pyspark is not importable or SPARK_MASTER is not set.
    """
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("pyspark / delta not installed")

    builder = (
        SparkSession.builder.appName("integration-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
    )

    master = os.getenv("SPARK_MASTER")
    builder = builder.master(master) if master else builder.master("local[2]")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
