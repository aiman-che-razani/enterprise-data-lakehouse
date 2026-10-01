"""
Drift guard between scripts/setup/init_postgres.sql and the code that reads/writes it.

The DDL and the pipeline/API code once disagreed on almost every column (the Bronze
ingestor keyed watermarks by ``source_name`` while everything else used ``domain``, the
API selected columns that did not exist, ...). Each entry below is a column some code
path depends on — add to it when code starts using a new column.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SQL = (_ROOT / "scripts" / "setup" / "init_postgres.sql").read_text(encoding="utf-8")

_NON_COLUMN = ("PRIMARY", "UNIQUE", "CONSTRAINT", "CHECK", "FOREIGN")


def _tables() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS ([\w.]+)\s*\((.*?)\n\);", _SQL, re.S):
        cols = set()
        for line in m.group(2).split("\n"):
            tok = line.strip().split(" ")[0].strip(",")
            if tok and not tok.startswith("--") and tok.upper() not in _NON_COLUMN:
                cols.add(tok)
        out[m.group(1)] = cols
    return out


TABLES = _tables()

REQUIRED = {
    # written by Bronze ingestor / Silver / Gold / warehouse loaders; read by API, DAGs, sensors
    "pipeline.watermarks": {
        "domain",
        "bronze_last_ingest",
        "bronze_watermark",
        "bronze_rows_written",
        "bronze_last_status",
        "silver_last_run",
        "silver_bronze_version",
        "silver_rows_written",
        "gold_last_run",
        "gold_rows_written",
        "redshift_last_load",
        "snowflake_last_load",
        "bigquery_last_load",
        "updated_at",
    },
    # API PipelineRepository + bronze_batch_dag insert
    "pipeline.runs": {
        "id",
        "run_id",
        "dag_id",
        "execution_date",
        "layer",
        "status",
        "rows_written",
        "error_message",
        "started_at",
        "ended_at",
    },
    # PostgresReporter.save_sla_misses + API/DAG readers
    "pipeline.sla_misses": {
        "id",
        "run_id",
        "domain",
        "layer",
        "metric",
        "actual_value",
        "sla_threshold",
        "message",
        "detected_at",
    },
    # PostgresReporter.save_results + RowCountCheck history + QualityRepository
    "quality.check_results": {
        "id",
        "run_id",
        "domain",
        "layer",
        "check_name",
        "column_name",
        "status",
        "metric_value",
        "threshold",
        "message",
        "rows_checked",
        "run_at",
    },
    # PostgresReporter.save_report + QualityRepository + metrics refresh
    "quality.reports": {
        "id",
        "run_id",
        "domain",
        "layer",
        "total_checks",
        "passed",
        "failed",
        "warnings",
        "errors",
        "pass_rate",
        "run_at",
    },
    # ContractEnforcer._persist
    "quality.contract_violations": {
        "run_id",
        "domain",
        "contract_version",
        "rule",
        "column_name",
        "severity",
        "message",
        "detected_at",
    },
    # DeltaCatalogManager, CatalogSynchronizer, API catalog router
    "catalog.tables": {
        "table_id",
        "schema_name",
        "table_name",
        "layer",
        "domain",
        "description",
        "s3_location",
        "format",
        "partition_columns",
        "primary_key_columns",
        "row_count",
        "size_bytes",
        "schema_json",
        "created_at",
        "updated_at",
    },
    "catalog.columns": {
        "table_id",
        "column_name",
        "data_type",
        "ordinal_position",
        "is_nullable",
        "is_pii",
        "is_pci",
        "description",
    },
    # LineageTracker + API lineage endpoint
    "catalog.lineage": {
        "source_table",
        "target_table",
        "transformation_type",
        "job_name",
        "run_id",
    },
    # api.dependencies.verify_api_key
    "api.api_keys": {"key_id", "key_hash", "client_name", "permissions", "is_active", "expires_at"},
}


@pytest.mark.parametrize("table", sorted(REQUIRED))
def test_table_has_every_column_the_code_uses(table):
    assert table in TABLES, f"{table} is not created by init_postgres.sql"
    missing = REQUIRED[table] - TABLES[table]
    assert not missing, f"{table} is missing columns used by the code: {sorted(missing)}"


def test_watermark_table_is_keyed_by_domain():
    assert re.search(
        r"pipeline\.watermarks\s*\(\s*domain\s+VARCHAR\(\d+\)\s+NOT NULL PRIMARY KEY", _SQL
    )


def test_every_warehouse_target_has_a_watermark_column():
    # BaseWarehouseLoader writes f"{target}_last_load"
    for target in ("redshift", "snowflake", "bigquery"):
        assert f"{target}_last_load" in TABLES["pipeline.watermarks"]


def test_lineage_upsert_has_a_matching_unique_constraint():
    # LineageTracker uses ON CONFLICT (source_table, target_table, job_name)
    assert "UNIQUE (source_table, target_table, job_name)" in _SQL


def test_catalog_upsert_has_a_matching_unique_constraint():
    # DeltaCatalogManager / CatalogSynchronizer use ON CONFLICT (schema_name, table_name)
    assert "UNIQUE (schema_name, table_name)" in _SQL


def test_check_status_values_are_not_constrained_to_uppercase():
    # CheckStatus values are lower-case; an upper-case CHECK constraint rejects every insert.
    block = re.search(
        r"CREATE TABLE IF NOT EXISTS quality\.check_results.*?\n\);", _SQL, re.S
    ).group(0)
    assert "CHECK" not in block


def test_every_domain_and_gold_table_seeded_in_watermarks():
    from src.processing.gold.domain_meta import GOLD_DIM_ORDER, GOLD_FACT_ORDER
    from src.processing.silver.domain_meta import SILVER_PROCESSING_ORDER

    seeded = set()
    for seed in re.findall(
        r"INSERT INTO pipeline\.watermarks \(domain\)\s*VALUES(.*?)ON CONFLICT", _SQL, re.S
    ):
        seeded |= set(re.findall(r"'(\w+)'", seed))
    assert seeded == set(SILVER_PROCESSING_ORDER) | set(GOLD_DIM_ORDER) | set(GOLD_FACT_ORDER)


def test_reports_have_a_unique_key_matching_the_reporter_upsert():
    assert "UNIQUE (run_id, domain, layer)" in _SQL
    reporter = (_ROOT / "src" / "quality" / "reporters" / "pg_reporter.py").read_text(
        encoding="utf-8"
    )
    assert "ON CONFLICT (run_id, domain, layer) DO UPDATE" in reporter


def test_lineage_job_name_cannot_be_null():
    block = re.search(r"CREATE TABLE IF NOT EXISTS catalog\.lineage.*?\);", _SQL, re.S).group(0)
    assert re.search(r"job_name\s+VARCHAR\(256\)\s+NOT NULL DEFAULT ''", block)


_SEED = (_ROOT / "scripts" / "setup" / "seed_dev_data.sql").read_text(encoding="utf-8")


def test_init_script_seeds_no_credentials():
    """init_postgres.sql is what DEPLOYMENT.md runs against production: it must not carry a key."""
    assert "INSERT INTO api.api_keys" not in _SQL
    assert "dev-api-key" not in _SQL


def test_dev_api_key_seed_is_dev_only_hashed_and_expiring():
    import hashlib

    assert "LOCAL DEVELOPMENT ONLY" in _SEED
    assert "encode(sha256('dev-api-key-retailnexus-2026'::bytea), 'hex')" in _SEED
    assert "expires_at" in _SEED and "INTERVAL '1 year'" in _SEED
    assert len(hashlib.sha256(b"dev-api-key-retailnexus-2026").hexdigest()) == 64


def test_only_the_dev_compose_stack_mounts_the_seed():
    compose = (_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "seed_dev_data.sql:/docker-entrypoint-initdb.d/02_seed_dev.sql" in compose
    for f in (_ROOT / ".github" / "workflows").glob("cd-*.yml"):
        assert "seed_dev_data" not in f.read_text(encoding="utf-8")
    deploy_doc = (_ROOT / "docs" / "deployment" / "DEPLOYMENT.md").read_text(encoding="utf-8")
    assert (
        "-f scripts/setup/seed_dev_data.sql" not in deploy_doc
    )  # only mentioned to warn against it


def test_updated_at_triggers_are_recreatable():
    # init script must be re-runnable: triggers are dropped before being created
    for trg, table in (
        ("trg_watermarks_updated_at", "pipeline.watermarks"),
        ("trg_catalog_tables_updated_at", "catalog.tables"),
    ):
        assert f"DROP TRIGGER IF EXISTS {trg} ON {table}" in _SQL
