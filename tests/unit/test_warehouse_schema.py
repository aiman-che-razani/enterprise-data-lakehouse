"""
Schema consistency tests for warehouse DDL files.

Verifies that all 14 Gold tables appear in each warehouse DDL file,
and that the column count across warehouses is consistent.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_BASE = Path(__file__).parent.parent.parent / "warehouse"

_EXPECTED_TABLES = {
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_store",
    "dim_employee",
    "dim_supplier",
    "dim_location",
    "dim_promotion",
    "fact_sales",
    "fact_inventory",
    "fact_marketing",
    "fact_returns",
    "fact_shipments",
    "fact_finance",
}


def _extract_tables(sql: str) -> set[str]:
    """Return set of table names created in a SQL DDL file."""
    # Matches: CREATE [OR REPLACE] TABLE [IF NOT EXISTS] [schema.]table_name
    # The (possibly quoted, possibly dotted) identifier is captured whole and the
    # last segment is the table name, e.g. `project.dataset.dim_date` -> dim_date.
    pattern = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"((?:[`\"]?[\w-]+[`\"]?\.)*[`\"]?[\w-]+[`\"]?)",
        re.IGNORECASE,
    )
    return {m.group(1).split(".")[-1].strip('`"') for m in pattern.finditer(sql)}


class TestRedshiftDDL:
    @pytest.fixture
    def ddl(self):
        return (_BASE / "redshift" / "ddl" / "gold_schema.sql").read_text()

    def test_all_14_tables_present(self, ddl):
        found = _extract_tables(ddl)
        missing = _EXPECTED_TABLES - found
        assert not missing, f"Missing from Redshift DDL: {missing}"

    def test_distkey_on_facts(self, ddl):
        assert "DISTKEY" in ddl, "Redshift fact tables should have DISTKEY"

    def test_diststyle_all_on_dims(self, ddl):
        assert "DISTSTYLE ALL" in ddl, "Redshift dim tables should use DISTSTYLE ALL"

    def test_az64_encoding_used(self, ddl):
        assert "AZ64" in ddl, "Redshift DDL should use AZ64 encoding for numerics"

    def test_zstd_encoding_on_strings(self, ddl):
        assert "ZSTD" in ddl, "Redshift DDL should use ZSTD encoding for strings"


class TestSnowflakeDDL:
    @pytest.fixture
    def ddl(self):
        return (_BASE / "snowflake" / "ddl" / "gold_schema.sql").read_text()

    def test_all_14_tables_present(self, ddl):
        found = _extract_tables(ddl)
        missing = _EXPECTED_TABLES - found
        assert not missing, f"Missing from Snowflake DDL: {missing}"

    def test_cluster_by_on_tables(self, ddl):
        assert "CLUSTER BY" in ddl, "Snowflake tables should define CLUSTER BY"

    def test_timestamp_ntz_used(self, ddl):
        assert "TIMESTAMP_NTZ" in ddl, "Snowflake DDL should use TIMESTAMP_NTZ (no timezone)"

    def test_number_type_for_sk(self, ddl):
        assert "NUMBER(19)" in ddl, "Snowflake surrogate keys should be NUMBER(19)"


class TestBigQueryDDL:
    @pytest.fixture
    def ddl(self):
        return (_BASE / "bigquery" / "ddl" / "gold_schema.sql").read_text()

    def test_all_14_tables_present(self, ddl):
        found = _extract_tables(ddl)
        missing = _EXPECTED_TABLES - found
        assert not missing, f"Missing from BigQuery DDL: {missing}"

    def test_partition_by_present(self, ddl):
        assert "PARTITION BY" in ddl, "BigQuery tables should be partitioned"

    def test_cluster_by_present(self, ddl):
        assert "CLUSTER BY" in ddl, "BigQuery tables should define CLUSTER BY"

    def test_int64_for_surrogate_keys(self, ddl):
        assert "INT64" in ddl, "BigQuery surrogate keys should be INT64"

    def test_numeric_for_money(self, ddl):
        assert "NUMERIC" in ddl, "BigQuery monetary columns should use NUMERIC"


class TestDDLConsistency:
    """Cross-warehouse consistency checks."""

    def _tables_in(self, warehouse: str) -> set[str]:
        path = _BASE / warehouse / "ddl" / "gold_schema.sql"
        return _extract_tables(path.read_text())

    def test_same_tables_in_all_warehouses(self):
        rs = self._tables_in("redshift")
        sf = self._tables_in("snowflake")
        bq = self._tables_in("bigquery")

        assert rs == sf == bq == _EXPECTED_TABLES, (
            f"Table mismatch across warehouses:\n"
            f"  Redshift:  {rs}\n"
            f"  Snowflake: {sf}\n"
            f"  BigQuery:  {bq}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Column-level parity, migrations and safety (added with the DDL hardening)
# ─────────────────────────────────────────────────────────────────────────────
def _columns(sql: str) -> dict[str, dict[str, bool]]:
    """table -> {column: is_not_null} for every CREATE TABLE in *sql*."""
    tables: dict[str, dict[str, bool]] = {}
    for m in re.finditer(
        r"CREATE (?:OR REPLACE )?TABLE (?:IF NOT EXISTS )?`?([\w.]+)`?\s*\((.*?)\n\)",
        sql,
        re.S | re.IGNORECASE,
    ):
        cols: dict[str, bool] = {}
        for line in m.group(2).split("\n"):
            line = line.strip().rstrip(",")
            if (
                not line
                or line.startswith("--")
                or line.upper().startswith(("PRIMARY", "UNIQUE", "FOREIGN", "CONSTRAINT"))
            ):
                continue
            cols[line.split()[0]] = "NOT NULL" in line.upper()
        tables[m.group(1).split(".")[-1]] = cols
    return tables


_DDL = {
    w: (_BASE / w / "ddl" / "gold_schema.sql").read_text(encoding="utf-8")
    for w in ("redshift", "snowflake", "bigquery")
}
_MIGRATIONS = {w: _BASE / w / "migrations" for w in _DDL}


class TestColumnParity:
    cols = {w: _columns(sql) for w, sql in _DDL.items()}

    @pytest.mark.parametrize("table", sorted(_EXPECTED_TABLES))
    def test_same_columns_in_every_warehouse(self, table):
        sets = {w: set(self.cols[w][table]) for w in self.cols}
        assert sets["redshift"] == sets["snowflake"] == sets["bigquery"], f"{table}: {sets}"

    @pytest.mark.parametrize("table", sorted(_EXPECTED_TABLES))
    def test_same_nullability_in_every_warehouse(self, table):
        """A NOT NULL in one warehouse only aborts that warehouse's load when the builder emits NULL."""
        rs = self.cols["redshift"][table]
        for w in ("snowflake", "bigquery"):
            diff = {c for c in rs if rs[c] != self.cols[w][table][c]}
            assert not diff, f"{table}.{sorted(diff)} nullability differs between redshift and {w}"

    def test_fact_shipments_store_key_is_nullable_everywhere(self):
        # The builder emits NULL when the order has no store (left join).
        for w in self.cols:
            assert self.cols[w]["fact_shipments"]["sk_store_id"] is False, w

    def test_fact_shipments_date_key_is_required_and_always_populated(self):
        for w in self.cols:
            assert self.cols[w]["fact_shipments"]["sk_date_id"] is True, w
        builder = (
            _BASE.parent / "src" / "processing" / "gold" / "facts" / "fact_shipments.py"
        ).read_text(encoding="utf-8")
        assert 'F.coalesce(F.col("s.ship_date"), F.to_date(F.col("s.created_at")))' in builder


class TestMigrations:
    @pytest.mark.parametrize("w", sorted(_MIGRATIONS))
    def test_table_migration_exists_and_matches_ddl(self, w):
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "build_migrations", _BASE.parent / "scripts" / "deployment" / "build_migrations.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        path = _MIGRATIONS[w] / mod.NAME
        assert path.is_file(), f"missing {path.name}"
        assert path.read_text(encoding="utf-8").replace("\r\n", "\n") == mod.render(
            w
        ), f"{w} migration is stale — run scripts/deployment/build_migrations.py"

    @pytest.mark.parametrize("w", sorted(_MIGRATIONS))
    def test_migrations_create_every_table(self, w):
        sql = next(_MIGRATIONS[w].glob("V002*.sql")).read_text(encoding="utf-8")
        assert _extract_tables(sql) >= _EXPECTED_TABLES

    @pytest.mark.parametrize("w", sorted(_MIGRATIONS))
    def test_no_destructive_or_runner_specific_statements(self, w):
        for path in _MIGRATIONS[w].glob("*.sql"):
            body = "\n".join(
                l
                for l in path.read_text(encoding="utf-8").splitlines()
                if not l.strip().startswith("--")
            )
            assert not re.search(
                r"OR\s+REPLACE\s+TABLE|DROP\s+TABLE|TRUNCATE|^\\i\s", body, re.I | re.M
            ), path.name

    @pytest.mark.parametrize("w", sorted(_DDL))
    def test_ddl_is_idempotent(self, w):
        assert not re.search(r"OR\s+REPLACE\s+TABLE", _DDL[w], re.I)
        assert len(re.findall(r"^CREATE TABLE IF NOT EXISTS", _DDL[w], re.M)) == 14

    def test_snowflake_grants_cover_future_tables(self):
        v1 = (_MIGRATIONS["snowflake"] / "V001__create_gold_schema.sql").read_text(encoding="utf-8")
        assert (
            "ON FUTURE TABLES IN SCHEMA" in v1 and "ALTER DEFAULT PRIVILEGES" not in v1
        )  # not Snowflake syntax

    def test_redshift_v001_has_no_psql_include(self):
        assert "\\i " not in (_MIGRATIONS["redshift"] / "V001__create_gold_schema.sql").read_text(
            encoding="utf-8"
        )


class TestDistribution:
    def test_dim_customer_is_key_distributed_not_replicated(self):
        block = _DDL["redshift"][_DDL["redshift"].index("gold.dim_customer") :]
        block = block[: block.index(";")]
        assert "DISTKEY (sk_customer_id)" in block and "DISTSTYLE ALL" not in block

    def test_small_dimensions_stay_replicated(self):
        code = "\n".join(l for l in _DDL["redshift"].splitlines() if not l.startswith("--"))
        assert code.count("DISTSTYLE ALL") == 7  # 8 dims minus dim_customer

    def test_snowflake_comment_matches_reality(self):
        assert "double-quoted" not in _DDL["snowflake"]

    def test_bigquery_header_documents_business_date_partitions(self):
        header = _DDL["bigquery"][:900]
        assert "snapshot_date" in header and "event_date" in header and "transaction_date" in header
