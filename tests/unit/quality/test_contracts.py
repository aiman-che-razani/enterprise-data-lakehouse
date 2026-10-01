"""Data contract models, definitions, schema checks, and (Spark-backed) data checks."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.processing.silver.domain_meta import SILVER_PII_HANDLING, SILVER_PROCESSING_ORDER
from src.quality.contracts import (
    CONTRACTS,
    ColumnContract,
    ContractEnforcer,
    ContractViolationError,
    DataContract,
    Severity,
    TypeFamily,
    check_schema,
    get_contract,
    resolve_mode,
)
from src.quality.contracts.definitions import SILVER_METADATA_COLUMNS
from src.quality.contracts.enforcer import type_family
from src.quality.contracts.models import ContractViolation

_SPARK_TYPE = {
    TypeFamily.STRING: "string",
    TypeFamily.INTEGER: "bigint",
    TypeFamily.DOUBLE: "double",
    TypeFamily.BOOLEAN: "boolean",
    TypeFamily.TIMESTAMP: "timestamp",
    TypeFamily.DATE: "date",
}


def _dtypes(contract: DataContract) -> list[tuple[str, str]]:
    """A dtypes list that satisfies *contract* exactly."""
    return [(c.name, _SPARK_TYPE[c.type]) for c in contract.columns]


# ─── Models ───────────────────────────────────────────────────────────────────
class TestModels:
    def test_allowed_values_only_for_strings(self):
        with pytest.raises(ValueError, match="allowed_values"):
            ColumnContract("n", TypeFamily.INTEGER, allowed_values=("1",))

    def test_min_max_only_for_numerics(self):
        with pytest.raises(ValueError, match="numeric"):
            ColumnContract("s", TypeFamily.STRING, min_value=0)

    def test_min_greater_than_max_rejected(self):
        with pytest.raises(ValueError, match="min_value > max_value"):
            ColumnContract("n", TypeFamily.INTEGER, min_value=5, max_value=1)

    def test_contract_requires_pk_in_columns(self):
        with pytest.raises(ValueError, match="primary key"):
            DataContract("d", "1.0.0", "o", "id", (ColumnContract("x", TypeFamily.STRING),))

    def test_contract_rejects_duplicate_columns(self):
        col = ColumnContract("id", TypeFamily.STRING)
        with pytest.raises(ValueError, match="duplicate"):
            DataContract("d", "1.0.0", "o", "id", (col, col))

    def test_contract_rejects_required_and_forbidden_overlap(self):
        col = ColumnContract("id", TypeFamily.STRING)
        with pytest.raises(ValueError, match="both required and forbidden"):
            DataContract("d", "1.0.0", "o", "id", (col,), forbidden_columns=("id",))


# ─── Definitions ──────────────────────────────────────────────────────────────
class TestDefinitions:
    def test_every_silver_domain_has_a_contract(self):
        assert set(CONTRACTS) == set(SILVER_PROCESSING_ORDER)

    def test_get_contract_unknown_domain_is_none(self):
        assert get_contract("not_a_domain") is None

    @pytest.mark.parametrize("domain", sorted(CONTRACTS))
    def test_primary_key_is_non_nullable(self, domain):
        contract = CONTRACTS[domain]
        assert contract.column(contract.primary_key).nullable is False

    @pytest.mark.parametrize("domain", sorted(CONTRACTS))
    def test_metadata_columns_present(self, domain):
        names = {c.name for c in CONTRACTS[domain].columns}
        assert {c.name for c in SILVER_METADATA_COLUMNS} <= names

    def test_pii_domains_forbid_raw_pii(self):
        # Every column the tokenizer drops or replaces must be forbidden in the contract.
        for domain, handling in SILVER_PII_HANDLING.items():
            forbidden = set(CONTRACTS[domain].forbidden_columns)
            assert (
                set(handling) <= forbidden
            ), f"{domain}: {set(handling) - forbidden} not forbidden"

    def test_versions_are_semver(self):
        for c in CONTRACTS.values():
            parts = c.version.split(".")
            assert len(parts) == 3 and all(p.isdigit() for p in parts)


# ─── Type mapping / mode ──────────────────────────────────────────────────────
class TestTypeFamily:
    @pytest.mark.parametrize(
        "spark_type,expected",
        [
            ("string", TypeFamily.STRING),
            ("int", TypeFamily.INTEGER),
            ("bigint", TypeFamily.INTEGER),
            ("double", TypeFamily.DOUBLE),
            ("decimal(18,2)", TypeFamily.DOUBLE),
            ("boolean", TypeFamily.BOOLEAN),
            ("timestamp", TypeFamily.TIMESTAMP),
            ("timestamp_ntz", TypeFamily.TIMESTAMP),
            ("date", TypeFamily.DATE),
            ("array<string>", None),
        ],
    )
    def test_mapping(self, spark_type, expected):
        assert type_family(spark_type) is expected


class TestMode:
    def test_default_is_warn(self, monkeypatch):
        monkeypatch.delenv("DATA_CONTRACT_MODE", raising=False)
        assert resolve_mode() == "warn"

    def test_env_and_explicit(self, monkeypatch):
        monkeypatch.setenv("DATA_CONTRACT_MODE", "ENFORCE")
        assert resolve_mode() == "enforce"
        assert resolve_mode("off") == "off"

    def test_invalid_mode_rejected(self):
        with pytest.raises(ValueError, match="DATA_CONTRACT_MODE"):
            resolve_mode("strict")


# ─── Schema checks (pure Python) ──────────────────────────────────────────────
class TestSchemaCheck:
    def test_conforming_schema_has_no_violations(self):
        c = CONTRACTS["orders"]
        assert check_schema(_dtypes(c), c) == []

    def test_extra_columns_are_allowed(self):
        c = CONTRACTS["orders"]
        assert check_schema([*_dtypes(c), ("order_value_band", "string")], c) == []

    def test_missing_column(self):
        c = CONTRACTS["orders"]
        dtypes = [d for d in _dtypes(c) if d[0] != "customer_id"]
        (v,) = check_schema(dtypes, c)
        assert (v.rule, v.column, v.severity) == ("missing_column", "customer_id", Severity.ERROR)

    def test_type_mismatch(self):
        c = CONTRACTS["orders"]
        dtypes = [(n, "string" if n == "total_amount" else t) for n, t in _dtypes(c)]
        (v,) = check_schema(dtypes, c)
        assert (v.rule, v.column) == ("type_mismatch", "total_amount")

    def test_forbidden_pii_column_flagged(self):
        c = CONTRACTS["customers"]
        violations = check_schema([*_dtypes(c), ("email", "string"), ("phone", "string")], c)
        assert {(v.rule, v.column) for v in violations} == {
            ("forbidden_column", "email"),
            ("forbidden_column", "phone"),
        }


# ─── Enforcer behaviour with a stubbed data check ─────────────────────────────
class _FakeDF:
    def __init__(self, dtypes):
        self.dtypes = dtypes
        self.columns = [n for n, _ in dtypes]


class TestEnforcer:
    def _patch_data(self, monkeypatch, violations=(), rows=10):
        monkeypatch.setattr(
            "src.quality.contracts.enforcer.check_data",
            lambda df, contract: (rows, list(violations)),
        )

    def test_off_skips_everything(self):
        assert ContractEnforcer(mode="off").check(_FakeDF([]), "orders") is None

    def test_no_contract_returns_none(self):
        assert ContractEnforcer(mode="warn").check(_FakeDF([]), "unknown") is None

    def test_clean_run_passes(self, monkeypatch):
        c = CONTRACTS["orders"]
        self._patch_data(monkeypatch)
        report = ContractEnforcer(mode="enforce").check(_FakeDF(_dtypes(c)), "orders")
        assert report.passed and report.rows_checked == 10

    def test_warn_mode_never_raises(self, monkeypatch):
        self._patch_data(monkeypatch)
        report = ContractEnforcer(mode="warn").check(_FakeDF([]), "orders")  # every column missing
        assert not report.passed and report.errors

    def test_enforce_mode_raises_on_errors(self, monkeypatch):
        self._patch_data(monkeypatch)
        with pytest.raises(ContractViolationError, match="orders@1.0.0"):
            ContractEnforcer(mode="enforce").check(_FakeDF([]), "orders")

    def test_enforce_mode_tolerates_warnings_only(self, monkeypatch):
        c = CONTRACTS["orders"]
        warn = ContractViolation(
            "orders", c.version, "below_minimum", Severity.WARNING, "x", "total_amount", 1
        )
        self._patch_data(monkeypatch, [warn])
        report = ContractEnforcer(mode="enforce").check(_FakeDF(_dtypes(c)), "orders")
        assert report.passed and len(report.warnings) == 1

    def test_persistence_failure_is_swallowed(self, monkeypatch):
        self._patch_data(monkeypatch)
        settings = MagicMock(postgres_dsn="host=nowhere")
        monkeypatch.setattr("psycopg2.connect", MagicMock(side_effect=RuntimeError("db down")))
        report = ContractEnforcer(settings=settings, mode="warn").check(_FakeDF([]), "orders")
        assert report is not None  # would have raised if _persist propagated

    def test_persist_writes_one_row_per_violation(self, monkeypatch):
        self._patch_data(monkeypatch)
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        monkeypatch.setattr("psycopg2.connect", MagicMock(return_value=conn))
        report = ContractEnforcer(settings=MagicMock(postgres_dsn="x"), mode="warn").check(
            _FakeDF([]), "orders", run_id="r1"
        )
        (sql, rows), _ = cur.executemany.call_args
        assert "quality.contract_violations" in sql
        assert len(rows) == len(report.violations) and rows[0][0] == "r1"
        conn.close.assert_called_once()


# ─── Data checks (need a real local SparkSession) ─────────────────────────────
# DataFrames are built with SQL VALUES so no Python worker is needed (the PySpark
# Python daemon does not run on Windows; the JVM-only path works everywhere).
@pytest.mark.spark
class TestDataChecks:
    _COLS = "order_id string, customer_id string, status string, total_amount double"

    def _df(self, spark, values_sql: str):
        cols = ", ".join(c.split()[0] for c in self._COLS.split(", "))
        casts = ", ".join(
            f"CAST({c.split()[0]} AS {c.split()[1].upper()}) AS {c.split()[0]}"
            for c in self._COLS.split(", ")
        )
        return spark.sql(f"SELECT {casts} FROM VALUES {values_sql} AS t({cols})")

    def _mini_contract(self):
        return DataContract(
            "orders",
            "9.9.9",
            "test",
            "order_id",
            (
                ColumnContract("order_id", TypeFamily.STRING, nullable=False),
                ColumnContract("customer_id", TypeFamily.STRING, nullable=False),
                ColumnContract(
                    "status", TypeFamily.STRING, allowed_values=("confirmed", "shipped")
                ),
                ColumnContract("total_amount", TypeFamily.DOUBLE, min_value=0.0, max_value=1000.0),
            ),
        )

    def test_clean_data_has_no_violations(self, local_spark):
        from src.quality.contracts.enforcer import check_data

        df = self._df(local_spark, "('o1','c1','confirmed',10.0), ('o2','c2','shipped',20.0)")
        rows, violations = check_data(df, self._mini_contract())
        assert rows == 2 and violations == []

    def test_all_rule_types_detected(self, local_spark):
        from src.quality.contracts.enforcer import check_data

        df = self._df(
            local_spark,
            "('o1','c1','confirmed',10.0),"
            " ('o1',NULL,'bogus',-5.0),"  # duplicate pk, null customer, bad enum, below min
            " ('o3','c3','shipped',5000.0)",  # above max
        )
        _, violations = check_data(df, self._mini_contract())
        found = {(v.rule, v.column): v.offending_rows for v in violations}
        assert found[("null_in_required", "customer_id")] == 1
        assert found[("invalid_value", "status")] == 1
        assert found[("below_minimum", "total_amount")] == 1
        assert found[("above_maximum", "total_amount")] == 1
        assert found[("duplicate_primary_key", "order_id")] == 1
        severities = {v.rule: v.severity for v in violations}
        assert severities["below_minimum"] is Severity.WARNING
        assert severities["duplicate_primary_key"] is Severity.ERROR

    def test_null_pk_is_reported_once_not_as_duplicate(self, local_spark):
        from src.quality.contracts.enforcer import check_data

        df = self._df(
            local_spark,
            "(NULL,'c1','confirmed',1.0), (NULL,'c2','shipped',2.0), ('o3','c3','shipped',3.0)",
        )
        _, violations = check_data(df, self._mini_contract())
        rules = {(v.rule, v.column) for v in violations}
        assert ("null_in_required", "order_id") in rules
        assert ("duplicate_primary_key", "order_id") not in rules

    def test_empty_dataset_violates_min_rows(self, local_spark):
        from src.quality.contracts.enforcer import check_data

        df = self._df(local_spark, "('o1','c1','confirmed',1.0)").where("1 = 0")
        rows, violations = check_data(df, self._mini_contract())
        assert rows == 0 and any(v.rule == "min_rows" for v in violations)

    def test_enforcer_end_to_end_on_real_dataframe(self, local_spark):
        df = self._df(local_spark, "('o1','c1','confirmed',10.0)")
        # The real 'orders' contract needs many more columns -> schema errors in enforce mode.
        with pytest.raises(ContractViolationError):
            ContractEnforcer(mode="enforce").check(df, "orders")
        report = ContractEnforcer(mode="warn").check(df, "orders")
        assert {v.rule for v in report.errors} >= {"missing_column"}
