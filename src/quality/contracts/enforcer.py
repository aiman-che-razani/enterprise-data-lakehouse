"""
Data contract enforcement for Silver runs.

Two layers of checks:

* **Schema checks** (``check_schema``) — pure Python over ``df.dtypes``; free.
* **Data checks** (``check_data``) — a *single* Spark aggregation computing
  null / enum / range / duplicate-key counts for every rule at once.

``ContractEnforcer.check`` runs both, logs and persists every violation, and
— depending on ``DATA_CONTRACT_MODE`` — raises ``ContractViolationError``:

    off      skip contract validation entirely
    warn     (default) log + persist violations, never fail the run
    enforce  fail the run on any ERROR-severity violation
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from src.common.logging import get_logger

from .definitions import get_contract
from .models import (
    ContractReport,
    ContractViolation,
    ContractViolationError,
    DataContract,
    Severity,
    TypeFamily,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger(__name__)

MODE_ENV = "DATA_CONTRACT_MODE"
VALID_MODES = ("off", "warn", "enforce")


def resolve_mode(mode: str | None = None) -> str:
    """Return the effective enforcement mode, validating the value."""
    value = (mode or os.environ.get(MODE_ENV) or "warn").strip().lower()
    if value not in VALID_MODES:
        raise ValueError(f"{MODE_ENV} must be one of {VALID_MODES}, got {value!r}")
    return value


def type_family(spark_type: str) -> TypeFamily | None:
    """Map a Spark ``dtype`` string (e.g. ``decimal(10,2)``) to a contract type family."""
    t = spark_type.lower()
    if t == "string":
        return TypeFamily.STRING
    if t in ("tinyint", "smallint", "int", "bigint", "long", "integer", "short", "byte"):
        return TypeFamily.INTEGER
    if t in ("double", "float") or t.startswith("decimal"):
        return TypeFamily.DOUBLE
    if t == "boolean":
        return TypeFamily.BOOLEAN
    if t.startswith("timestamp"):
        return TypeFamily.TIMESTAMP
    if t == "date":
        return TypeFamily.DATE
    return None


# ─── Schema-level checks (no Spark action) ────────────────────────────────────
def check_schema(dtypes: list[tuple[str, str]], contract: DataContract) -> list[ContractViolation]:
    """Validate column presence, type family, and forbidden columns."""
    actual = {name: type_family(t) for name, t in dtypes}
    raw = dict(dtypes)
    out: list[ContractViolation] = []
    v, d = contract.version, contract.domain

    for col in contract.columns:
        if col.name not in actual:
            out.append(
                ContractViolation(
                    d,
                    v,
                    "missing_column",
                    Severity.ERROR,
                    f"Required column '{col.name}' is absent",
                    col.name,
                )
            )
        elif actual[col.name] is not col.type:
            out.append(
                ContractViolation(
                    d,
                    v,
                    "type_mismatch",
                    Severity.ERROR,
                    f"'{col.name}' is {raw[col.name]} ({actual[col.name]}), contract requires {col.type.value}",
                    col.name,
                )
            )

    for name in contract.forbidden_columns:
        if name in actual:
            out.append(
                ContractViolation(
                    d,
                    v,
                    "forbidden_column",
                    Severity.ERROR,
                    f"Forbidden column '{name}' is present (PII / dropped field)",
                    name,
                )
            )
    return out


# ─── Data-level checks (one Spark aggregation) ────────────────────────────────
def check_data(df: DataFrame, contract: DataContract) -> tuple[int, list[ContractViolation]]:
    """Return ``(row_count, violations)`` computed in a single pass over *df*."""
    from pyspark.sql import functions as F

    present = set(df.columns)
    exprs: list[Any] = [F.count(F.lit(1)).alias("_rows")]
    # alias -> (rule, column, severity, message template)
    meta: dict[str, tuple[str, str, Severity, str]] = {}

    def add(alias: str, expr: Any, rule: str, col: str, sev: Severity, msg: str) -> None:
        exprs.append(F.sum(F.when(expr, 1).otherwise(0)).alias(alias))
        meta[alias] = (rule, col, sev, msg)

    for i, c in enumerate(contract.columns):
        if c.name not in present:
            continue
        col = F.col(f"`{c.name}`")
        if not c.nullable:
            add(
                f"n{i}",
                col.isNull(),
                "null_in_required",
                c.name,
                Severity.ERROR,
                f"{{n}} null value(s) in non-nullable column '{c.name}'",
            )
        if c.allowed_values is not None:
            add(
                f"e{i}",
                col.isNotNull() & ~col.isin(*c.allowed_values),
                "invalid_value",
                c.name,
                Severity.ERROR,
                f"{{n}} value(s) of '{c.name}' outside {list(c.allowed_values)}",
            )
        if c.min_value is not None:
            add(
                f"lo{i}",
                col < c.min_value,
                "below_minimum",
                c.name,
                Severity.WARNING,
                f"{{n}} value(s) of '{c.name}' below {c.min_value}",
            )
        if c.max_value is not None:
            add(
                f"hi{i}",
                col > c.max_value,
                "above_maximum",
                c.name,
                Severity.WARNING,
                f"{{n}} value(s) of '{c.name}' above {c.max_value}",
            )

    pk_present = contract.primary_key in present
    if pk_present:
        exprs.append(F.countDistinct(F.col(f"`{contract.primary_key}`")).alias("_pk_distinct"))
        exprs.append(
            F.sum(F.when(F.col(f"`{contract.primary_key}`").isNull(), 1).otherwise(0)).alias(
                "_pk_nulls"
            )
        )

    row = df.agg(*exprs).first()
    assert row is not None  # a global aggregate always yields exactly one row
    rows = int(row["_rows"] or 0)
    violations: list[ContractViolation] = []
    d, v = contract.domain, contract.version

    if rows < contract.min_rows:
        violations.append(
            ContractViolation(
                d,
                v,
                "min_rows",
                Severity.ERROR,
                f"Dataset has {rows} row(s); contract requires at least {contract.min_rows}",
                None,
                rows,
            )
        )

    for alias, (rule, column_name, sev, msg) in meta.items():
        n = int(row[alias] or 0)
        if n > 0:
            violations.append(ContractViolation(d, v, rule, sev, msg.format(n=n), column_name, n))

    if pk_present:
        # Distinct count ignores NULLs, so duplicates = non-null rows - distinct keys.
        non_null = rows - int(row["_pk_nulls"] or 0)
        dupes = non_null - int(row["_pk_distinct"] or 0)
        if dupes > 0:
            violations.append(
                ContractViolation(
                    d,
                    v,
                    "duplicate_primary_key",
                    Severity.ERROR,
                    f"{dupes} duplicate value(s) of primary key '{contract.primary_key}'",
                    contract.primary_key,
                    dupes,
                )
            )
    return rows, violations


class ContractEnforcer:
    """Validate a DataFrame against its domain contract and report the outcome."""

    def __init__(self, settings: Any | None = None, mode: str | None = None) -> None:
        self.settings = settings
        # Precedence: explicit argument > Settings (env-aware default) > DATA_CONTRACT_MODE env > warn
        from_settings = getattr(settings, "contract_mode", None)
        self.mode = resolve_mode(
            mode or (from_settings if isinstance(from_settings, str) else None)
        )

    def check(self, df: DataFrame, domain: str, run_id: str | None = None) -> ContractReport | None:
        """Validate *df*; returns None when disabled or no contract exists for *domain*."""
        if self.mode == "off":
            return None
        contract = get_contract(domain)
        if contract is None:
            logger.info("no_contract_for_domain", domain=domain)
            return None

        violations = check_schema(df.dtypes, contract)
        rows, data_violations = check_data(df, contract)
        violations.extend(data_violations)
        report = ContractReport(domain, contract.version, rows, violations)

        for viol in violations:
            log = logger.error if viol.severity is Severity.ERROR else logger.warning
            log("contract_violation", **viol.as_log_fields())
        logger.info(
            "contract_checked",
            domain=domain,
            version=contract.version,
            rows=rows,
            errors=len(report.errors),
            warnings=len(report.warnings),
            mode=self.mode,
        )

        self._persist(report, run_id)
        if self.mode == "enforce" and not report.passed:
            raise ContractViolationError(report)
        return report

    # ------------------------------------------------------------------
    def _persist(self, report: ContractReport, run_id: str | None) -> None:
        """Best-effort write to quality.contract_violations (never fails the run)."""
        if not report.violations or self.settings is None:
            return
        try:
            import psycopg2

            conn = psycopg2.connect(self.settings.postgres_dsn)
            try:
                with conn, conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO quality.contract_violations
                            (run_id, domain, contract_version, rule, column_name, severity, message)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                run_id,
                                v.domain,
                                v.contract_version,
                                v.rule,
                                v.column,
                                v.severity.value,
                                v.message,
                            )
                            for v in report.violations
                        ],
                    )
            finally:
                conn.close()
        except Exception as exc:  # — persistence must not mask the real outcome
            logger.warning("contract_persist_failed", domain=report.domain, error=str(exc))
