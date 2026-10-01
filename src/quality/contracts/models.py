"""
Data contract models.

A *data contract* is the producer's published promise about a dataset: which
columns exist, their type families, nullability, allowed values and ranges,
and which sensitive columns must never appear. Contracts here describe the
**Silver** output of each domain and are enforced on every Silver run
(see ``enforcer.py``).

The models are plain dataclasses — no Spark import — so contracts can be
loaded, diffed and unit-tested anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    ERROR = "error"  # breaks the contract — blocks the run in enforce mode
    WARNING = "warning"  # suspicious but tolerated


class TypeFamily(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"


@dataclass(frozen=True)
class ColumnContract:
    name: str
    type: TypeFamily
    nullable: bool = True
    allowed_values: tuple[str, ...] | None = None
    min_value: float | None = None
    max_value: float | None = None

    def __post_init__(self) -> None:
        if self.allowed_values is not None and self.type is not TypeFamily.STRING:
            raise ValueError(f"{self.name}: allowed_values only applies to string columns")
        if (self.min_value is not None or self.max_value is not None) and self.type not in (
            TypeFamily.INTEGER,
            TypeFamily.DOUBLE,
        ):
            raise ValueError(f"{self.name}: min/max only apply to numeric columns")
        if (
            self.min_value is not None
            and self.max_value is not None
            and self.min_value > self.max_value
        ):
            raise ValueError(f"{self.name}: min_value > max_value")


@dataclass(frozen=True)
class DataContract:
    domain: str
    version: str
    owner: str
    primary_key: str
    columns: tuple[ColumnContract, ...]
    forbidden_columns: tuple[str, ...] = ()
    min_rows: int = 1
    description: str = ""

    def __post_init__(self) -> None:
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError(f"{self.domain}: duplicate column in contract")
        if self.primary_key not in names:
            raise ValueError(f"{self.domain}: primary key {self.primary_key!r} not in columns")
        clash = set(names) & set(self.forbidden_columns)
        if clash:
            raise ValueError(f"{self.domain}: columns both required and forbidden: {sorted(clash)}")

    def column(self, name: str) -> ColumnContract | None:
        return next((c for c in self.columns if c.name == name), None)


@dataclass(frozen=True)
class ContractViolation:
    domain: str
    contract_version: str
    rule: str
    severity: Severity
    message: str
    column: str | None = None
    offending_rows: int | None = None

    def as_log_fields(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "contract_version": self.contract_version,
            "rule": self.rule,
            "column": self.column,
            "severity": self.severity.value,
            "offending_rows": self.offending_rows,
            "message": self.message,
        }


@dataclass
class ContractReport:
    domain: str
    contract_version: str
    rows_checked: int
    violations: list[ContractViolation] = field(default_factory=list)

    @property
    def errors(self) -> list[ContractViolation]:
        return [v for v in self.violations if v.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[ContractViolation]:
        return [v for v in self.violations if v.severity is Severity.WARNING]

    @property
    def passed(self) -> bool:
        return not self.errors


class ContractViolationError(RuntimeError):
    """Raised in ``enforce`` mode when a dataset breaks its data contract."""

    def __init__(self, report: ContractReport) -> None:
        self.report = report
        rules = ", ".join(f"{v.rule}({v.column or '-'})" for v in report.errors[:5])
        super().__init__(
            f"Data contract {report.domain}@{report.contract_version} violated "
            f"({len(report.errors)} error(s)): {rules}"
        )
