"""
Base classes for the RetailNexus Data Quality framework.

Every check returns a list[CheckResult].  An empty list means the check
could not be applied (e.g. table was empty and no assertion was possible).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class CheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class CheckResult:
    domain: str
    layer: str  # bronze | silver | gold
    check_name: str
    status: CheckStatus
    rows_checked: int
    message: str
    column_name: str | None = None
    metric_value: float | None = None
    threshold: float | None = None
    run_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_ok(self) -> bool:
        return self.status in (CheckStatus.PASSED, CheckStatus.WARNING)


class BaseQualityCheck(ABC):
    """Abstract quality check.  Concrete subclasses implement ``run()``."""

    name: str = ""

    @abstractmethod
    def run(
        self,
        df: DataFrame,
        domain: str,
        layer: str,
        **kwargs: Any,
    ) -> list[CheckResult]:
        """Execute the check and return zero or more CheckResult objects."""
        ...
