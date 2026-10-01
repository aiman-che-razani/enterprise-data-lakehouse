"""Uniqueness check — zero duplicates on a primary-key column."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base_check import BaseQualityCheck, CheckResult, CheckStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class UniquenessCheck(BaseQualityCheck):
    """Assert that *pk_col* has no duplicate non-null values."""

    name = "uniqueness"

    def __init__(self, pk_col: str) -> None:
        self.pk_col = pk_col

    def run(self, df: DataFrame, domain: str, layer: str, **kwargs: Any) -> list[CheckResult]:
        if self.pk_col not in df.columns:
            return [
                CheckResult(
                    domain=domain,
                    layer=layer,
                    check_name=self.name,
                    column_name=self.pk_col,
                    status=CheckStatus.ERROR,
                    rows_checked=0,
                    message=f"PK column '{self.pk_col}' not in DataFrame",
                )
            ]

        total = df.count()
        distinct = df.select(self.pk_col).distinct().count()
        dups = total - distinct
        return [
            CheckResult(
                domain=domain,
                layer=layer,
                check_name=self.name,
                column_name=self.pk_col,
                status=CheckStatus.FAILED if dups > 0 else CheckStatus.PASSED,
                rows_checked=total,
                metric_value=float(dups),
                threshold=0.0,
                message=(
                    f"{dups:,} duplicate values in PK '{self.pk_col}'"
                    if dups > 0
                    else f"No duplicates in PK '{self.pk_col}'"
                ),
            )
        ]
