"""Freshness check — most-recent record must be within the SLA window."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from .base_check import BaseQualityCheck, CheckResult, CheckStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FreshnessCheck(BaseQualityCheck):
    """Verify that max(*timestamp_col*) is no older than *max_age_hours*."""

    name = "freshness"

    def __init__(self, timestamp_col: str, max_age_hours: float = 26.0) -> None:
        self.timestamp_col = timestamp_col
        self.max_age_hours = max_age_hours

    def run(self, df: DataFrame, domain: str, layer: str, **kwargs: Any) -> list[CheckResult]:
        total = df.count()

        if total == 0:
            return [
                CheckResult(
                    domain=domain,
                    layer=layer,
                    check_name=self.name,
                    column_name=self.timestamp_col,
                    status=CheckStatus.FAILED,
                    rows_checked=0,
                    message="Table is empty — freshness SLA cannot be validated",
                )
            ]

        if self.timestamp_col not in df.columns:
            return [
                CheckResult(
                    domain=domain,
                    layer=layer,
                    check_name=self.name,
                    column_name=self.timestamp_col,
                    status=CheckStatus.ERROR,
                    rows_checked=total,
                    message=f"Timestamp column '{self.timestamp_col}' not found",
                )
            ]

        # Bronze keeps timestamps as ISO strings; cast so the same check works on every layer
        max_ts = df.select(F.max(F.col(self.timestamp_col).cast("timestamp")).alias("m")).collect()[
            0
        ]["m"]
        if max_ts is None:
            return [
                CheckResult(
                    domain=domain,
                    layer=layer,
                    check_name=self.name,
                    column_name=self.timestamp_col,
                    status=CheckStatus.FAILED,
                    rows_checked=total,
                    message=f"All values in '{self.timestamp_col}' are null",
                )
            ]

        # Normalise to UTC-aware datetime
        if hasattr(max_ts, "tzinfo") and max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=UTC)

        age_hours = (datetime.now(UTC) - max_ts).total_seconds() / 3600
        passed = age_hours <= self.max_age_hours
        return [
            CheckResult(
                domain=domain,
                layer=layer,
                check_name=self.name,
                column_name=self.timestamp_col,
                status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                rows_checked=total,
                metric_value=round(age_hours, 2),
                threshold=self.max_age_hours,
                message=(
                    f"Data is {age_hours:.1f}h old — SLA={self.max_age_hours}h EXCEEDED"
                    if not passed
                    else f"Data is {age_hours:.1f}h old — within SLA of {self.max_age_hours}h"
                ),
            )
        ]
