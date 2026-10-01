"""Completeness check — null-rate per column vs. configurable threshold."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from .base_check import BaseQualityCheck, CheckResult, CheckStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class CompletenessCheck(BaseQualityCheck):
    """Verify that null rate for each column stays below *threshold*.

    Columns absent from the DataFrame are silently skipped so that
    Bronze / Silver / Gold can share the same config dict.
    """

    name = "completeness"

    def __init__(self, columns: list[str], threshold: float = 0.05) -> None:
        self.columns = columns
        self.threshold = threshold

    def run(self, df: DataFrame, domain: str, layer: str, **kwargs: Any) -> list[CheckResult]:
        total = df.count()
        if total == 0:
            return []

        present = [c for c in self.columns if c in df.columns]
        if not present:
            return []

        agg_row = (
            df.select([F.sum(F.col(c).isNull().cast("long")).alias(c) for c in present])
            .collect()[0]
            .asDict()
        )

        results: list[CheckResult] = []
        for col_name, null_count in agg_row.items():
            null_rate = (null_count or 0) / total
            passed = null_rate <= self.threshold
            results.append(
                CheckResult(
                    domain=domain,
                    layer=layer,
                    check_name=self.name,
                    column_name=col_name,
                    status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                    rows_checked=total,
                    metric_value=round(null_rate, 6),
                    threshold=self.threshold,
                    message=(
                        f"null_rate={null_rate:.2%} exceeds threshold={self.threshold:.2%}"
                        if not passed
                        else f"null_rate={null_rate:.2%} within threshold"
                    ),
                )
            )
        return results
