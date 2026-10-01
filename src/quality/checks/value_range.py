"""Value-range check — numeric columns must fall within expected min/max bounds."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from .base_check import BaseQualityCheck, CheckResult, CheckStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ValueRangeCheck(BaseQualityCheck):
    """Assert that actual min/max of numeric columns respect configured bounds.

    *ranges* maps column name → (lower_bound, upper_bound).
    Either bound may be None to skip that side.
    Columns absent from the DataFrame are silently skipped.
    """

    name = "value_range"

    def __init__(
        self,
        ranges: dict[str, tuple[float | None, float | None]],
    ) -> None:
        self.ranges = ranges

    def run(self, df: DataFrame, domain: str, layer: str, **kwargs: Any) -> list[CheckResult]:
        total = df.count()
        present = [c for c in self.ranges if c in df.columns]
        if not present or total == 0:
            return []

        agg_exprs = []
        for c in present:
            agg_exprs += [F.min(c).alias(f"min_{c}"), F.max(c).alias(f"max_{c}")]
        stats = df.agg(*agg_exprs).collect()[0].asDict()

        results: list[CheckResult] = []
        for c in present:
            lo, hi = self.ranges[c]
            col_min = stats.get(f"min_{c}")
            col_max = stats.get(f"max_{c}")
            violations: list[str] = []
            if lo is not None and col_min is not None and float(col_min) < lo:
                violations.append(f"min={col_min} < {lo}")
            if hi is not None and col_max is not None and float(col_max) > hi:
                violations.append(f"max={col_max} > {hi}")

            results.append(
                CheckResult(
                    domain=domain,
                    layer=layer,
                    check_name=self.name,
                    column_name=c,
                    status=CheckStatus.FAILED if violations else CheckStatus.PASSED,
                    rows_checked=total,
                    metric_value=float(col_max) if col_max is not None else None,
                    threshold=hi,
                    message=(
                        f"Range violations in '{c}': {'; '.join(violations)}"
                        if violations
                        else f"'{c}' values within bounds [{lo}, {hi}]"
                    ),
                )
            )
        return results
