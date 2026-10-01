"""Row-count check — minimum threshold + Z-score anomaly detection.

Anomaly detection queries *quality.check_results* for the last 30 historical
counts for the same (domain, layer) pair and raises a WARNING if today's count
deviates more than *max_z_score* standard deviations from that mean.

Requires at least 7 historical data points before the Z-score test fires.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base_check import BaseQualityCheck, CheckResult, CheckStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class RowCountCheck(BaseQualityCheck):
    name = "row_count"

    def __init__(
        self,
        min_count: int = 1,
        max_z_score: float = 3.0,
        history_conn: Any = None,
    ) -> None:
        self.min_count = min_count
        self.max_z_score = max_z_score
        self.history_conn = history_conn  # psycopg2 connection | None

    # ------------------------------------------------------------------
    def run(self, df: DataFrame, domain: str, layer: str, **kwargs: Any) -> list[CheckResult]:
        count = df.count()
        results: list[CheckResult] = []

        # 1. Minimum count
        passed = count >= self.min_count
        results.append(
            CheckResult(
                domain=domain,
                layer=layer,
                check_name=f"{self.name}.minimum",
                status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                rows_checked=count,
                metric_value=float(count),
                threshold=float(self.min_count),
                message=(
                    f"Row count {count:,} below minimum {self.min_count:,}"
                    if not passed
                    else f"Row count {count:,} meets minimum"
                ),
            )
        )

        # 2. Z-score anomaly detection
        if self.history_conn is not None and passed:
            anomaly = self._zscore_check(count, domain, layer)
            if anomaly is not None:
                results.append(anomaly)

        return results

    # ------------------------------------------------------------------
    def _zscore_check(self, count: int, domain: str, layer: str) -> CheckResult | None:
        try:
            with self.history_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT AVG(metric_value), STDDEV(metric_value), COUNT(*)
                    FROM (
                        SELECT metric_value
                        FROM   quality.check_results
                        WHERE  domain     = %s
                          AND  layer      = %s
                          AND  check_name = 'row_count.minimum'
                          AND  status    != 'error'
                        ORDER  BY run_at DESC
                        LIMIT  30
                    ) recent
                    """,
                    (domain, layer),
                )
                row = cur.fetchone()
        except Exception:
            return None

        if row is None or row[2] < 7:
            return None

        avg, stddev, _ = float(row[0]), float(row[1] or 0), int(row[2])
        if stddev == 0:
            return None

        z = abs(count - avg) / stddev
        passed = z <= self.max_z_score
        return CheckResult(
            domain=domain,
            layer=layer,
            check_name=f"{self.name}.anomaly",
            status=CheckStatus.PASSED if passed else CheckStatus.WARNING,
            rows_checked=count,
            metric_value=round(z, 4),
            threshold=self.max_z_score,
            message=(
                f"Z-score {z:.2f} exceeds {self.max_z_score} "
                f"(count={count:,}, μ={avg:,.0f}, σ={stddev:,.0f}) — possible data anomaly"
                if not passed
                else f"Row count Z-score {z:.2f} within normal range"
            ),
        )
