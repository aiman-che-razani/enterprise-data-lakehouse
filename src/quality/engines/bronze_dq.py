"""Bronze DQ engine — schema, row count, completeness, freshness, uniqueness.

Runs against the raw Delta tables at *settings.bronze_s3_path/<domain>*.
No FK checks at this layer (raw data may have referential gaps).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.common.logging import get_logger
from src.quality.checks.base_check import CheckResult
from src.quality.checks.completeness import CompletenessCheck
from src.quality.checks.freshness import FreshnessCheck
from src.quality.checks.row_count import RowCountCheck
from src.quality.checks.uniqueness import UniquenessCheck
from src.quality.domain_config import BRONZE_CONFIGS, DomainCheckConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.common.config import Settings

logger = get_logger(__name__)


class BronzeDQEngine:
    """Runs quality checks on Bronze Delta tables.

    Parameters
    ----------
    spark:       active SparkSession
    settings:    common Settings (provides bronze_path)
    history_conn: optional psycopg2 connection for Z-score anomaly detection
    """

    LAYER = "bronze"

    def __init__(
        self,
        spark: SparkSession,
        settings: Settings,
        history_conn: Any = None,
    ) -> None:
        self.spark = spark
        self.settings = settings
        self.history_conn = history_conn

    # ------------------------------------------------------------------
    def run_domain(self, domain: str) -> list[CheckResult]:
        path = f"{self.settings.bronze_s3_path}/{domain}"
        logger.info("bronze_dq_start", domain=domain, path=path)

        try:
            df = self.spark.read.format("delta").load(path)
        except Exception as exc:
            logger.error("bronze_dq_table_load_failed", domain=domain, error=str(exc))
            return [
                CheckResult(
                    domain=domain,
                    layer=self.LAYER,
                    check_name="table.load",
                    status=__import__(
                        "src.quality.checks.base_check", fromlist=["CheckStatus"]
                    ).CheckStatus.ERROR,
                    rows_checked=0,
                    message=f"Cannot load Bronze Delta table: {exc}",
                )
            ]

        config: DomainCheckConfig = BRONZE_CONFIGS.get(domain, DomainCheckConfig())
        results: list[CheckResult] = []

        # Row count (always runs)
        results.extend(
            RowCountCheck(min_count=1, history_conn=self.history_conn).run(df, domain, self.LAYER)
        )

        for columns, max_null_rate in config.completeness_plan(optional_threshold=0.05):
            results.extend(
                CompletenessCheck(columns, threshold=max_null_rate).run(df, domain, self.LAYER)
            )

        # Freshness
        if config.timestamp_col:
            results.extend(
                FreshnessCheck(config.timestamp_col, config.sla_hours).run(df, domain, self.LAYER)
            )

        # Uniqueness
        if config.pk_col:
            results.extend(UniquenessCheck(config.pk_col).run(df, domain, self.LAYER))

        # Value ranges
        if config.numeric_ranges:
            from src.quality.checks.value_range import ValueRangeCheck

            results.extend(ValueRangeCheck(config.numeric_ranges).run(df, domain, self.LAYER))

        _log_summary(domain, self.LAYER, results)
        return results

    def run_all(self, domains: list[str] | None = None) -> list[CheckResult]:
        targets = domains or list(BRONZE_CONFIGS)
        all_results: list[CheckResult] = []
        for d in targets:
            all_results.extend(self.run_domain(d))
        return all_results


# ------------------------------------------------------------------
def _log_summary(domain: str, layer: str, results: list[CheckResult]) -> None:
    from src.quality.checks.base_check import CheckStatus

    counts = {s: 0 for s in CheckStatus}
    for r in results:
        counts[r.status] += 1
    logger.info(
        "dq_check_summary",
        domain=domain,
        layer=layer,
        passed=counts[CheckStatus.PASSED],
        failed=counts[CheckStatus.FAILED],
        warnings=counts[CheckStatus.WARNING],
        errors=counts[CheckStatus.ERROR],
    )
