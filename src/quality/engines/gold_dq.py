"""Gold DQ engine — dimension / fact completeness, SK integrity, value ranges,
cross-layer row-count consistency.

Gold tables live at *settings.gold_s3_path/<table_name>*.
All Gold dims are full-refresh (no is_current filter needed).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.common.logging import get_logger
from src.quality.checks.base_check import CheckResult, CheckStatus
from src.quality.checks.completeness import CompletenessCheck
from src.quality.checks.freshness import FreshnessCheck
from src.quality.checks.row_count import RowCountCheck
from src.quality.checks.uniqueness import UniquenessCheck
from src.quality.checks.value_range import ValueRangeCheck
from src.quality.domain_config import GOLD_CONFIGS, DomainCheckConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.common.config import Settings

logger = get_logger(__name__)


class GoldDQEngine:
    LAYER = "gold"

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
    def run_table(self, table_name: str) -> list[CheckResult]:
        path = f"{self.settings.gold_s3_path}/{table_name}"
        logger.info("gold_dq_start", table=table_name, path=path)

        try:
            df = self.spark.read.format("delta").load(path)
        except Exception as exc:
            logger.error("gold_dq_table_load_failed", table=table_name, error=str(exc))
            return [
                CheckResult(
                    domain=table_name,
                    layer=self.LAYER,
                    check_name="table.load",
                    status=CheckStatus.ERROR,
                    rows_checked=0,
                    message=f"Cannot load Gold Delta table: {exc}",
                )
            ]

        config: DomainCheckConfig = GOLD_CONFIGS.get(table_name, DomainCheckConfig())
        results: list[CheckResult] = []

        results.extend(
            RowCountCheck(min_count=1, history_conn=self.history_conn).run(
                df, table_name, self.LAYER
            )
        )

        for columns, max_null_rate in config.completeness_plan(optional_threshold=0.1):
            results.extend(
                CompletenessCheck(columns, threshold=max_null_rate).run(df, table_name, self.LAYER)
            )

        if config.timestamp_col:
            results.extend(
                FreshnessCheck(config.timestamp_col, config.sla_hours).run(
                    df, table_name, self.LAYER
                )
            )

        if config.pk_col:
            results.extend(UniquenessCheck(config.pk_col).run(df, table_name, self.LAYER))

        if config.numeric_ranges:
            results.extend(ValueRangeCheck(config.numeric_ranges).run(df, table_name, self.LAYER))

        # SK null-rate check for fact tables (FKs may legitimately be null, but
        # date and first-class dimension SKs should never be null)
        if table_name.startswith("fact_") and "sk_date_id" in df.columns:
            results.extend(
                CompletenessCheck(["sk_date_id"], threshold=0.0).run(df, table_name, self.LAYER)
            )

        _log_summary(table_name, self.LAYER, results)
        return results

    def run_all(self, tables: list[str] | None = None) -> list[CheckResult]:
        targets = tables or list(GOLD_CONFIGS)
        all_results: list[CheckResult] = []
        for t in targets:
            all_results.extend(self.run_table(t))
        return all_results


# ------------------------------------------------------------------
def _log_summary(domain: str, layer: str, results: list[CheckResult]) -> None:
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
