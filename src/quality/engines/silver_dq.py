"""Silver DQ engine — extends Bronze checks with FK integrity and business-rule
value ranges.  Reads Silver Delta tables (is_current filtering for SCD2 dims).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.common.logging import get_logger
from src.quality.checks.base_check import CheckResult, CheckStatus
from src.quality.checks.completeness import CompletenessCheck
from src.quality.checks.freshness import FreshnessCheck
from src.quality.checks.referential import ReferentialCheck
from src.quality.checks.row_count import RowCountCheck
from src.quality.checks.uniqueness import UniquenessCheck
from src.quality.checks.value_range import ValueRangeCheck
from src.quality.domain_config import SILVER_CONFIGS, DomainCheckConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.common.config import Settings

logger = get_logger(__name__)

_SCD2_DOMAINS = frozenset({"customers", "products", "suppliers", "stores", "employees"})


class SilverDQEngine:
    LAYER = "silver"

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
        path = f"{self.settings.silver_s3_path}/{domain}"
        logger.info("silver_dq_start", domain=domain, path=path)

        try:
            df_full = self.spark.read.format("delta").load(path)
            # SCD2 dims: check the current snapshot only
            if domain in _SCD2_DOMAINS and "is_current" in df_full.columns:
                df = df_full.filter("is_current = true")
            else:
                df = df_full
        except Exception as exc:
            logger.error("silver_dq_table_load_failed", domain=domain, error=str(exc))
            return [
                CheckResult(
                    domain=domain,
                    layer=self.LAYER,
                    check_name="table.load",
                    status=CheckStatus.ERROR,
                    rows_checked=0,
                    message=f"Cannot load Silver Delta table: {exc}",
                )
            ]

        config: DomainCheckConfig = SILVER_CONFIGS.get(domain, DomainCheckConfig())
        results: list[CheckResult] = []

        results.extend(
            RowCountCheck(min_count=1, history_conn=self.history_conn).run(df, domain, self.LAYER)
        )

        for columns, max_null_rate in config.completeness_plan(optional_threshold=0.05):
            results.extend(
                CompletenessCheck(columns, threshold=max_null_rate).run(df, domain, self.LAYER)
            )

        if config.timestamp_col:
            results.extend(
                FreshnessCheck(config.timestamp_col, config.sla_hours).run(df, domain, self.LAYER)
            )

        if config.pk_col:
            results.extend(UniquenessCheck(config.pk_col).run(df, domain, self.LAYER))

        if config.numeric_ranges:
            results.extend(ValueRangeCheck(config.numeric_ranges).run(df, domain, self.LAYER))

        # FK integrity — only attempt if parent Silver tables likely exist
        for fk_col, parent_domain, parent_pk in config.fk_checks:
            parent_path = f"{self.settings.silver_s3_path}/{parent_domain}"
            results.extend(
                ReferentialCheck(fk_col, parent_path, parent_pk, self.spark).run(
                    df, domain, self.LAYER
                )
            )

        _log_summary(domain, self.LAYER, results)
        return results

    def run_all(self, domains: list[str] | None = None) -> list[CheckResult]:
        targets = domains or list(SILVER_CONFIGS)
        all_results: list[CheckResult] = []
        for d in targets:
            all_results.extend(self.run_domain(d))
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
