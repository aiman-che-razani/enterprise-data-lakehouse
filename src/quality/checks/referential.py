"""Referential-integrity check — FK values must exist in parent table."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from .base_check import BaseQualityCheck, CheckResult, CheckStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class ReferentialCheck(BaseQualityCheck):
    """Count rows where *fk_col* is not null but has no matching *parent_pk*
    in the Delta table at *parent_path*.

    Any non-zero orphan count is a FAILED result.
    Errors loading the parent table surface as ERROR status (not an exception).
    """

    name = "referential"

    def __init__(
        self,
        fk_col: str,
        parent_path: str,
        parent_pk: str,
        spark: SparkSession,
    ) -> None:
        self.fk_col = fk_col
        self.parent_path = parent_path
        self.parent_pk = parent_pk
        self.spark = spark

    def run(self, df: DataFrame, domain: str, layer: str, **kwargs: Any) -> list[CheckResult]:
        total = df.count()
        try:
            parent = self.spark.read.format("delta").load(self.parent_path)
            child_keys = df.filter(F.col(self.fk_col).isNotNull()).select(
                F.col(self.fk_col).alias("__fk")
            )
            parent_keys = parent.select(F.col(self.parent_pk).alias("__fk")).distinct()
            # anti-join, not exceptAll: exceptAll subtracts multisets, so N child rows sharing one valid
            # key would report N-1 "orphans" against a single parent row
            orphans = child_keys.join(parent_keys, "__fk", "left_anti").count()
        except Exception as exc:
            return [
                CheckResult(
                    domain=domain,
                    layer=layer,
                    check_name=self.name,
                    column_name=self.fk_col,
                    status=CheckStatus.ERROR,
                    rows_checked=total,
                    message=f"Referential check could not execute: {exc}",
                )
            ]

        return [
            CheckResult(
                domain=domain,
                layer=layer,
                check_name=self.name,
                column_name=self.fk_col,
                status=CheckStatus.FAILED if orphans > 0 else CheckStatus.PASSED,
                rows_checked=total,
                metric_value=float(orphans),
                threshold=0.0,
                message=(
                    f"{orphans:,} orphaned '{self.fk_col}' values " f"(not in {self.parent_path})"
                    if orphans > 0
                    else f"No orphaned FK values in '{self.fk_col}'"
                ),
            )
        ]
