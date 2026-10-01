"""Schema-drift detection — missing / extra / type-changed columns."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base_check import BaseQualityCheck, CheckResult, CheckStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class SchemaCheck(BaseQualityCheck):
    """Compare the live schema against an expected StructType or a list of
    required column names.  Type mismatches and missing required columns are
    FAILED; unexpected extra columns are WARNING only."""

    name = "schema"

    def __init__(
        self,
        expected: StructType | None = None,
        required_columns: list[str] | None = None,
    ) -> None:
        self.expected = expected
        self.required_columns: list[str] = required_columns or []

    # ------------------------------------------------------------------
    def run(self, df: DataFrame, domain: str, layer: str, **kwargs: Any) -> list[CheckResult]:
        results: list[CheckResult] = []
        actual = {f.name: f.dataType for f in df.schema.fields}
        total = df.count()

        # 1. Required columns
        for col in self.required_columns:
            if col not in actual:
                results.append(
                    CheckResult(
                        domain=domain,
                        layer=layer,
                        check_name=f"{self.name}.required_column",
                        column_name=col,
                        status=CheckStatus.FAILED,
                        rows_checked=total,
                        message=f"Required column '{col}' missing from schema",
                    )
                )

        # 2. Full StructType comparison
        if self.expected:
            exp = {f.name: f.dataType for f in self.expected.fields}
            for col in exp.keys() - actual.keys():
                results.append(
                    CheckResult(
                        domain=domain,
                        layer=layer,
                        check_name=f"{self.name}.missing_column",
                        column_name=col,
                        status=CheckStatus.FAILED,
                        rows_checked=total,
                        message=f"Expected column '{col}' not found in DataFrame",
                    )
                )
            for col in actual.keys() - exp.keys():
                results.append(
                    CheckResult(
                        domain=domain,
                        layer=layer,
                        check_name=f"{self.name}.extra_column",
                        column_name=col,
                        status=CheckStatus.WARNING,
                        rows_checked=total,
                        message=f"Unexpected column '{col}' present — schema drift?",
                    )
                )
            for col, exp_type in exp.items():
                if col in actual and actual[col] != exp_type:
                    results.append(
                        CheckResult(
                            domain=domain,
                            layer=layer,
                            check_name=f"{self.name}.type_mismatch",
                            column_name=col,
                            status=CheckStatus.FAILED,
                            rows_checked=total,
                            message=(f"Column '{col}': expected {exp_type}, " f"got {actual[col]}"),
                        )
                    )

        if not results:
            results.append(
                CheckResult(
                    domain=domain,
                    layer=layer,
                    check_name=self.name,
                    status=CheckStatus.PASSED,
                    rows_checked=total,
                    message="Schema matches expected structure",
                )
            )
        return results
