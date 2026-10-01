"""Regression tests for SilverValidator: lazy rule construction and NULL handling."""

from __future__ import annotations

import importlib
import sys

import pytest


class TestImportWithoutSpark:
    def test_validator_imports_without_an_active_spark_context(self):
        """Building F.col() rules at import time used to need a live SparkContext,
        which broke importing ``src.processing.silver`` (and every DAG/test that does)."""
        for name in [m for m in sys.modules if m.startswith("src.processing.silver")]:
            del sys.modules[name]
        module = importlib.import_module("src.processing.silver.validator")
        assert not hasattr(module, "_RULES"), "rules must be built lazily via _build_rules()"

    def test_processors_package_imports(self):
        importlib.import_module("src.processing.silver")


# DataFrames are built with SQL VALUES (JVM-only) so no Python worker is required.
@pytest.mark.spark
class TestNullPredicates:
    def _orders(self, spark, values: str):
        cols = "order_id, customer_id, status, total_amount, subtotal"
        return spark.sql(
            "SELECT CAST(order_id AS STRING) order_id, CAST(customer_id AS STRING) customer_id, "
            "CAST(status AS STRING) status, CAST(total_amount AS DOUBLE) total_amount, "
            f"CAST(subtotal AS DOUBLE) subtotal FROM VALUES {values} AS t({cols})"
        )

    def test_null_amount_goes_to_dlq_instead_of_vanishing(self, local_spark):
        from src.processing.silver.validator import SilverValidator

        df = self._orders(
            local_spark,
            "('o1','c1','confirmed',10.0,10.0), ('o2','c2','confirmed',NULL,5.0), ('o3','c3','bogus',7.0,7.0)",
        )
        valid, invalid = SilverValidator().validate(df, "orders")
        valid_ids = {r.order_id for r in valid.collect()}
        invalid_ids = {r.order_id for r in invalid.collect()}

        assert valid_ids == {"o1"}
        assert invalid_ids == {"o2", "o3"}  # o2 (NULL total) used to match neither output
        assert valid.count() + invalid.count() == df.count()

    def test_failed_rules_named(self, local_spark):
        from src.processing.silver.validator import SilverValidator

        df = self._orders(local_spark, "('o2','c2','confirmed',NULL,5.0)")
        _, invalid = SilverValidator().validate(df, "orders")
        assert "total_positive" in invalid.first()["_failed_rules"]

    def test_all_valid_returns_no_dlq(self, local_spark):
        from src.processing.silver.validator import SilverValidator

        df = self._orders(local_spark, "('o1','c1','shipped',10.0,9.0)")
        valid, invalid = SilverValidator().validate(df, "orders")
        assert invalid is None and valid.count() == 1

    def test_domain_without_rules_passes_through(self, local_spark):
        from src.processing.silver.validator import SilverValidator

        df = local_spark.sql("SELECT 1 AS x")
        valid, invalid = SilverValidator().validate(df, "no_such_domain")
        assert invalid is None and valid.count() == 1
