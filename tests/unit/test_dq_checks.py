"""
Unit tests for the DQ check framework (no Spark required).

Covers:
  - CheckResult / CheckStatus dataclass behaviour
  - AlertReporter.summary() counts
  - AlertReporter.report() raise-on-critical path
  - Domain config completeness (all 16 Bronze + 16 Silver + 14 Gold)
  - Domain config invariants (positive SLA, pk_col in required_columns)
"""

from __future__ import annotations

import pytest

from src.quality.checks.base_check import CheckResult, CheckStatus
from src.quality.domain_config import BRONZE_CONFIGS, GOLD_CONFIGS, SILVER_CONFIGS
from src.quality.reporters.alert_reporter import AlertReporter

_ALL_BRONZE_DOMAINS = {
    "customers",
    "products",
    "suppliers",
    "stores",
    "employees",
    "orders",
    "order_lines",
    "payments",
    "shipments",
    "returns",
    "inventory",
    "reviews",
    "clickstream",
    "iot_sensors",
    "finance",
    "campaigns",
}
_ALL_GOLD_TABLES = {
    "dim_date",
    "dim_location",
    "dim_supplier",
    "dim_store",
    "dim_employee",
    "dim_product",
    "dim_customer",
    "dim_promotion",
    "fact_sales",
    "fact_inventory",
    "fact_marketing",
    "fact_returns",
    "fact_shipments",
    "fact_finance",
}


# ─────────────────────────────────────────────────────────────────────────────
# CheckResult / CheckStatus
# ─────────────────────────────────────────────────────────────────────────────
class TestCheckResult:
    def _make(self, status: CheckStatus) -> CheckResult:
        return CheckResult(
            domain="orders",
            layer="bronze",
            check_name="completeness",
            status=status,
            rows_checked=1000,
            message="test",
        )

    def test_is_ok_true_for_passed(self):
        assert self._make(CheckStatus.PASSED).is_ok is True

    def test_is_ok_true_for_warning(self):
        assert self._make(CheckStatus.WARNING).is_ok is True

    def test_is_ok_false_for_failed(self):
        assert self._make(CheckStatus.FAILED).is_ok is False

    def test_is_ok_false_for_error(self):
        assert self._make(CheckStatus.ERROR).is_ok is False

    def test_default_run_at_is_utc(self):
        r = self._make(CheckStatus.PASSED)
        assert r.run_at.tzinfo is not None

    def test_status_string_values(self):
        assert CheckStatus.PASSED.value == "passed"
        assert CheckStatus.FAILED.value == "failed"
        assert CheckStatus.WARNING.value == "warning"
        assert CheckStatus.ERROR.value == "error"


# ─────────────────────────────────────────────────────────────────────────────
# AlertReporter
# ─────────────────────────────────────────────────────────────────────────────
class TestAlertReporter:
    def _results(self) -> list[CheckResult]:
        def r(status, check="completeness"):
            return CheckResult(
                domain="orders",
                layer="bronze",
                check_name=check,
                status=status,
                rows_checked=100,
                message="msg",
            )

        return [
            r(CheckStatus.PASSED),
            r(CheckStatus.PASSED),
            r(CheckStatus.WARNING),
            r(CheckStatus.FAILED),
            r(CheckStatus.ERROR),
        ]

    def test_summary_counts(self):
        summary = AlertReporter().summary(self._results())
        assert summary["passed"] == 2
        assert summary["failed"] == 1
        assert summary["warnings"] == 1
        assert summary["errors"] == 1
        assert summary["total"] == 5

    def test_summary_pass_rate(self):
        summary = AlertReporter().summary(self._results())
        assert summary["pass_rate"] == pytest.approx(0.4)

    def test_summary_empty(self):
        summary = AlertReporter().summary([])
        assert summary["total"] == 0
        assert summary["pass_rate"] == 0.0

    def test_report_no_raise_when_non_critical(self):
        r = CheckResult(
            domain="orders",
            layer="bronze",
            check_name="freshness",
            status=CheckStatus.FAILED,
            rows_checked=100,
            message="old data",
        )
        # freshness is not in _CRITICAL_CHECKS, so should NOT raise
        AlertReporter(fail_on_critical=True).report([r])

    def test_report_raises_on_critical_failure(self):
        r = CheckResult(
            domain="orders",
            layer="bronze",
            check_name="row_count.minimum",
            status=CheckStatus.FAILED,
            rows_checked=0,
            message="empty table",
        )
        with pytest.raises(RuntimeError, match="critical DQ check"):
            AlertReporter(fail_on_critical=True).report([r])

    def test_report_no_raise_when_fail_on_critical_false(self):
        r = CheckResult(
            domain="orders",
            layer="bronze",
            check_name="row_count.minimum",
            status=CheckStatus.FAILED,
            rows_checked=0,
            message="empty table",
        )
        AlertReporter(fail_on_critical=False).report([r])  # should not raise

    def test_report_does_not_raise_on_critical_warning(self):
        r = CheckResult(
            domain="orders",
            layer="bronze",
            check_name="row_count.minimum",
            status=CheckStatus.WARNING,
            rows_checked=10,
            message="near zero",
        )
        AlertReporter(fail_on_critical=True).report([r])  # WARNING ≠ FAILED


# ─────────────────────────────────────────────────────────────────────────────
# Domain config completeness
# ─────────────────────────────────────────────────────────────────────────────
class TestDomainConfigs:
    def test_bronze_has_all_16_domains(self):
        missing = _ALL_BRONZE_DOMAINS - set(BRONZE_CONFIGS)
        assert not missing, f"BRONZE_CONFIGS missing: {missing}"

    def test_silver_has_all_16_domains(self):
        missing = _ALL_BRONZE_DOMAINS - set(SILVER_CONFIGS)
        assert not missing, f"SILVER_CONFIGS missing: {missing}"

    def test_gold_has_all_14_tables(self):
        missing = _ALL_GOLD_TABLES - set(GOLD_CONFIGS)
        assert not missing, f"GOLD_CONFIGS missing: {missing}"

    def test_bronze_sla_hours_positive(self):
        for domain, cfg in BRONZE_CONFIGS.items():
            assert cfg.sla_hours > 0, f"{domain}: sla_hours must be > 0"

    def test_silver_sla_hours_positive(self):
        for domain, cfg in SILVER_CONFIGS.items():
            assert cfg.sla_hours > 0, f"{domain}: sla_hours must be > 0"

    def test_gold_sla_hours_positive(self):
        for table, cfg in GOLD_CONFIGS.items():
            assert cfg.sla_hours > 0, f"{table}: sla_hours must be > 0"

    def test_bronze_required_columns_non_empty(self):
        for domain, cfg in BRONZE_CONFIGS.items():
            assert cfg.required_columns, f"{domain}: required_columns must not be empty"

    def test_bronze_pk_col_in_required_when_set(self):
        for domain, cfg in BRONZE_CONFIGS.items():
            if cfg.pk_col:
                assert (
                    cfg.pk_col in cfg.required_columns
                ), f"{domain}: pk_col '{cfg.pk_col}' should appear in required_columns"

    def test_silver_fk_check_tuples_are_triples(self):
        for domain, cfg in SILVER_CONFIGS.items():
            for entry in cfg.fk_checks:
                assert (
                    len(entry) == 3
                ), f"{domain}: each fk_check must be (fk_col, parent_domain, parent_pk)"

    def test_gold_facts_have_sk_date_in_required(self):
        fact_tables = [t for t in _ALL_GOLD_TABLES if t.startswith("fact_")]
        for tbl in fact_tables:
            cfg = GOLD_CONFIGS[tbl]
            assert (
                "sk_date_id" in cfg.required_columns
            ), f"{tbl}: sk_date_id must be a required column"

    def test_gold_dims_have_pk_col(self):
        dim_tables = [t for t in _ALL_GOLD_TABLES if t.startswith("dim_")]
        for tbl in dim_tables:
            cfg = GOLD_CONFIGS[tbl]
            assert cfg.pk_col is not None, f"{tbl}: Gold dimensions must define a pk_col"

    def test_clickstream_sla_is_short(self):
        # Clickstream and IoT are near-real-time — SLA must be ≤4 h at Bronze
        assert BRONZE_CONFIGS["clickstream"].sla_hours <= 4.0
        assert BRONZE_CONFIGS["iot_sensors"].sla_hours <= 4.0

    def test_numeric_ranges_lo_le_hi(self):
        for layer_name, configs in [
            ("bronze", BRONZE_CONFIGS),
            ("silver", SILVER_CONFIGS),
            ("gold", GOLD_CONFIGS),
        ]:
            for domain, cfg in configs.items():
                for col, (lo, hi) in cfg.numeric_ranges.items():
                    if lo is not None and hi is not None:
                        assert lo <= hi, f"{layer_name}/{domain}/{col}: lo={lo} > hi={hi}"
