"""
Unit tests for DQRunner, PostgresReporter, and AlertReporter.
All external I/O (Spark, PostgreSQL) is mocked.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.quality.checks.base_check import CheckResult, CheckStatus
from src.quality.reporters.alert_reporter import AlertReporter
from src.quality.reporters.pg_reporter import PostgresReporter


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _r(
    domain="orders",
    layer="bronze",
    check="completeness",
    status=CheckStatus.PASSED,
    col=None,
    metric=None,
    threshold=None,
    message="ok",
) -> CheckResult:
    return CheckResult(
        domain=domain,
        layer=layer,
        check_name=check,
        column_name=col,
        status=status,
        rows_checked=1000,
        message=message,
        metric_value=metric,
        threshold=threshold,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PostgresReporter
# ─────────────────────────────────────────────────────────────────────────────
class TestPostgresReporter:
    @pytest.fixture(autouse=True)
    def _stub_execute_batch(self):
        # psycopg2.extras.execute_batch needs a real cursor (mogrify); stub it and
        # assert on the calls instead.
        with patch("src.quality.reporters.pg_reporter.psycopg2.extras.execute_batch") as eb:
            self.execute_batch = eb
            yield

    def _make_conn(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return conn, cur

    def test_save_results_returns_run_id(self):
        conn, _ = self._make_conn()
        reporter = PostgresReporter(conn)
        run_id = reporter.save_results([_r()])
        assert isinstance(run_id, str) and len(run_id) == 36  # UUID v4

    def test_save_results_accepts_provided_run_id(self):
        conn, _ = self._make_conn()
        reporter = PostgresReporter(conn)
        run_id = reporter.save_results([_r()], run_id="abc-123")
        assert run_id == "abc-123"

    def test_save_results_commits(self):
        conn, _ = self._make_conn()
        PostgresReporter(conn).save_results([_r()])
        conn.commit.assert_called_once()

    def test_save_report_commits(self):
        conn, _ = self._make_conn()
        results = [
            _r(status=CheckStatus.PASSED),
            _r(status=CheckStatus.FAILED),
        ]
        PostgresReporter(conn).save_report(results, run_id="run-1")
        conn.commit.assert_called_once()

    def test_save_sla_misses_skips_non_freshness(self):
        conn, cur = self._make_conn()
        results = [
            _r(check="completeness", status=CheckStatus.FAILED),
            _r(check="uniqueness", status=CheckStatus.FAILED),
        ]
        PostgresReporter(conn).save_sla_misses(results, run_id="run-1")
        # No freshness FAILED → commit should not be called for SLA misses
        conn.commit.assert_not_called()

    def test_save_sla_misses_inserts_freshness_failures(self):
        conn, _ = self._make_conn()
        results = [
            _r(
                check="freshness",
                status=CheckStatus.FAILED,
                col="created_at",
                metric=30.5,
                threshold=26.0,
            ),
        ]
        PostgresReporter(conn).save_sla_misses(results, run_id="run-1")
        conn.commit.assert_called_once()

    def test_save_sla_misses_ignores_freshness_passed(self):
        conn, _ = self._make_conn()
        results = [
            _r(check="freshness", status=CheckStatus.PASSED),
        ]
        PostgresReporter(conn).save_sla_misses(results, run_id="run-1")
        conn.commit.assert_not_called()

    def test_save_report_groups_by_domain_layer(self):
        """One report row per (domain, layer) combination."""
        conn, cur = self._make_conn()
        results = [
            _r(domain="orders", layer="bronze", status=CheckStatus.PASSED),
            _r(domain="orders", layer="bronze", status=CheckStatus.FAILED),
            _r(domain="payments", layer="bronze", status=CheckStatus.PASSED),
        ]
        PostgresReporter(conn).save_report(results, run_id="run-1")
        # execute_batch should be called once (or multiple times); we just
        # verify commit was called to confirm the write happened
        conn.commit.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# AlertReporter
# ─────────────────────────────────────────────────────────────────────────────
class TestAlertReporterUnit:
    def test_report_does_not_raise_on_passed(self):
        AlertReporter(fail_on_critical=True).report(
            [
                _r(check="row_count.minimum", status=CheckStatus.PASSED),
            ]
        )

    def test_summary_pass_rate_zero_when_all_failed(self):
        results = [_r(status=CheckStatus.FAILED)] * 4
        s = AlertReporter().summary(results)
        assert s["pass_rate"] == 0.0

    def test_summary_pass_rate_one_when_all_passed(self):
        results = [_r(status=CheckStatus.PASSED)] * 4
        s = AlertReporter().summary(results)
        assert s["pass_rate"] == 1.0


# ─────────────────────────────────────────────────────────────────────────────
# DQRunner (all I/O mocked)
# ─────────────────────────────────────────────────────────────────────────────
class TestDQRunner:
    def _make_runner(self):
        from src.quality.dq_runner import DQRunner

        spark = MagicMock()
        settings = MagicMock()
        settings.postgres_dsn = "postgresql://test"
        return DQRunner(spark=spark, settings=settings)

    def _patch_all(self, mock_results=None):
        """Return a set of patches that bypass all I/O."""
        if mock_results is None:
            mock_results = [_r(status=CheckStatus.PASSED)]
        return {
            "psycopg2": patch("src.quality.dq_runner.psycopg2"),
            # No autospec: DQRunner closes ``pg.conn``, an instance attribute
            # that autospec of the class cannot see.
            "pg_reporter": patch("src.quality.dq_runner.PostgresReporter"),
            "bronze_engine": patch(
                "src.quality.engines.bronze_dq.BronzeDQEngine.run_all",
                return_value=mock_results,
            ),
            "silver_engine": patch(
                "src.quality.engines.silver_dq.SilverDQEngine.run_all",
                return_value=mock_results,
            ),
            "gold_engine": patch(
                "src.quality.engines.gold_dq.GoldDQEngine.run_all",
                return_value=mock_results,
            ),
        }

    def test_run_returns_summary_keys(self):
        runner = self._make_runner()
        patches = self._patch_all()

        with (
            patches["psycopg2"],
            patches["pg_reporter"],
            patch(
                "src.quality.dq_runner.DQRunner._run_engine",
                return_value=[_r()],
            ),
        ):
            summary = runner.run(layer="bronze", batch_id="2025-06-01T10:00:00")

        assert "run_id" in summary
        assert "batch_id" in summary
        assert "layer" in summary
        assert "total" in summary
        assert "passed" in summary
        assert "failed" in summary
        assert "pass_rate" in summary

    def test_run_invalid_layer_raises(self):
        runner = self._make_runner()
        with pytest.raises(ValueError, match="layer must be one of"):
            runner.run(layer="platinum")

    def test_run_all_layers_returns_combined_summary(self):
        runner = self._make_runner()
        with (
            patch("src.quality.dq_runner.psycopg2"),
            patch("src.quality.dq_runner.PostgresReporter"),
            patch(
                "src.quality.dq_runner.DQRunner._run_engine",
                return_value=[_r()],
            ),
        ):
            summary = runner.run_all_layers(batch_id="2025-06-01T10:00:00")

        assert "run_id" in summary
        assert "overall" in summary
        assert "by_layer" in summary
        assert set(summary["by_layer"]) == {"bronze", "silver", "gold"}

    def test_fail_on_critical_propagates(self):
        from src.quality.dq_runner import DQRunner

        spark = MagicMock()
        settings = MagicMock()
        settings.postgres_dsn = "postgresql://test"
        runner = DQRunner(spark=spark, settings=settings, fail_on_critical=True)

        critical_result = _r(check="row_count.minimum", status=CheckStatus.FAILED)

        with (
            patch("src.quality.dq_runner.psycopg2"),
            patch("src.quality.dq_runner.PostgresReporter"),
            patch(
                "src.quality.dq_runner.DQRunner._run_engine",
                return_value=[critical_result],
            ),
            pytest.raises(RuntimeError),
        ):
            runner.run(layer="bronze")

    def test_run_engine_selects_bronze(self):
        runner = self._make_runner()
        mock_conn = MagicMock()
        with patch("src.quality.engines.bronze_dq.BronzeDQEngine") as mock_cls:
            mock_cls.return_value.run_all.return_value = [_r()]
            results = runner._run_engine("bronze", None, mock_conn)
        mock_cls.assert_called_once()
        assert len(results) == 1

    def test_run_engine_selects_silver(self):
        runner = self._make_runner()
        mock_conn = MagicMock()
        with patch("src.quality.engines.silver_dq.SilverDQEngine") as mock_cls:
            mock_cls.return_value.run_all.return_value = [_r()]
            runner._run_engine("silver", None, mock_conn)
        mock_cls.assert_called_once()

    def test_run_engine_selects_gold(self):
        runner = self._make_runner()
        mock_conn = MagicMock()
        with patch("src.quality.engines.gold_dq.GoldDQEngine") as mock_cls:
            mock_cls.return_value.run_all.return_value = [_r()]
            runner._run_engine("gold", None, mock_conn)
        mock_cls.assert_called_once()
