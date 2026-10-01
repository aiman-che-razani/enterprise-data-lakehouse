"""
Unit tests for custom Airflow operators and hooks.
No live Airflow DB, Spark cluster, or PostgreSQL connection required.
All external I/O is mocked.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root is on the path
_PROJECT_ROOT = str(Path(__file__).parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")


# ─────────────────────────────────────────────────────────────────────────────
# DQSparkOperator
# ─────────────────────────────────────────────────────────────────────────────


def _test_dag():
    """A throwaway DAG: Airflow 2.9 refuses ``operator.dag`` on an operator that has none."""
    from datetime import datetime

    from airflow import DAG

    return DAG("operator_unit_test", start_date=datetime(2025, 1, 1), schedule=None)


class TestDQSparkOperator:
    def _make(self, layer="bronze", **kwargs):
        from operators.dq_operator import DQSparkOperator

        return DQSparkOperator(task_id=f"dq_{layer}", layer=layer, **kwargs)

    def test_valid_layers_accepted(self):
        for layer in ("bronze", "silver", "gold", "all"):
            op = self._make(layer=layer)
            assert op.layer == layer

    def test_invalid_layer_raises(self):
        from operators.dq_operator import DQSparkOperator

        with pytest.raises(ValueError, match="layer must be one of"):
            DQSparkOperator(task_id="bad", layer="platinum")

    def test_default_batch_id_is_templated(self):
        op = self._make()
        assert "{{ ds }}" in op.batch_id

    def test_domains_default_empty(self):
        op = self._make()
        assert op.domains == []

    def test_domains_set_correctly(self):
        op = self._make(layer="silver", domains=["orders", "payments"])
        assert op.domains == ["orders", "payments"]

    def test_fail_on_critical_default_false(self):
        op = self._make()
        assert op.fail_on_critical is False

    def test_execute_builds_correct_args(self):
        from operators.dq_operator import DQSparkOperator

        op = DQSparkOperator(
            dag=_test_dag(),
            task_id="test_dq",
            layer="silver",
            domains=["orders"],
            fail_on_critical=True,
            batch_id="2025-06-01T00:00:00",
        )

        context = MagicMock()
        mock_submit = MagicMock()

        with patch(
            "operators.dq_operator.SparkSubmitOperator",
            return_value=mock_submit,
        ) as mock_cls:
            op.execute(context)

        call_kwargs = mock_cls.call_args.kwargs
        assert call_kwargs["application"] == "spark/jobs/quality/run_dq.py"
        args = call_kwargs["application_args"]
        assert "--layer" in args
        assert "silver" in args
        assert "--batch-id" in args
        assert "--domains" in args
        assert "orders" in args
        assert "--fail-fast" in args

    def test_execute_no_fail_fast_without_flag(self):
        from operators.dq_operator import DQSparkOperator

        op = DQSparkOperator(
            dag=_test_dag(),
            task_id="test_dq2",
            layer="bronze",
            batch_id="2025-06-01T00:00:00",
        )
        context = MagicMock()
        mock_submit = MagicMock()

        with patch(
            "operators.dq_operator.SparkSubmitOperator",
            return_value=mock_submit,
        ) as mock_cls:
            op.execute(context)

        args = mock_cls.call_args.kwargs["application_args"]
        assert "--fail-fast" not in args

    def test_execute_no_domains_flag_when_all(self):
        from operators.dq_operator import DQSparkOperator

        op = DQSparkOperator(
            dag=_test_dag(),
            task_id="test_dq3",
            layer="bronze",
            batch_id="2025-06-01T00:00:00",
        )
        context = MagicMock()
        mock_submit = MagicMock()

        with patch(
            "operators.dq_operator.SparkSubmitOperator",
            return_value=mock_submit,
        ) as mock_cls:
            op.execute(context)

        args = mock_cls.call_args.kwargs["application_args"]
        assert "--domains" not in args


# ─────────────────────────────────────────────────────────────────────────────
# WatermarkSensor
# ─────────────────────────────────────────────────────────────────────────────
class TestWatermarkSensor:
    def _make(self, **kwargs):
        from sensors.watermark_sensor import WatermarkSensor

        defaults = {
            "task_id": "wm_sensor",
            "domain": "orders",
            "column": "bronze_last_ingest",
            "min_value": "2025-06-01 02:00:00",
            "poke_interval": 30,
            "timeout": 3600,
        }
        defaults.update(kwargs)
        return WatermarkSensor(**defaults)

    def _mock_conn(self, return_value):
        """Return a mocked psycopg2 + Airflow connection that yields *return_value*."""
        mock_conn_info = MagicMock()
        mock_conn_info.host = "localhost"
        mock_conn_info.port = 5432
        mock_conn_info.schema = "retailnexus"
        mock_conn_info.login = "pipeline_user"
        mock_conn_info.password = "secret"

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone = MagicMock(return_value=(return_value,))

        mock_pg_conn = MagicMock()
        mock_pg_conn.cursor.return_value = mock_cursor

        return mock_conn_info, mock_pg_conn

    def test_poke_returns_false_when_value_is_none(self):
        sensor = self._make()
        conn_info, pg_conn = self._mock_conn(None)

        with (
            patch("sensors.watermark_sensor.BaseHook") as mock_hook,
            patch("sensors.watermark_sensor.psycopg2") as mock_psycopg,
        ):
            mock_hook.get_connection.return_value = conn_info
            mock_psycopg.connect.return_value = pg_conn
            assert sensor.poke(MagicMock()) is False

    def test_poke_returns_false_when_row_is_none(self):
        sensor = self._make()
        conn_info, pg_conn = self._mock_conn(None)
        pg_conn.cursor.return_value.__enter__.return_value.fetchone.return_value = None

        with (
            patch("sensors.watermark_sensor.BaseHook") as mock_hook,
            patch("sensors.watermark_sensor.psycopg2") as mock_psycopg,
        ):
            mock_hook.get_connection.return_value = conn_info
            mock_psycopg.connect.return_value = pg_conn
            assert sensor.poke(MagicMock()) is False

    def test_poke_returns_true_when_actual_meets_min(self):
        sensor = self._make(min_value="2025-06-01 02:00:00")
        # actual = "2025-06-01 03:00:00" which is ≥ min
        conn_info, pg_conn = self._mock_conn("2025-06-01 03:00:00")

        with (
            patch("sensors.watermark_sensor.BaseHook") as mock_hook,
            patch("sensors.watermark_sensor.psycopg2") as mock_psycopg,
        ):
            mock_hook.get_connection.return_value = conn_info
            mock_psycopg.connect.return_value = pg_conn
            assert sensor.poke(MagicMock()) is True

    def test_poke_returns_false_when_actual_before_min(self):
        sensor = self._make(min_value="2025-06-01 02:00:00")
        # actual = "2025-06-01 01:00:00" which is < min
        conn_info, pg_conn = self._mock_conn("2025-06-01 01:00:00")

        with (
            patch("sensors.watermark_sensor.BaseHook") as mock_hook,
            patch("sensors.watermark_sensor.psycopg2") as mock_psycopg,
        ):
            mock_hook.get_connection.return_value = conn_info
            mock_psycopg.connect.return_value = pg_conn
            assert sensor.poke(MagicMock()) is False

    def test_template_fields_include_min_value(self):
        from sensors.watermark_sensor import WatermarkSensor

        assert "min_value" in WatermarkSensor.template_fields
        assert "domain" in WatermarkSensor.template_fields
        assert "column" in WatermarkSensor.template_fields


# ─────────────────────────────────────────────────────────────────────────────
# PipelinePostgresHook
# ─────────────────────────────────────────────────────────────────────────────
class TestPipelinePostgresHook:
    def _make_hook(self, rows=None, scalar=None):
        from hooks.pipeline_hook import PipelinePostgresHook

        hook = PipelinePostgresHook()
        mock_cur = MagicMock()
        mock_cur.__enter__ = MagicMock(return_value=mock_cur)
        mock_cur.__exit__ = MagicMock(return_value=False)

        if scalar is not None:
            mock_cur.fetchone.return_value = (scalar,)
        elif rows is not None:
            mock_cur.fetchall.return_value = rows
            mock_cur.fetchone.return_value = (len(rows),)
        mock_cur.description = [("col",)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        hook.get_conn = MagicMock(return_value=mock_conn)
        return hook, mock_conn, mock_cur

    def test_get_watermark_returns_scalar(self):
        hook, _, cur = self._make_hook(scalar="2025-06-01 02:00:00")
        result = hook.get_watermark("orders", "bronze_last_ingest")
        assert result == "2025-06-01 02:00:00"

    def test_get_watermark_returns_none_on_empty(self):
        hook, _, cur = self._make_hook()
        cur.fetchone.return_value = None
        result = hook.get_watermark("orders", "bronze_last_ingest")
        assert result is None

    def test_get_sla_miss_count_returns_int(self):
        hook, _, cur = self._make_hook(scalar=7)
        count = hook.get_sla_miss_count(hours=24.0)
        assert count == 7

    def test_get_recent_dq_failures_returns_list(self):
        hook, _, cur = self._make_hook(
            rows=[
                ("orders", "silver", "completeness", "email", 0.12, 0.05, "too many nulls", None),
            ]
        )
        cur.description = [
            ("domain",),
            ("layer",),
            ("check_name",),
            ("column_name",),
            ("metric_value",),
            ("threshold",),
            ("message",),
            ("run_at",),
        ]
        results = hook.get_recent_dq_failures(hours=1.0)
        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]["domain"] == "orders"

    def test_get_dq_pass_rate_returns_dict(self):
        hook, _, cur = self._make_hook()
        cur.fetchall.return_value = [("bronze", 0.95), ("silver", 0.88)]
        rates = hook.get_dq_pass_rate(hours=24.0)
        assert isinstance(rates, dict)
        assert rates["bronze"] == pytest.approx(0.95)
        assert rates["silver"] == pytest.approx(0.88)

    def test_default_conn_name(self):
        from hooks.pipeline_hook import PipelinePostgresHook

        assert PipelinePostgresHook.default_conn_name == "postgres_pipeline"


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap script
# ─────────────────────────────────────────────────────────────────────────────
class TestBootstrapScript:
    def test_variables_dict_has_required_keys(self):
        import scripts.airflow.bootstrap_airflow as bs

        variables = bs._variables()
        assert "BRONZE_PATH" in variables
        assert "SILVER_PATH" in variables
        assert "GOLD_PATH" in variables
        assert "ALL_DOMAINS" in variables
        assert "GOLD_DIMS" in variables
        assert "GOLD_FACTS" in variables

    def test_variables_paths_contain_s3a(self):
        import scripts.airflow.bootstrap_airflow as bs

        variables = bs._variables()
        assert variables["BRONZE_PATH"].startswith("s3a://")

    def test_connections_list_non_empty(self):
        import scripts.airflow.bootstrap_airflow as bs

        connections = bs._connections()
        assert len(connections) >= 3

    def test_connections_include_spark_and_postgres(self):
        import scripts.airflow.bootstrap_airflow as bs

        conn_ids = {c["conn_id"] for c in bs._connections()}
        assert "spark_default" in conn_ids
        assert "postgres_pipeline" in conn_ids
        assert "minio_default" in conn_ids

    def test_all_domains_comma_separated(self):
        import scripts.airflow.bootstrap_airflow as bs

        variables = bs._variables()
        all_domains = variables["ALL_DOMAINS"].split(",")
        assert len(all_domains) == 16

    def test_every_connection_id_used_by_the_dags_is_bootstrapped(self):
        import re
        from pathlib import Path

        import scripts.airflow.bootstrap_airflow as bs

        used: set[str] = set()
        for f in (Path(__file__).parents[2] / "airflow").rglob("*.py"):
            used |= set(re.findall(r"conn_id=[\"']([a-z_]+)[\"']", f.read_text(encoding="utf-8")))
            used |= set(
                re.findall(r"get_connection\([\"']([a-z_]+)[\"']\)", f.read_text(encoding="utf-8"))
            )
        defined = {c["conn_id"] for c in bs._connections()}
        assert (
            used and used <= defined
        ), f"connections used but never created: {sorted(used - defined)}"

    def test_spark_connection_host_has_no_port(self):
        import scripts.airflow.bootstrap_airflow as bs

        spark = next(c for c in bs._connections() if c["conn_id"] == "spark_default")
        assert spark["host"] == "spark://spark-master" and spark["port"] == 7077


class TestDQOperatorInsideADag:
    def test_the_inner_spark_task_does_not_collide_with_the_operator_in_a_real_dag(self):
        """Regression: the inner SparkSubmitOperator reused this operator's task_id, which Airflow rejects
        ("Task id ... has already been added to the DAG") as soon as the operator runs in its own DAG.
        """
        from unittest.mock import MagicMock, patch

        from operators.dq_operator import DQSparkOperator

        dag = _test_dag()
        op = DQSparkOperator(task_id="silver_dq_gate", layer="silver", dag=dag)
        with patch("operators.dq_operator.SparkSubmitOperator.execute", MagicMock()):
            op.execute(MagicMock())

        assert "silver_dq_gate" in dag.task_ids
        assert "_spark_silver_dq_gate" in dag.task_ids
