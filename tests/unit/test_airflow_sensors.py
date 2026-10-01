"""
Airflow sensors (WatermarkSensor, DQPassRateSensor).

The sensor modules are loaded by file path with a minimal stand-in for Airflow when the real
package is not importable, so these tests run in any environment (the repo's top-level
``airflow/`` directory shadows the real package name, which made ``airflow.plugins.*``
imports environment-dependent).
"""

from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_SENSORS = Path(__file__).resolve().parents[2] / "airflow" / "plugins" / "sensors"


def _install_fake_airflow() -> None:
    try:
        from airflow.hooks.base import BaseHook
        from airflow.sensors.base import BaseSensorOperator
        from airflow.utils.context import Context  # noqa: F401

        return
    except Exception:
        pass

    class BaseSensorOperator:
        template_fields: tuple = ()

        def __init__(
            self, task_id: str = "t", poke_interval: int = 60, timeout: int = 0, **_: object
        ) -> None:
            self.task_id, self.poke_interval, self.timeout = task_id, poke_interval, timeout
            self.log = MagicMock()

    class BaseHook:
        @staticmethod
        def get_connection(conn_id: str):  # replaced per-test
            raise RuntimeError("no connection configured")

    modules = {
        "airflow": types.ModuleType("airflow"),
        "airflow.sensors": types.ModuleType("airflow.sensors"),
        "airflow.sensors.base": types.ModuleType("airflow.sensors.base"),
        "airflow.hooks": types.ModuleType("airflow.hooks"),
        "airflow.hooks.base": types.ModuleType("airflow.hooks.base"),
        "airflow.utils": types.ModuleType("airflow.utils"),
        "airflow.utils.context": types.ModuleType("airflow.utils.context"),
    }
    modules["airflow.sensors.base"].BaseSensorOperator = BaseSensorOperator
    modules["airflow.hooks.base"].BaseHook = BaseHook
    modules["airflow.utils.context"].Context = dict
    for name, mod in modules.items():
        sys.modules[name] = mod


def _load(filename: str):
    _install_fake_airflow()
    spec = importlib.util.spec_from_file_location(f"_sensor_{filename[:-3]}", _SENSORS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def wm_module():
    return _load("watermark_sensor.py")


@pytest.fixture()
def dq_module():
    return _load("dq_gate_sensor.py")


def _patch_db(monkeypatch, row):
    """Route psycopg2.connect + BaseHook.get_connection to fakes; return the cursor."""
    from airflow.hooks.base import BaseHook

    info = MagicMock(host="h", port=5432, schema="db", login="u", password="p")
    monkeypatch.setattr(BaseHook, "get_connection", staticmethod(lambda _id: info))
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchone.return_value = row
    monkeypatch.setattr("psycopg2.connect", MagicMock(return_value=conn))
    return cur, conn


class TestSensorsPackage:
    def test_sensors_live_in_the_sensors_package(self):
        assert (_SENSORS / "watermark_sensor.py").is_file()
        assert (_SENSORS / "dq_gate_sensor.py").is_file()

    def test_legacy_operators_import_path_still_works(self):
        shim = (_SENSORS.parent / "operators" / "watermark_sensor.py").read_text(encoding="utf-8")
        assert "from sensors.watermark_sensor import WatermarkSensor" in shim


class TestWatermarkSensor:
    def _sensor(self, mod, **kw):
        defaults = {
            "task_id": "wm",
            "domain": "orders",
            "column": "bronze_last_ingest",
            "min_value": "2025-06-01 02:00:00",
        }
        return mod.WatermarkSensor(**{**defaults, **kw})

    def test_true_when_watermark_reached(self, wm_module, monkeypatch):
        _patch_db(monkeypatch, (datetime(2025, 6, 1, 3, 0),))
        assert self._sensor(wm_module).poke({}) is True

    def test_false_when_watermark_behind(self, wm_module, monkeypatch):
        _patch_db(monkeypatch, (datetime(2025, 6, 1, 1, 0),))
        assert self._sensor(wm_module).poke({}) is False

    @pytest.mark.parametrize("row", [None, (None,)])
    def test_false_when_no_value_yet(self, wm_module, monkeypatch, row):
        _patch_db(monkeypatch, row)
        assert self._sensor(wm_module).poke({}) is False

    def test_column_is_quoted_as_an_identifier(self, wm_module, monkeypatch):
        cur, _ = _patch_db(monkeypatch, (datetime(2025, 6, 2),))
        self._sensor(wm_module, column="silver_last_run").poke({})
        from psycopg2 import sql

        stmt = cur.execute.call_args[0][0]
        assert isinstance(
            stmt, sql.Composed
        ), "column name must never be interpolated as a plain string"
        assert [p.strings for p in stmt.seq if isinstance(p, sql.Identifier)] == [
            ("silver_last_run",)
        ]

    def test_connection_always_closed(self, wm_module, monkeypatch):
        _, conn = _patch_db(monkeypatch, None)
        self._sensor(wm_module).poke({})
        conn.close.assert_called_once()

    def test_min_value_is_templated(self, wm_module):
        assert "min_value" in wm_module.WatermarkSensor.template_fields


class TestDQPassRateSensor:
    def _sensor(self, mod, **kw):
        return mod.DQPassRateSensor(
            **{**{"task_id": "gate", "layer": "silver", "min_pass_rate": 0.95}, **kw}
        )

    def test_passes_when_rate_meets_threshold(self, dq_module, monkeypatch):
        _patch_db(monkeypatch, (0.97, 16))
        assert self._sensor(dq_module).poke({}) is True

    def test_waits_when_rate_below_threshold(self, dq_module, monkeypatch):
        _patch_db(monkeypatch, (0.90, 16))
        assert self._sensor(dq_module).poke({}) is False

    @pytest.mark.parametrize("row", [(None, 0), (0.99, 0), None])
    def test_waits_when_no_recent_report(self, dq_module, monkeypatch, row):
        _patch_db(monkeypatch, row)
        assert self._sensor(dq_module).poke({}) is False

    def test_query_is_parameterised_by_layer_and_window(self, dq_module, monkeypatch):
        cur, conn = _patch_db(monkeypatch, (1.0, 1))
        self._sensor(dq_module, layer="gold", max_age_hours=6).poke({})
        sql, params = cur.execute.call_args[0]
        assert "FROM quality.reports" in sql and "layer = %s" in sql
        assert params == ("gold", 6.0)
        conn.close.assert_called_once()

    def test_threshold_is_inclusive(self, dq_module, monkeypatch):
        _patch_db(monkeypatch, (0.95, 3))
        assert self._sensor(dq_module).poke({}) is True

    @pytest.mark.parametrize(
        "kwargs", [{"layer": "platinum"}, {"min_pass_rate": 1.5}, {"min_pass_rate": -0.1}]
    )
    def test_invalid_arguments_rejected(self, dq_module, kwargs):
        with pytest.raises(ValueError):
            self._sensor(dq_module, **kwargs)
