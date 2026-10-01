"""Static DAG checks that need no Airflow install: plugin imports resolve and the DQ gates are wired."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_DAGS = sorted((_ROOT / "airflow" / "dags").glob("*_dag.py"))
_PLUGINS = _ROOT / "airflow" / "plugins"


def _src(name: str) -> str:
    return (_ROOT / "airflow" / "dags" / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("dag", _DAGS, ids=lambda p: p.name)
def test_plugin_imports_use_airflows_plugin_path_and_resolve(dag):
    """In the container $AIRFLOW_HOME/plugins is on sys.path, so plugins import as `operators.x`.
    `airflow.plugins.x` does not exist there (bronze_batch_dag used it and failed to import)."""
    tree = ast.parse(dag.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            assert not node.module.startswith("airflow.plugins"), f"{dag.name}: {node.module}"
            top, _, rest = node.module.partition(".")
            if top in {"operators", "sensors", "hooks"} and rest:
                assert (
                    _PLUGINS / top / f"{rest}.py"
                ).is_file(), f"{dag.name}: no plugin module {node.module}"


def test_no_plugin_uses_the_shadowed_import_path():
    for path in _PLUGINS.rglob("*.py"):
        assert "airflow.plugins" not in path.read_text(encoding="utf-8").replace(
            "airflow.plugins_manager", ""
        ), path.name


class TestQualityGates:
    def test_silver_gate_blocks_all_gold_builds(self):
        text = _src("gold_batch_dag.py")
        assert re.search(
            r'DQSparkOperator\(\s*task_id="silver_dq_gate",\s*layer="silver",\s*fail_on_critical=True',
            text,
        )
        assert "start >> silver_dq_gate" in text
        assert (
            "prev_phase_tasks: list = [silver_dq_gate]" in text
        )  # the first Gold phase hangs off the gate

    def test_gold_gate_blocks_all_warehouse_loads(self):
        text = _src("warehouse_load_dag.py")
        assert re.search(
            r'DQSparkOperator\(\s*task_id="gold_dq_gate",\s*layer="gold",\s*fail_on_critical=True',
            text,
        )
        assert "start >> gold_dq_gate >> [rs_dims, sf_dims, bq_dims]" in text

    def test_reporting_dag_still_runs_all_layers(self):
        assert "bronze_dq" in _src("dq_dag.py") and "gold_dq" in _src("dq_dag.py")
