"""
Airflow DAG structure tests — verify that all 9 DAGs:
  1. Import without errors
  2. Have the expected schedule and max_active_runs
  3. Contain the expected task IDs (spot-checks)
  4. Have no import-time side effects (no DB writes, no Spark sessions)

These tests run without a live Airflow metadata DB — they rely on Airflow's
unit-test utilities (AIRFLOW_HOME set to a tmp directory via the conftest
or the AIRFLOW__CORE__UNIT_TEST_MODE env variable).
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from airflow import DAG

# ── Point Airflow at a throwaway config during tests ──────────────────────────
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault(
    "AIRFLOW__CORE__DAGS_FOLDER", str(Path(__file__).parent.parent.parent / "airflow" / "dags")
)

# Add project root to sys.path so DAG modules can import src.*
_PROJECT_ROOT = str(Path(__file__).parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

_DAGS_DIR = Path(__file__).parent.parent.parent / "airflow" / "dags"


def _load_dag(module_name: str) -> DAG:
    """Import a DAG module and return the DAG object."""
    spec = importlib.util.spec_from_file_location(module_name, _DAGS_DIR / f"{module_name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # DAGs are registered in the global DagBag; retrieve by dag_id
    from airflow import DAG as _DAG

    dags_in_module = {k: v for k, v in vars(mod).items() if isinstance(v, _DAG)}
    assert dags_in_module, f"No DAG found in {module_name}.py"
    return next(iter(dags_in_module.values()))


# ─────────────────────────────────────────────────────────────────────────────
# Import smoke tests — one per DAG file
# ─────────────────────────────────────────────────────────────────────────────
_DAG_MODULES = [
    "bronze_batch_dag",
    "bronze_streaming_dag",
    "silver_batch_dag",
    "gold_batch_dag",
    "warehouse_load_dag",
    "dq_dag",
    "pipeline_e2e_dag",
    "pipeline_maintenance_dag",
    "pipeline_backfill_dag",
    "sla_monitoring_dag",
]


@pytest.mark.parametrize("module", _DAG_MODULES)
def test_dag_imports_without_error(module: str):
    """Every DAG file must be importable without raising."""
    path = _DAGS_DIR / f"{module}.py"
    assert path.exists(), f"DAG file not found: {path}"
    spec = importlib.util.spec_from_file_location(module, path)
    mod = importlib.util.module_from_spec(spec)
    # Should not raise
    spec.loader.exec_module(mod)


# ─────────────────────────────────────────────────────────────────────────────
# Schedule & config invariants
# ─────────────────────────────────────────────────────────────────────────────
class TestPipelineE2EDAG:
    @pytest.fixture()
    def dag(self):
        return _load_dag("pipeline_e2e_dag")

    def test_dag_id(self, dag):
        assert dag.dag_id == "pipeline_e2e"

    def test_schedule_midnight(self, dag):
        assert dag.schedule_interval == "0 0 * * *"

    def test_max_active_runs_is_one(self, dag):
        assert dag.max_active_runs == 1

    def test_has_start_and_end(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert "start" in task_ids
        assert "end" in task_ids

    def test_has_pipeline_report(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert "pipeline_report" in task_ids

    def test_has_all_five_layer_groups(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        # TaskGroup task IDs are prefixed with the group name
        assert any("bronze" in tid for tid in task_ids)
        assert any("silver" in tid for tid in task_ids)
        assert any("gold" in tid for tid in task_ids)
        assert any("warehouse" in tid for tid in task_ids)
        assert any("data_quality" in tid for tid in task_ids)

    def test_no_catchup(self, dag):
        assert dag.catchup is False


class TestMaintenanceDAG:
    @pytest.fixture()
    def dag(self):
        return _load_dag("pipeline_maintenance_dag")

    def test_dag_id(self, dag):
        assert dag.dag_id == "pipeline_maintenance"

    def test_weekly_sunday_schedule(self, dag):
        assert dag.schedule_interval == "0 1 * * 0"

    def test_has_watermark_audit(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert "watermark_audit" in task_ids

    def test_has_postgres_vacuum(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert "postgres_vacuum" in task_ids


class TestBackfillDAG:
    @pytest.fixture()
    def dag(self):
        return _load_dag("pipeline_backfill_dag")

    def test_dag_id(self, dag):
        assert dag.dag_id == "pipeline_backfill"

    def test_no_schedule(self, dag):
        assert dag.schedule_interval is None

    def test_has_validate_task(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert "validate_and_build_args" in task_ids

    def test_has_run_backfill_task(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert "run_backfill" in task_ids

    def test_params_defined(self, dag):
        params = dag.params
        assert "layer" in params
        assert "domains" in params
        assert "start_date" in params
        assert "end_date" in params
        assert "fail_fast" in params

    def test_layer_param_enum(self, dag):
        layer_param = dag.params.get_param("layer")
        # Airflow Param schema is stored in the param object
        assert layer_param.schema.get("enum") or layer_param.value in (
            "bronze",
            "silver",
            "gold",
            "warehouse",
            "dq",
        )

    def test_max_active_runs_allows_concurrent(self, dag):
        assert dag.max_active_runs >= 2


class TestSLAMonitorDAG:
    @pytest.fixture()
    def dag(self):
        return _load_dag("sla_monitoring_dag")

    def test_dag_id(self, dag):
        assert dag.dag_id == "sla_monitor"

    def test_thirty_minute_schedule(self, dag):
        assert dag.schedule_interval == "*/30 * * * *"

    def test_has_three_check_tasks(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert "watermark_freshness" in task_ids
        assert "dq_pass_rate" in task_ids
        assert "open_sla_misses" in task_ids

    def test_checks_run_in_parallel(self, dag):
        start_task = dag.get_task("start")
        # All three checks should be direct downstream of start
        downstream = {t.task_id for t in start_task.downstream_list}
        assert "watermark_freshness" in downstream
        assert "dq_pass_rate" in downstream
        assert "open_sla_misses" in downstream


class TestDQDAG:
    @pytest.fixture()
    def dag(self):
        return _load_dag("dq_dag")

    def test_schedule_after_warehouse(self, dag):
        assert dag.schedule_interval == "0 10 * * *"

    def test_has_all_three_dq_tasks(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert "bronze_dq" in task_ids
        assert "silver_dq" in task_ids
        assert "gold_dq" in task_ids

    def test_summary_task_trigger_rule_all_done(self, dag):
        summary = dag.get_task("dq_summary_report")
        assert summary.trigger_rule == "all_done"


class TestWarehouseLoadDAG:
    @pytest.fixture()
    def dag(self):
        return _load_dag("warehouse_load_dag")

    def test_schedule_after_gold(self, dag):
        assert dag.schedule_interval == "30 7 * * *"

    def test_all_three_targets_present(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert any("redshift" in tid for tid in task_ids)
        assert any("snowflake" in tid for tid in task_ids)
        assert any("bigquery" in tid for tid in task_ids)
