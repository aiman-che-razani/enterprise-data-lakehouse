"""
DagBag integration test — runs only where the real Apache Airflow is installed (the Airflow
container / CI image). Every DAG must import, and every task's Spark application must exist.

    AIRFLOW__CORE__UNIT_TEST_MODE=True pytest tests/integration/airflow -m integration
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_ROOT = Path(__file__).resolve().parents[3]
_DAGS = _ROOT / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    models = pytest.importorskip("airflow.models", reason="Apache Airflow not installed")
    if not hasattr(models, "DagBag"):
        pytest.skip("airflow package is shadowed by the repo's airflow/ directory")
    return models.DagBag(dag_folder=str(_DAGS), include_examples=False)


def test_all_dags_import_cleanly(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_expected_dags_are_registered(dagbag):
    expected = {
        "bronze_batch_ingestion",
        "bronze_streaming_health",
        "silver_batch",
        "gold_batch",
        "warehouse_load",
        "data_quality",
        "pipeline_maintenance",
        "sla_monitor",
        "pipeline_backfill",
        "pipeline_e2e",
    }
    assert expected <= set(
        dagbag.dag_ids
    ), f"missing DAGs: {sorted(expected - set(dagbag.dag_ids))}"


def test_spark_applications_exist_on_disk(dagbag):
    missing = []
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            app = getattr(task, "_application", None) or getattr(task, "application", None)
            if app and not (_ROOT / app).is_file():
                missing.append(f"{dag.dag_id}.{task.task_id} -> {app}")
    assert not missing, missing


def test_no_dag_lets_runs_overlap_unbounded(dagbag):
    for dag in dagbag.dags.values():
        assert (
            dag.max_active_runs <= 16
        ), f"{dag.dag_id} allows {dag.max_active_runs} concurrent runs"


def test_maintenance_dag_includes_catalog_sync(dagbag):
    dag = dagbag.get_dag("pipeline_maintenance")
    assert {
        "catalog_sync",
        "gold_optimize_vacuum",
        "silver_optimize_vacuum",
        "bronze_optimize_vacuum",
    } <= {t.task_id for t in dag.tasks}
