"""`run_dq` exit codes: --fail-fast turns the job into a gate that only critical failures can close."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("pyspark")

from spark.jobs.quality import run_dq


def _main(argv, summary=None, error=None):
    runner = MagicMock()
    if error is not None:
        runner.run.side_effect = error
    else:
        runner.run.return_value = summary
    with (
        patch.object(run_dq, "create_bronze_spark_session", return_value=MagicMock()),
        patch.object(run_dq, "DQRunner", return_value=runner),
        patch.object(run_dq, "Settings", return_value=MagicMock()),
        patch.object(run_dq, "configure_logging"),
    ):
        return run_dq.main(argv)


_WITH_FAILURES = {"overall": {"failed": 8, "passed": 161}}
_CLEAN = {"overall": {"failed": 0, "passed": 169}}


def test_clean_run_exits_zero():
    assert _main(["--layer", "silver"], _CLEAN) == 0


def test_failures_exit_one_without_fail_fast():
    assert _main(["--layer", "silver"], _WITH_FAILURES) == 1


def test_non_critical_failures_do_not_close_the_gate():
    assert _main(["--layer", "silver", "--fail-fast"], _WITH_FAILURES) == 0


def test_a_critical_failure_closes_the_gate():
    assert _main(["--layer", "silver", "--fail-fast"], error=RuntimeError("critical")) == 3
