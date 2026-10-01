"""`_with_delta_jars`: add the Delta jars for plain Python runs, stay out of the way under spark-submit."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from src.processing.bronze.spark_session import _with_delta_jars


class PackageNotFoundError(Exception):
    """Stand-in for importlib(_)metadata's exception of the same name."""


def _fake_delta(monkeypatch, file: str) -> types.ModuleType:
    mod = types.ModuleType("delta")
    mod.__file__ = file
    mod.configure_spark_with_delta_pip = MagicMock(name="configure_spark_with_delta_pip")
    monkeypatch.setitem(sys.modules, "delta", mod)
    return mod


def test_adds_jars_for_a_plain_python_process(monkeypatch):
    mod = _fake_delta(monkeypatch, "/venv/lib/python3.12/site-packages/delta/__init__.py")
    builder = MagicMock(name="builder")
    assert _with_delta_jars(builder) is mod.configure_spark_with_delta_pip.return_value
    mod.configure_spark_with_delta_pip.assert_called_once_with(builder)


def test_leaves_the_builder_alone_under_spark_submit(monkeypatch):
    """--packages puts the jar on sys.path, so `delta` is imported from inside it."""
    mod = _fake_delta(
        monkeypatch,
        "/home/airflow/.ivy2/jars/io.delta_delta-spark_2.12-3.2.0.jar/delta/__init__.py",
    )
    builder = MagicMock(name="builder")
    assert _with_delta_jars(builder) is builder
    mod.configure_spark_with_delta_pip.assert_not_called()


def test_a_missing_pip_metadata_error_is_tolerated(monkeypatch):
    mod = _fake_delta(monkeypatch, "/somewhere/delta/__init__.py")
    mod.configure_spark_with_delta_pip.side_effect = PackageNotFoundError("delta_spark")
    builder = MagicMock(name="builder")
    assert _with_delta_jars(builder) is builder


def test_other_errors_are_not_swallowed(monkeypatch):
    mod = _fake_delta(monkeypatch, "/somewhere/delta/__init__.py")
    mod.configure_spark_with_delta_pip.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        _with_delta_jars(MagicMock())


def test_without_delta_spark_installed_the_builder_is_returned(monkeypatch):
    monkeypatch.setitem(sys.modules, "delta", None)  # makes `import delta` raise ImportError
    builder = MagicMock(name="builder")
    assert _with_delta_jars(builder) is builder
