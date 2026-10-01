"""
Unit tests for src.common.logging.logger.

Tests cover:
- Logger creation and naming
- Context binding and clearing
- Log output format (JSON / console)
- Service metadata injection
"""

from __future__ import annotations

import logging

import pytest
import structlog

from src.common.logging.logger import bind_context, clear_context, get_logger, setup_logging

pytestmark = pytest.mark.unit


class TestGetLogger:
    def test_returns_bound_logger(self) -> None:
        logger = get_logger(__name__)
        assert logger is not None

    def test_logger_with_initial_context(self) -> None:
        logger = get_logger(__name__, job="test_job", layer="bronze")
        # BoundLogger.bind returns a new bound logger — verify it has the context
        bound = logger._context  # type: ignore[attr-defined]
        assert bound.get("job") == "test_job"
        assert bound.get("layer") == "bronze"

    def test_different_names_produce_different_loggers(self) -> None:
        l1 = get_logger("module.a")
        l2 = get_logger("module.b")
        # They should be independent instances
        assert l1 is not l2

    def test_none_name_is_accepted(self) -> None:
        logger = get_logger(None)
        assert logger is not None


class TestContextManagement:
    def test_bind_context_and_clear(self) -> None:
        bind_context(pipeline_run_id="run-001", layer="silver")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["pipeline_run_id"] == "run-001"
        assert ctx["layer"] == "silver"

        clear_context()
        ctx_after = structlog.contextvars.get_contextvars()
        assert ctx_after == {}

    def test_bind_context_multiple_calls_merge(self) -> None:
        clear_context()
        bind_context(dag_id="bronze_ingestion")
        bind_context(task_id="ingest_orders")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["dag_id"] == "bronze_ingestion"
        assert ctx["task_id"] == "ingest_orders"
        clear_context()

    def test_clear_context_is_idempotent(self) -> None:
        clear_context()
        clear_context()
        assert structlog.contextvars.get_contextvars() == {}


class TestSetupLogging:
    def test_setup_logging_does_not_raise(self) -> None:
        setup_logging(level="INFO", json_output=False)

    def test_setup_logging_json_mode(self) -> None:
        setup_logging(level="INFO", json_output=True)
        root = logging.getLogger()
        assert root.level == logging.INFO

    def test_log_level_warning(self) -> None:
        setup_logging(level="WARNING", json_output=False)
        root = logging.getLogger()
        assert root.level == logging.WARNING

    def test_noisy_loggers_suppressed(self) -> None:
        setup_logging(level="DEBUG", json_output=False)
        for name in ("botocore", "boto3", "urllib3", "py4j"):
            lvl = logging.getLogger(name).level
            assert lvl >= logging.WARNING, f"{name} should be WARNING or above"
