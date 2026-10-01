"""
Structured JSON logging for the RetailNexus Data Lakehouse.

Uses structlog for consistent, machine-parseable log output across all
components: Spark jobs, Airflow operators, FastAPI endpoints, and scripts.

Usage:
    from src.common.logging import get_logger

    logger = get_logger(__name__)
    logger.info("records_ingested", count=10_000, source="orders", layer="bronze")

Each log line is a JSON object containing:
    - timestamp (ISO8601)
    - level (INFO / WARNING / ERROR / CRITICAL)
    - logger (module name)
    - event (the message string)
    - + any keyword arguments passed to the log call

In production, this JSON is shipped to Elasticsearch via Logstash/Filebeat.
"""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from structlog.types import EventDict, Processor


def _add_service_context(
    logger: Any,
    method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """Inject service-level metadata into every log record."""
    event_dict.setdefault("service", "lakehouse")
    event_dict.setdefault("environment", _get_environment())
    return event_dict


def _get_environment() -> str:
    import os

    return os.getenv("ENVIRONMENT", "dev")


def _drop_color_message_key(
    logger: Any,
    method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """Remove uvicorn's color_message key to keep logs clean."""
    event_dict.pop("color_message", None)
    return event_dict


def setup_logging(
    level: str = "INFO",
    json_output: bool | None = None,
    service_name: str = "lakehouse",
) -> None:
    """
    Configure structlog + standard library logging.

    Call once at application startup (e.g., in FastAPI lifespan,
    Airflow DAG module-level, or Spark job main()).

    Args:
        level: Minimum log level (DEBUG/INFO/WARNING/ERROR/CRITICAL).
        json_output: Force JSON output. Defaults to True in non-dev environments.
        service_name: Service identifier stamped on every log line.
    """
    environment = _get_environment()
    if json_output is None:
        json_output = environment != "dev"

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        _add_service_context,
        _drop_color_message_key,
    ]

    if json_output:
        # Production: emit each log as a single JSON line
        renderer: Processor = structlog.processors.JSONRenderer()
    else:
        # Development: emit colourful, human-readable console output
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level.upper())

    # Silence noisy third-party loggers
    for noisy_logger in (
        "botocore",
        "boto3",
        "urllib3",
        "s3transfer",
        "py4j",
        "pyspark",
    ):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)


def get_logger(name: str | None = None, **initial_context: Any) -> structlog.stdlib.BoundLogger:
    """
    Return a structlog BoundLogger for the given module name.

    Args:
        name: Logger name, typically __name__.
        **initial_context: Key-value pairs bound to every log call from this logger.

    Example:
        logger = get_logger(__name__, job="orders_ingestion", layer="bronze")
        logger.info("started")
        # → {"event": "started", "logger": "...", "job": "orders_ingestion", ...}
    """
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    if initial_context:
        logger = logger.bind(**initial_context)
    return logger


def bind_context(**context: Any) -> None:
    """
    Bind key-value pairs to the current execution context.

    Context is automatically included in all subsequent log calls within the
    same async context / thread. Use in Airflow tasks, FastAPI request handlers,
    or Spark job phases to add pipeline_run_id, dag_id, task_id, etc.

    Example:
        bind_context(pipeline_run_id="run-2026-07-03", layer="bronze")
    """
    structlog.contextvars.bind_contextvars(**context)


def clear_context() -> None:
    """Clear all bound context variables (call at end of request/task)."""
    structlog.contextvars.clear_contextvars()
