"""
Retry utilities for the RetailNexus Data Lakehouse.

Wraps tenacity with lakehouse-aware defaults and structured logging.
Used for transient failures: network calls, S3 operations, DB connections,
Kafka producer/consumer errors.

Usage:
    from src.common.utils.retry import retry_with_backoff, RetryConfig

    @retry_with_backoff()
    def read_from_s3(path: str) -> bytes: ...

    # Custom config
    @retry_with_backoff(RetryConfig(max_attempts=5, wait_min=2, wait_max=30))
    def call_external_api() -> dict: ...
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random_exponential,
)

from src.common.logging import get_logger

logger = get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


@dataclass
class RetryConfig:
    """
    Configuration for retry behaviour.

    Attributes:
        max_attempts: Total number of attempts (including the first try).
        wait_min: Minimum wait time in seconds between retries.
        wait_max: Maximum wait time in seconds between retries.
        jitter: If True, adds random jitter to prevent thundering-herd.
        retriable_exceptions: Exception types that trigger a retry.
            Defaults to Exception (retry on anything).
    """

    max_attempts: int = 3
    wait_min: float = 1.0
    wait_max: float = 60.0
    jitter: bool = True
    retriable_exceptions: tuple[type[Exception], ...] = field(default_factory=lambda: (Exception,))


_DEFAULT_CONFIG = RetryConfig()


def _make_before_sleep_log(operation_name: str) -> Callable[[RetryCallState], None]:
    """Return a tenacity before_sleep callback that logs retry attempts."""

    def _log(retry_state: RetryCallState) -> None:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        logger.warning(
            "retrying_operation",
            operation=operation_name,
            attempt=retry_state.attempt_number,
            exception=str(exc),
            next_wait_s=(
                round(retry_state.next_action.sleep, 2) if retry_state.next_action else None
            ),
        )

    return _log


def _make_after_log(operation_name: str) -> Callable[[RetryCallState], None]:
    """Return a tenacity after callback that logs success after retry."""

    def _log(retry_state: RetryCallState) -> None:
        if retry_state.attempt_number > 1:
            logger.info(
                "operation_succeeded_after_retry",
                operation=operation_name,
                attempts=retry_state.attempt_number,
            )

    return _log


def retry_with_backoff(
    config: RetryConfig | None = None,
    operation_name: str | None = None,
) -> Callable[[F], F]:
    """
    Decorator factory that applies exponential-backoff retry logic.

    Args:
        config: RetryConfig instance. Uses _DEFAULT_CONFIG if None.
        operation_name: Human-readable name for log messages.
            Defaults to the decorated function's __qualname__.

    Returns:
        Decorator that wraps the function with retry logic.

    Example:
        @retry_with_backoff(RetryConfig(max_attempts=5))
        def ingest_file(path: str) -> int: ...
    """
    cfg = config or _DEFAULT_CONFIG

    def decorator(func: F) -> F:
        name = operation_name or func.__qualname__

        wait_strategy = (
            wait_random_exponential(min=cfg.wait_min, max=cfg.wait_max)
            if cfg.jitter
            else wait_exponential(min=cfg.wait_min, max=cfg.wait_max)
        )

        @retry(
            stop=stop_after_attempt(cfg.max_attempts),
            wait=wait_strategy,
            retry=retry_if_exception_type(cfg.retriable_exceptions),
            before_sleep=_make_before_sleep_log(name),
            after=_make_after_log(name),
            reraise=True,
        )
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


# ─── Pre-configured decorators for common scenarios ───────────────────────────


def retry_s3(func: F) -> F:
    """Retry decorator tuned for S3/MinIO operations (3 attempts, 2–30s)."""
    return retry_with_backoff(
        RetryConfig(max_attempts=3, wait_min=2, wait_max=30),
        operation_name=f"s3:{func.__qualname__}",
    )(func)


def retry_db(func: F) -> F:
    """Retry decorator tuned for database operations (5 attempts, 1–20s)."""
    return retry_with_backoff(
        RetryConfig(max_attempts=5, wait_min=1, wait_max=20),
        operation_name=f"db:{func.__qualname__}",
    )(func)


def retry_kafka(func: F) -> F:
    """Retry decorator tuned for Kafka produce/consume operations (4 attempts, 1–15s)."""
    return retry_with_backoff(
        RetryConfig(max_attempts=4, wait_min=1, wait_max=15),
        operation_name=f"kafka:{func.__qualname__}",
    )(func)
