"""Health and readiness endpoints — no auth required."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse

from api.dependencies import get_db
from src.common.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(tags=["health"])


@router.get("/", summary="Liveness probe")
def liveness() -> dict[str, str]:
    """Returns 200 immediately — used by Docker health checks and load balancers."""
    return {
        "status": "ok",
        "service": "retailnexus-api",
        "version": "1.0.0",
        "time": datetime.now(UTC).isoformat(),
    }


@router.get("/ready", summary="Readiness probe")
def readiness(conn: Any = Depends(get_db)) -> Any:
    """Returns 200 when the DB is reachable, 503 otherwise.

    The body is deliberately generic — this endpoint is unauthenticated, and driver
    errors contain the host, user and database name. Details go to the log.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return {"status": "ready", "db": "ok"}
    except Exception as exc:
        logger.error("readiness_probe_failed", error=str(exc), exc_type=type(exc).__name__)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "not_ready", "db": "unavailable"},
        )


@router.get("/startup", summary="Startup check")
def startup_info() -> dict[str, str]:
    """Returns service metadata for observability tooling."""
    import platform

    return {
        "service": "retailnexus-api",
        "version": "1.0.0",
        "python": platform.python_version(),
        "platform": platform.platform(),
    }
