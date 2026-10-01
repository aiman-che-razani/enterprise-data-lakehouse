"""
RetailNexus Data Platform API

FastAPI application entry point.

Features:
  - API key auth (X-API-Key header → SHA-256 hash → api.api_keys lookup)
  - Pipeline status + watermark endpoints
  - Data quality check/report endpoints
  - Data catalog endpoints
  - Prometheus metrics via prometheus-fastapi-instrumentator
  - Structured JSON logging (structlog)
  - CORS configured for internal BI tooling origins
  - Custom pipeline Prometheus gauges refreshed on a background schedule

Start:
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4

  or via the Makefile:
    make api-up
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import Gauge
from prometheus_fastapi_instrumentator import Instrumentator

from api.ratelimit import RateLimiter, caller_id, is_exempt
from src.common.logging import configure_logging, get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

logger = get_logger(__name__)

try:
    from src.common.config import Settings as _Settings

    _IS_PROD = _Settings().environment == "prod"  # interactive docs are not exposed in production
except Exception:  # pragma: no cover - misconfiguration surfaces at first real request
    _IS_PROD = False

# ─────────────────────────────────────────────────────────────────────────────
# Custom pipeline Prometheus metrics (updated by a background refresh)
# ─────────────────────────────────────────────────────────────────────────────
_PIPELINE_FRESHNESS = Gauge(
    "retailnexus_pipeline_bronze_freshness_hours",
    "Hours since last Bronze ingest per domain",
    ["domain"],
)
_DQ_PASS_RATE = Gauge(
    "retailnexus_dq_pass_rate",
    "DQ check pass rate by layer (rolling 24h)",
    ["layer"],
)
_DQ_DOMAIN_PASS_RATE = Gauge(
    "retailnexus_dq_domain_pass_rate",
    "Latest DQ pass rate per layer and domain",
    ["layer", "domain"],
)
_DQ_DOMAIN_FAILED = Gauge(
    "retailnexus_dq_domain_failed_checks",
    "Failed DQ checks in the latest run per layer and domain",
    ["layer", "domain"],
)
_SLA_MISSES = Gauge(
    "retailnexus_sla_misses_24h",
    "Count of SLA miss events in the last 24 hours",
)


def _refresh_pipeline_metrics() -> None:
    """Query pipeline DB and update custom Prometheus gauges."""
    try:
        import psycopg2

        from src.common.config import Settings

        conn = psycopg2.connect(Settings().postgres_dsn)
        try:
            _read_and_set_gauges(conn)
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("pipeline_metrics_refresh_failed", error=str(exc))


def _read_and_set_gauges(conn: Any) -> None:
    """Read pipeline/quality state and publish it on the custom gauges."""
    with conn.cursor() as cur:
        # Freshness per domain
        cur.execute(
            """
            SELECT domain,
                   EXTRACT(EPOCH FROM (NOW() - bronze_last_ingest)) / 3600
            FROM pipeline.watermarks
            WHERE bronze_last_ingest IS NOT NULL
            """
        )
        for domain, age_hours in cur.fetchall():
            _PIPELINE_FRESHNESS.labels(domain=domain).set(float(age_hours))

        # DQ pass rates
        cur.execute(
            """
            SELECT layer, AVG(pass_rate)
            FROM quality.reports
            WHERE run_at >= NOW() - INTERVAL '24 hours'
            GROUP BY layer
            """
        )
        for layer, rate in cur.fetchall():
            _DQ_PASS_RATE.labels(layer=layer).set(float(rate))

        # Latest report per (layer, domain) — drives the per-domain DQ dashboard
        cur.execute(
            """
            SELECT DISTINCT ON (layer, domain) layer, domain, pass_rate, failed
            FROM quality.reports
            ORDER BY layer, domain, run_at DESC
            """
        )
        for layer, domain, rate, failed in cur.fetchall():
            _DQ_DOMAIN_PASS_RATE.labels(layer=layer, domain=domain).set(float(rate))
            _DQ_DOMAIN_FAILED.labels(layer=layer, domain=domain).set(int(failed))

        # SLA miss count
        cur.execute(
            "SELECT COUNT(*) FROM pipeline.sla_misses "
            "WHERE detected_at >= NOW() - INTERVAL '24 hours'"
        )
        _SLA_MISSES.set(cur.fetchone()[0])


# ─────────────────────────────────────────────────────────────────────────────
# Lifespan — runs once on startup and shutdown
# ─────────────────────────────────────────────────────────────────────────────
_METRICS_REFRESH_SECONDS = 60


async def _metrics_refresh_loop() -> None:
    """Keep the pipeline gauges current; the DB query runs off the event loop."""
    while True:
        await asyncio.sleep(_METRICS_REFRESH_SECONDS)
        await asyncio.to_thread(_refresh_pipeline_metrics)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    logger.info("retailnexus_api_starting", version="1.0.0")
    _refresh_pipeline_metrics()  # initial metrics load
    refresher = asyncio.create_task(_metrics_refresh_loop())
    yield
    refresher.cancel()
    logger.info("retailnexus_api_stopping")


# ─────────────────────────────────────────────────────────────────────────────
# Application
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="RetailNexus Data Platform API",
    description=(
        "Operational API for the RetailNexus Enterprise Data Lakehouse. "
        "Exposes pipeline watermarks, data quality results, and catalog metadata."
    ),
    version="1.0.0",
    docs_url=None if _IS_PROD else "/docs",
    redoc_url=None if _IS_PROD else "/redoc",
    openapi_url=None if _IS_PROD else "/openapi.json",
    lifespan=lifespan,
)

# Prometheus HTTP instrumentation (exposes /metrics endpoint)
Instrumentator(
    should_group_status_codes=True,
    should_ignore_untemplated=True,
    # Always on: with the env-var gate (ENABLE_METRICS, never set anywhere) /metrics returned
    # 404, so Prometheus scraped nothing and every alert/dashboard built on it stayed empty.
    should_respect_env_var=False,
    should_instrument_requests_inprogress=True,
    excluded_handlers=["/health", "/metrics"],
    inprogress_name="retailnexus_api_inprogress",
    inprogress_labels=True,
).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

# CORS — allow BI tools and internal dashboards
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # local dev
        "http://grafana:3000",  # Docker Compose Grafana
    ],
    # Starlette does not expand "*" inside allow_origins entries; use a regex for subdomains.
    allow_origin_regex=r"https://[a-z0-9-]+\.retailnexus\.internal",
    allow_credentials=False,  # auth is a header key, not cookies
    allow_methods=["GET"],
    allow_headers=["X-API-Key"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Rate limiting (registered before the timing middleware, so it runs *inside* it and 429s are logged)
# ─────────────────────────────────────────────────────────────────────────────
try:
    _rate_limiter = RateLimiter(_Settings().api_rate_limit_per_minute)
except Exception:  # pragma: no cover - misconfiguration surfaces at first real request
    _rate_limiter = RateLimiter(300)


@app.middleware("http")
async def rate_limit(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    if is_exempt(request.url.path) or request.method == "OPTIONS":
        return await call_next(request)
    caller = caller_id(
        request.headers.get("x-api-key"), request.client.host if request.client else None
    )
    allowed, retry_after = _rate_limiter.check(caller)
    if not allowed:
        logger.warning("rate_limited", caller=caller, path=request.url.path)
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "Rate limit exceeded"},
            headers={"Retry-After": str(max(1, int(retry_after + 0.999)))},
        )
    return await call_next(request)


# ─────────────────────────────────────────────────────────────────────────────
# Request timing middleware
# ─────────────────────────────────────────────────────────────────────────────
@app.middleware("http")
async def log_requests(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = round((time.perf_counter() - start) * 1000, 1)
    logger.info(
        "http_request",
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=elapsed,
    )
    return response


# ─────────────────────────────────────────────────────────────────────────────
# Global exception handler
# ─────────────────────────────────────────────────────────────────────────────
@app.exception_handler(Exception)
async def unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
    logger.error(
        "unhandled_exception",
        path=request.url.path,
        error=str(exc),
        exc_type=type(exc).__name__,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error — check API logs"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Routers
# ─────────────────────────────────────────────────────────────────────────────
from api.routers import catalog, health, pipeline, quality  # noqa: E402

app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(pipeline.router, prefix="/pipeline", tags=["pipeline"])
app.include_router(quality.router, prefix="/quality", tags=["quality"])
app.include_router(catalog.router, prefix="/catalog", tags=["catalog"])


# ─────────────────────────────────────────────────────────────────────────────
# Root redirect
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
def root() -> dict[str, Any]:
    return {
        "service": "RetailNexus Data Platform API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "metrics": "/metrics",
    }
