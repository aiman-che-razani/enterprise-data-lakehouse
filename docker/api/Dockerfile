# ═══════════════════════════════════════════════════════════════════════════════
# RetailNexus FastAPI Service Image
# Multi-stage: builder (compile deps) + runtime (slim final image)
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies into a prefix for copying
COPY pyproject.toml .
RUN pip install --no-cache-dir --prefix=/install \
    "fastapi>=0.135.0,<1.0.0" \
    "starlette>=1.3.1" \
    "uvicorn[standard]>=0.30.0" \
    "sqlalchemy>=2.0.30" \
    "asyncpg>=0.29.0" \
    "psycopg2-binary>=2.9.9" \
    "httpx>=0.27.0" \
    "slowapi>=0.1.9" \
    "pydantic>=2.7.0" \
    "pydantic-settings>=2.3.0" \
    "structlog>=24.2.0" \
    "boto3>=1.34.0" \
    "prometheus-client>=0.20.0" \
    "prometheus-fastapi-instrumentator>=6.1.0" \
    "tenacity>=8.3.0" \
    "python-dotenv>=1.0.0"

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Runtime system deps only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --shell /bin/bash --create-home appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY --chown=appuser:appgroup api/ ./api/
COPY --chown=appuser:appgroup src/ ./src/

ENV PYTHONPATH=/app/src:/app:${PYTHONPATH}
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

USER appuser

EXPOSE 8000

# Health check
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8000/health || exit 1

# Entrypoint — uvicorn with hot reload in dev, multiple workers in prod
CMD ["uvicorn", "api.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--no-access-log"]

LABEL org.opencontainers.image.title="RetailNexus FastAPI"
LABEL org.opencontainers.image.description="REST API service over Gold layer data"
LABEL org.opencontainers.image.version="1.0.0"
