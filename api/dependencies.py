"""
FastAPI dependency functions — database connection pool and API key auth.

Auth flow:
  1. Client sends  X-API-Key: <plaintext key>
  2. Middleware SHA-256 hashes it
  3. Hash is looked up in api.api_keys (is_active=true, not expired)
  4. On miss → 401 Unauthorized
  5. On hit  → injects ApiKeyContext into route handlers

Security invariants:
  - Plaintext keys never stored; only SHA-256 hashes in DB
  - API_SECRET_KEY model_validator in Settings rejects the default in prod
  - Connection pool is shared across the process; each request borrows a slot
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Annotated, Any

import psycopg2
import psycopg2.extras
import psycopg2.pool
from fastapi import Depends, Header, HTTPException, status
from pydantic import BaseModel, BeforeValidator

from src.common.config import Settings
from src.common.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

logger = get_logger(__name__)

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        settings = Settings()
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=settings.postgres_dsn,
        )
    return _pool


def get_db() -> Iterator[Any]:
    """Yield a psycopg2 connection from the shared pool.

    Every request ends with a rollback so a failed query never hands an aborted
    transaction to the next caller, and plain SELECTs do not sit idle-in-transaction.
    An unreachable database is a 503 with a generic body (details go to the log only).
    """
    try:
        pool = _get_pool()
        conn = pool.getconn()
    except Exception as exc:
        logger.error("db_pool_unavailable", error=str(exc), exc_type=type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unavailable"
        ) from exc
    try:
        yield conn
    finally:
        try:
            if not conn.closed:
                conn.rollback()
        finally:
            pool.putconn(conn, close=bool(conn.closed))


DbDep = Annotated[object, Depends(get_db)]


# ─────────────────────────────────────────────────────────────────────────────
# API key authentication
# ─────────────────────────────────────────────────────────────────────────────
class ApiKeyContext(BaseModel):
    # api.api_keys.key_id is a UUID; accept ints/UUID objects and normalise to str
    key_id: Annotated[str, BeforeValidator(str)]
    client_name: str
    permissions: list[str]

    def allows(self, scope: str) -> bool:
        """True if the key carries *scope* or the ``*`` wildcard."""
        return "*" in self.permissions or scope in self.permissions


def _hash_key(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode()).hexdigest()


def verify_api_key(
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    conn: Any = Depends(get_db),
) -> ApiKeyContext:
    """Validate X-API-Key header; raise 401 on any failure (a missing header is a 401, not a 422)."""
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    key_hash = _hash_key(x_api_key)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT key_id, client_name, permissions
            FROM   api.api_keys
            WHERE  key_hash  = %s
              AND  is_active  = true
              AND  (expires_at IS NULL OR expires_at > NOW())
            """,
            (key_hash,),
        )
        row = cur.fetchone()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    perms = row["permissions"] or []
    if isinstance(perms, str):
        import json

        perms = json.loads(perms)

    return ApiKeyContext(
        key_id=row["key_id"],
        client_name=row["client_name"],
        permissions=perms,
    )


AuthDep = Annotated[ApiKeyContext, Depends(verify_api_key)]


def require_scope(scope: str) -> Callable[..., ApiKeyContext]:
    """Dependency factory: authenticate, then require the key to carry *scope* (or ``*``)."""

    def _dependency(ctx: ApiKeyContext = Depends(verify_api_key)) -> ApiKeyContext:
        if not ctx.allows(scope):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"API key lacks required permission '{scope}'",
            )
        return ctx

    return _dependency
