"""
Per-caller request rate limiting (token bucket, in process).

Callers are identified by a hash of their ``X-API-Key`` (never the key itself), falling back to the
client address for unauthenticated traffic, so one noisy client cannot starve the rest and failed
auth attempts are throttled too. Health probes and ``/metrics`` are exempt.

The buckets live in this process: with N API tasks the effective ceiling is N x the configured rate.
That is the right trade-off for an internal operations API; put an ALB / WAF rate rule in front for a
hard global limit (see docs/deployment/DEPLOYMENT.md).
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

EXEMPT_PREFIXES = ("/health", "/metrics")


@dataclass
class _Bucket:
    tokens: float
    updated: float


class RateLimiter:
    """Token bucket: ``per_minute`` sustained rate, ``burst`` instantaneous capacity."""

    def __init__(
        self, per_minute: int, burst: int | None = None, clock: Callable[[], float] = time.monotonic
    ) -> None:
        if per_minute < 0:
            raise ValueError("per_minute must be >= 0 (0 disables limiting)")
        self.per_minute = per_minute
        self.rate = per_minute / 60.0
        self.capacity = float(burst if burst is not None else max(1, per_minute // 4))
        self._clock = clock
        self._buckets: dict[str, _Bucket] = {}
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self.per_minute > 0

    def check(self, caller: str) -> tuple[bool, float]:
        """Consume one token. Returns ``(allowed, retry_after_seconds)``."""
        if not self.enabled:
            return True, 0.0
        now = self._clock()
        with self._lock:
            b = self._buckets.get(caller)
            if b is None:
                b = self._buckets[caller] = _Bucket(self.capacity, now)
            b.tokens = min(self.capacity, b.tokens + (now - b.updated) * self.rate)
            b.updated = now
            if b.tokens >= 1.0:
                b.tokens -= 1.0
                return True, 0.0
            self._evict(now)
            return False, (1.0 - b.tokens) / self.rate

    def _evict(self, now: float) -> None:
        """Bound memory: drop buckets idle long enough to have fully refilled."""
        if len(self._buckets) < 10_000:
            return
        idle = self.capacity / self.rate
        for key in [k for k, b in self._buckets.items() if now - b.updated > idle]:
            del self._buckets[key]


def caller_id(api_key: str | None, client_host: str | None) -> str:
    """Stable, non-reversible identity for a request."""
    if api_key:
        return "key:" + hashlib.sha256(api_key.encode()).hexdigest()[:16]
    return "ip:" + (client_host or "unknown")


def is_exempt(path: str) -> bool:
    return path.startswith(EXEMPT_PREFIXES)
