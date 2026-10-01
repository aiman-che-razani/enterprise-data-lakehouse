"""Token-bucket rate limiting: algorithm, identity, and the middleware wired into the app."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import api.main as api_main
from api.dependencies import ApiKeyContext, get_db, verify_api_key
from api.ratelimit import RateLimiter, caller_id, is_exempt


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class TestRateLimiter:
    def test_burst_then_throttle(self):
        rl = RateLimiter(per_minute=60, burst=3, clock=_Clock())
        assert [rl.check("a")[0] for _ in range(4)] == [True, True, True, False]

    def test_retry_after_matches_the_refill_rate(self):
        clock = _Clock()
        rl = RateLimiter(per_minute=60, burst=1, clock=clock)  # 1 token / second
        rl.check("a")
        allowed, retry = rl.check("a")
        assert not allowed and retry == pytest.approx(1.0)

    def test_tokens_refill_over_time_but_never_exceed_capacity(self):
        clock = _Clock()
        rl = RateLimiter(per_minute=60, burst=2, clock=clock)
        rl.check("a"), rl.check("a")
        assert not rl.check("a")[0]
        clock.now += 1.0
        assert rl.check("a")[0] and not rl.check("a")[0]
        clock.now += 3600
        assert [rl.check("a")[0] for _ in range(3)] == [True, True, False]  # capped at burst

    def test_callers_are_independent(self):
        rl = RateLimiter(per_minute=60, burst=1, clock=_Clock())
        assert rl.check("a")[0] and not rl.check("a")[0]
        assert rl.check("b")[0]

    def test_zero_disables(self):
        rl = RateLimiter(per_minute=0)
        assert not rl.enabled and all(rl.check("a")[0] for _ in range(1000))

    def test_negative_rejected(self):
        with pytest.raises(ValueError):
            RateLimiter(per_minute=-1)

    def test_idle_buckets_are_evicted_to_bound_memory(self):
        clock = _Clock()
        rl = RateLimiter(per_minute=60, burst=1, clock=clock)
        for i in range(10_001):
            rl.check(f"c{i}")
        clock.now += 10
        rl.check("c0"), rl.check("c0")  # second call is throttled -> triggers eviction
        assert len(rl._buckets) < 10_001


class TestIdentity:
    def test_key_identity_is_hashed_not_the_key(self):
        cid = caller_id("super-secret", "1.2.3.4")
        assert cid.startswith("key:") and "super-secret" not in cid

    def test_same_key_same_id_and_ip_fallback(self):
        assert caller_id("k", "x") == caller_id("k", "y")
        assert caller_id(None, "1.2.3.4") == "ip:1.2.3.4" and caller_id(None, None) == "ip:unknown"

    @pytest.mark.parametrize(
        "path,exempt",
        [
            ("/health/", True),
            ("/health/ready", True),
            ("/metrics", True),
            ("/pipeline/status", False),
            ("/catalog/tables", False),
        ],
    )
    def test_exempt_paths(self, path, exempt):
        assert is_exempt(path) is exempt


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(
        api_main, "_rate_limiter", RateLimiter(per_minute=60, burst=2, clock=_Clock())
    )
    conn = MagicMock()

    def _db():
        yield conn

    api_main.app.dependency_overrides[get_db] = _db
    api_main.app.dependency_overrides[verify_api_key] = lambda: ApiKeyContext(
        key_id="1", client_name="t", permissions=["*"]
    )
    yield TestClient(api_main.app)
    api_main.app.dependency_overrides.clear()


class TestMiddleware:
    def test_third_request_is_429_with_retry_after(self, client):
        for _ in range(2):
            assert client.get("/pipeline/watermarks", headers={"X-API-Key": "k"}).status_code != 429
        resp = client.get("/pipeline/watermarks", headers={"X-API-Key": "k"})
        assert resp.status_code == 429
        assert resp.json() == {"detail": "Rate limit exceeded"}
        assert int(resp.headers["retry-after"]) >= 1

    def test_health_and_metrics_are_never_limited(self, client):
        for _ in range(20):
            assert client.get("/health/").status_code == 200
        assert client.get("/metrics").status_code == 200

    def test_other_keys_are_unaffected(self, client):
        for _ in range(3):
            client.get("/pipeline/watermarks", headers={"X-API-Key": "noisy"})
        assert client.get("/pipeline/watermarks", headers={"X-API-Key": "quiet"}).status_code != 429

    def test_unauthenticated_floods_are_throttled_by_ip(self, client):
        codes = [client.get("/pipeline/status").status_code for _ in range(5)]
        assert 429 in codes


def test_rate_limit_is_configurable_and_sane_by_default(monkeypatch):
    from src.common.config.settings import Settings

    monkeypatch.delenv("API_RATE_LIMIT_PER_MINUTE", raising=False)

    assert Settings(_env_file=None).api_rate_limit_per_minute == 300
