"""Hardening regressions: scopes, NULL-safe models, bounds, generic errors, pool hygiene."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import api.dependencies as deps
from api.dependencies import ApiKeyContext, get_db, verify_api_key
from api.main import app
from api.models.pipeline import PipelineRunResponse, SlaMissResponse
from api.models.quality import CheckResultResponse

_H = {"X-API-Key": "k"}


def _ctx(*perms: str) -> ApiKeyContext:
    return ApiKeyContext(key_id=str(uuid.uuid4()), client_name="t", permissions=list(perms))


@pytest.fixture()
def make_client():
    conn = MagicMock()

    def _build(*perms: str) -> TestClient:
        app.dependency_overrides.clear()
        app.dependency_overrides[get_db] = lambda: iter([conn])
        app.dependency_overrides[verify_api_key] = lambda: _ctx(*perms)
        return TestClient(app, raise_server_exceptions=True)

    yield _build
    app.dependency_overrides.clear()


# ─── Scopes ───────────────────────────────────────────────────────────────────
class TestScopes:
    @pytest.mark.parametrize(
        "path,scope",
        [
            ("/pipeline/watermarks", "read:pipeline"),
            ("/quality/summary", "read:quality"),
            ("/catalog/tables", "read:catalog"),
        ],
    )
    def test_key_without_scope_is_403(self, make_client, path, scope):
        other = "read:quality" if scope != "read:quality" else "read:pipeline"
        assert make_client(other).get(path, headers=_H).status_code == 403

    def test_matching_scope_and_wildcard_pass_authorization(self, make_client):
        with patch("api.routers.catalog.CatalogRepository") as repo:
            repo.return_value.list_tables.return_value = []
            assert make_client("read:catalog").get("/catalog/tables", headers=_H).status_code == 200
            assert make_client("*").get("/catalog/tables", headers=_H).status_code == 200

    def test_allows_helper(self):
        assert _ctx("*").allows("read:x") and _ctx("read:x").allows("read:x")
        assert not _ctx("read:y").allows("read:x") and not _ctx().allows("read:x")

    def test_health_needs_no_key(self):
        app.dependency_overrides.clear()
        assert TestClient(app).get("/health/").status_code == 200


# ─── Auth: every protected router answers 401 (not 422) without a key ─────────
@pytest.mark.parametrize(
    "path", ["/pipeline/status", "/quality/summary", "/catalog/tables", "/catalog/lineage/x.y"]
)
def test_missing_key_is_401_on_every_router(path):
    conn = MagicMock()
    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = lambda: iter([conn])
    try:
        resp = TestClient(app).get(path)
    finally:
        app.dependency_overrides.clear()
    assert resp.status_code == 401 and resp.headers["www-authenticate"] == "ApiKey"


# ─── Models accept the NULLs the DDL allows ───────────────────────────────────
class TestNullableModels:
    def test_sla_miss_with_null_run_layer_message(self):
        m = SlaMissResponse(
            id=1,
            run_id=None,
            domain="orders",
            layer=None,
            metric="m",
            message=None,
            detected_at=datetime.now(UTC),
        )
        assert m.run_id is None and m.layer is None and m.message is None

    def test_run_with_null_layer(self):
        assert PipelineRunResponse(id=1, run_id="r", layer=None, status="success").layer is None

    def test_check_result_with_null_message_and_rows(self):
        m = CheckResultResponse(
            id=1,
            run_id="r",
            domain="d",
            layer="bronze",
            check_name="c",
            status="passed",
            message=None,
            rows_checked=None,
            run_at=datetime.now(UTC),
        )
        assert m.message is None and m.rows_checked is None


# ─── Bounds / enums → 422 ─────────────────────────────────────────────────────
class TestValidation:
    @pytest.mark.parametrize(
        "url",
        [
            "/pipeline/sla-misses?limit=0",
            "/pipeline/sla-misses?limit=1001",
            "/pipeline/sla-misses?hours=0",
            "/pipeline/sla-misses?hours=721",
            "/pipeline/sla-misses?layer=platinum",
            "/pipeline/runs?limit=0",
            "/pipeline/runs?limit=501",
            "/pipeline/runs?layer=x",
            "/quality/checks?limit=0",
            "/quality/checks?limit=501",
            "/quality/checks?status=weird",
            "/quality/checks?layer=x",
            "/quality/trend?limit=0",
            "/quality/trend?hours=721",
            "/quality/reports?limit=1001",
            "/catalog/tables?limit=0",
            "/catalog/tables?limit=501",
            "/catalog/tables?layer=platinum",
            "/catalog/lineage/a.b?direction=sideways",
        ],
    )
    def test_out_of_bounds_is_422(self, make_client, url):
        assert make_client("*").get(url, headers=_H).status_code == 422

    def test_limits_reach_the_repository(self, make_client):
        with patch("api.routers.pipeline.PipelineService") as svc:
            svc.return_value.list_sla_misses.return_value = []
            make_client("*").get("/pipeline/sla-misses?limit=7", headers=_H)
        assert svc.return_value.list_sla_misses.call_args.kwargs["limit"] == 7

    def test_status_enum_is_converted_for_the_repository(self, make_client):
        with patch("api.routers.quality.QualityService") as svc:
            svc.return_value.list_checks.return_value = {
                "total": 0,
                "limit": 100,
                "offset": 0,
                "items": [],
            }
            make_client("*").get("/quality/checks?status=failed", headers=_H)
        assert svc.return_value.list_checks.call_args.kwargs["status"] == "failed"


# ─── Catalog behaviour ────────────────────────────────────────────────────────
class TestCatalog:
    def test_columns_of_unknown_table_is_404(self, make_client):
        with patch("api.routers.catalog.CatalogRepository") as repo:
            repo.return_value.table_exists.return_value = False
            resp = make_client("*").get("/catalog/columns/silver.nope", headers=_H)
        assert resp.status_code == 404 and "silver.nope" in resp.json()["detail"]

    def test_columns_of_known_table(self, make_client):
        with patch("api.routers.catalog.CatalogRepository") as repo:
            repo.return_value.table_exists.return_value = True
            repo.return_value.list_columns.return_value = [
                {
                    "column_name": "a",
                    "data_type": "string",
                    "is_nullable": False,
                    "is_pii": False,
                    "description": None,
                    "ordinal_position": 1,
                }
            ]
            resp = make_client("*").get("/catalog/columns/silver.orders", headers=_H)
        assert resp.status_code == 200 and resp.json()[0]["column_name"] == "a"


# ─── Health: no internals in the body ─────────────────────────────────────────
def test_readiness_failure_body_is_generic():
    bad = MagicMock()
    bad.cursor.side_effect = RuntimeError('connection to server at "10.0.0.5" user "svc" failed')
    app.dependency_overrides[get_db] = lambda: iter([bad])
    try:
        resp = TestClient(app).get("/health/ready")
    finally:
        app.dependency_overrides.clear()
    assert resp.status_code == 503
    assert resp.json() == {"status": "not_ready", "db": "unavailable"}
    assert "10.0.0.5" not in resp.text and "svc" not in resp.text


def test_unreachable_pool_is_503_with_generic_detail(monkeypatch):
    monkeypatch.setattr(
        deps, "_get_pool", MagicMock(side_effect=RuntimeError("password=hunter2 host=db"))
    )
    gen = deps.get_db()
    with pytest.raises(deps.HTTPException) as exc:
        next(gen)
    assert exc.value.status_code == 503 and exc.value.detail == "Database unavailable"


# ─── Connection pool hygiene ──────────────────────────────────────────────────
class TestPool:
    def _pool(self, monkeypatch, closed=0):
        pool, conn = MagicMock(), MagicMock(closed=closed)
        pool.getconn.return_value = conn
        monkeypatch.setattr(deps, "_get_pool", lambda: pool)
        return pool, conn

    def test_rolls_back_before_returning_to_pool(self, monkeypatch):
        pool, conn = self._pool(monkeypatch)
        gen = deps.get_db()
        assert next(gen) is conn
        gen.close()
        conn.rollback.assert_called_once()
        pool.putconn.assert_called_once_with(conn, close=False)

    def test_rolls_back_after_a_failed_request(self, monkeypatch):
        pool, conn = self._pool(monkeypatch)
        gen = deps.get_db()
        next(gen)
        with pytest.raises(ValueError):
            gen.throw(ValueError("query failed"))
        conn.rollback.assert_called_once()
        pool.putconn.assert_called_once()

    def test_closed_connections_are_discarded(self, monkeypatch):
        pool, conn = self._pool(monkeypatch, closed=2)
        gen = deps.get_db()
        next(gen)
        gen.close()
        conn.rollback.assert_not_called()
        pool.putconn.assert_called_once_with(conn, close=True)


# ─── Metrics refresh never leaks its connection ───────────────────────────────
def test_metrics_refresh_closes_connection_when_a_query_fails(monkeypatch):
    import api.main as api_main

    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value.execute.side_effect = RuntimeError("boom")
    monkeypatch.setattr("psycopg2.connect", MagicMock(return_value=conn))
    api_main._refresh_pipeline_metrics()
    conn.close.assert_called_once()


# ─── CORS ─────────────────────────────────────────────────────────────────────
class TestCors:
    def _preflight(self, origin):
        return TestClient(app).options(
            "/health/", headers={"Origin": origin, "Access-Control-Request-Method": "GET"}
        )

    def test_internal_subdomain_allowed(self):
        assert (
            self._preflight("https://bi.retailnexus.internal").headers.get(
                "access-control-allow-origin"
            )
            == "https://bi.retailnexus.internal"
        )

    def test_other_origins_and_credentials_not_allowed(self):
        r = self._preflight("https://evil.example.com")
        assert "access-control-allow-origin" not in r.headers
        assert (
            self._preflight("http://localhost:3000").headers.get("access-control-allow-credentials")
            is None
        )


class TestModelsAreRuntimeComplete:
    """Ruff's TCH autofix once moved `from datetime import datetime` under TYPE_CHECKING in the response
    models; Pydantic needs it at runtime, so every endpoint returning them answered 500."""

    def test_every_pydantic_model_in_api_is_fully_defined(self):
        import importlib
        import inspect

        from pydantic import BaseModel

        for module_name in (
            "api.models.pipeline",
            "api.models.quality",
            "api.models.catalog",
            "api.dependencies",
        ):
            module = importlib.import_module(module_name)
            for _, cls in inspect.getmembers(module, inspect.isclass):
                if issubclass(cls, BaseModel) and cls.__module__ == module_name:
                    assert (
                        cls.__pydantic_complete__
                    ), f"{module_name}.{cls.__name__} is not fully defined"

    def test_openapi_schema_builds(self):
        app.openapi_schema = None
        schema = app.openapi()
        assert (
            "/pipeline/status" in schema["paths"]
            and "/catalog/lineage/{table_name}" in schema["paths"]
        )

    def test_ruff_is_told_pydantic_needs_runtime_imports(self):
        from pathlib import Path

        text = (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text(encoding="utf-8")
        assert "runtime-evaluated-base-classes" in text and "pydantic.BaseModel" in text
