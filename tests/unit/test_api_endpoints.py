"""
FastAPI endpoint tests using TestClient (no live DB required).

All DB calls are mocked via FastAPI's dependency_overrides mechanism.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_db, verify_api_key
from api.main import app
from api.models.pipeline import PipelineStatusResponse, SlaMissResponse, WatermarkResponse
from api.models.quality import PaginatedChecks, QualitySummaryResponse

# ─────────────────────────────────────────────────────────────────────────────
# Test fixtures
# ─────────────────────────────────────────────────────────────────────────────
_NOW = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
_FAKE_AUTH = MagicMock()
_FAKE_AUTH.key_id = 1
_FAKE_AUTH.client_name = "test-client"
_FAKE_AUTH.permissions = ["read"]


def _override_auth():
    """FastAPI dep override — skip real DB key lookup."""
    from api.dependencies import ApiKeyContext

    return ApiKeyContext(key_id=1, client_name="test", permissions=["*"])


def _override_db():
    """FastAPI dep override — yield a mock psycopg2 connection."""
    yield MagicMock()


@pytest.fixture(scope="module")
def client():
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[verify_api_key] = _override_auth
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


_HEADERS = {"X-API-Key": "test-key"}


# ─────────────────────────────────────────────────────────────────────────────
# Health endpoints (no auth)
# ─────────────────────────────────────────────────────────────────────────────
class TestHealthEndpoints:
    def test_liveness_returns_200(self, client):
        resp = client.get("/health/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "service" in data
        assert "version" in data

    def test_readiness_returns_200_on_db_ok(self, client):
        resp = client.get("/health/ready")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ready"

    def test_startup_returns_python_version(self, client):
        resp = client.get("/health/startup")
        assert resp.status_code == 200
        assert "python" in resp.json()

    def test_root_returns_service_info(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert "docs" in data
        assert "health" in data
        assert "metrics" in data


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline endpoints
# ─────────────────────────────────────────────────────────────────────────────
class TestPipelineEndpoints:
    def test_status_returns_200(self, client):
        with patch("api.routers.pipeline.PipelineService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.get_pipeline_status.return_value = PipelineStatusResponse(
                as_of=_NOW,
                total_domains=16,
                domains_ok=16,
                domains_warn=0,
                sla_miss_count_24h=0,
                domains=[],
            )
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/pipeline/status", headers=_HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_domains"] == 16
        assert data["domains_ok"] == 16

    def test_watermarks_returns_list(self, client):
        with patch("api.routers.pipeline.PipelineService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.list_watermarks.return_value = [
                WatermarkResponse(domain="orders", bronze_last_ingest=_NOW),
                WatermarkResponse(domain="payments"),
            ]
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/pipeline/watermarks", headers=_HEADERS)
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_watermark_single_domain(self, client):
        with patch("api.routers.pipeline.PipelineService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.get_watermark.return_value = WatermarkResponse(
                domain="orders", bronze_last_ingest=_NOW
            )
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/pipeline/watermarks/orders", headers=_HEADERS)
        assert resp.status_code == 200
        assert resp.json()["domain"] == "orders"

    def test_watermark_unknown_domain_returns_404(self, client):
        with patch("api.routers.pipeline.PipelineService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.get_watermark.return_value = None
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/pipeline/watermarks/nonexistent", headers=_HEADERS)
        assert resp.status_code == 404

    def test_sla_misses_returns_list(self, client):
        with patch("api.routers.pipeline.PipelineService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.list_sla_misses.return_value = [
                SlaMissResponse(
                    id=1,
                    run_id="abc",
                    domain="orders",
                    layer="bronze",
                    metric="freshness",
                    message="stale",
                    detected_at=_NOW,
                )
            ]
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/pipeline/sla-misses", headers=_HEADERS)
        assert resp.status_code == 200
        assert resp.json()[0]["domain"] == "orders"

    def test_sla_misses_hours_param(self, client):
        with patch("api.routers.pipeline.PipelineService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.list_sla_misses.return_value = []
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/pipeline/sla-misses?hours=48", headers=_HEADERS)
        assert resp.status_code == 200

    def test_pipeline_requires_auth(self):
        # Drop the auth override (keep the mocked DB) so the real key check runs.
        saved = dict(app.dependency_overrides)
        app.dependency_overrides.pop(verify_api_key, None)

        def _db_without_keys():
            conn = MagicMock()
            cur = conn.cursor.return_value.__enter__.return_value
            cur.fetchone.return_value = None  # no api_keys row for any hash
            yield conn

        app.dependency_overrides[get_db] = _db_without_keys
        try:
            client_no_auth = TestClient(app, raise_server_exceptions=False)
            resp = client_no_auth.get("/pipeline/status")  # no X-API-Key header
            assert resp.status_code in (401, 422)  # 422 = missing header
            resp = client_no_auth.get("/pipeline/status", headers={"X-API-Key": "wrong"})
            assert resp.status_code == 401  # unknown key hash → no row
        finally:
            app.dependency_overrides.clear()
            app.dependency_overrides.update(saved)


# ─────────────────────────────────────────────────────────────────────────────
# Quality endpoints
# ─────────────────────────────────────────────────────────────────────────────
class TestQualityEndpoints:
    def test_summary_returns_list(self, client):
        with patch("api.routers.quality.QualityService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.summary.return_value = [
                QualitySummaryResponse(
                    layer="bronze",
                    period_hours=24.0,
                    total_checks=100,
                    passed=95,
                    failed=5,
                    warnings=0,
                    errors=0,
                    pass_rate=0.95,
                    domains_checked=16,
                )
            ]
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/quality/summary", headers=_HEADERS)
        assert resp.status_code == 200
        assert resp.json()[0]["layer"] == "bronze"

    def test_checks_returns_paginated(self, client):
        with patch("api.routers.quality.QualityService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.list_checks.return_value = PaginatedChecks(
                total=0, limit=100, offset=0, items=[]
            )
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/quality/checks", headers=_HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert "total" in data
        assert "items" in data
        assert "limit" in data
        assert "offset" in data

    def test_checks_status_filter(self, client):
        with patch("api.routers.quality.QualityService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.list_checks.return_value = PaginatedChecks(
                total=0, limit=100, offset=0, items=[]
            )
            mock_svc_cls.return_value = mock_svc
            resp = client.get(
                "/quality/checks?status=failed&layer=bronze",
                headers=_HEADERS,
            )
        assert resp.status_code == 200
        # Verify status was passed through to service
        call_kwargs = mock_svc.list_checks.call_args.kwargs
        assert call_kwargs.get("status") == "failed"
        assert call_kwargs.get("layer") == "bronze"

    def test_trend_returns_list(self, client):
        with patch("api.routers.quality.QualityService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.trend.return_value = []
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/quality/trend?hours=48", headers=_HEADERS)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_reports_hours_default_24(self, client):
        with patch("api.routers.quality.QualityService") as mock_svc_cls:
            mock_svc = MagicMock()
            mock_svc.list_reports.return_value = []
            mock_svc_cls.return_value = mock_svc
            resp = client.get("/quality/reports", headers=_HEADERS)
        assert resp.status_code == 200
        call_kwargs = mock_svc.list_reports.call_args.kwargs
        assert call_kwargs.get("hours") == 24.0


# ─────────────────────────────────────────────────────────────────────────────
# Catalog endpoints
# ─────────────────────────────────────────────────────────────────────────────
class TestCatalogEndpoints:
    def test_tables_returns_200(self, client):
        row = {
            "table_name": "silver.orders",
            "layer": "silver",
            "domain": "orders",
            "location": None,
            "format": "delta",
            "row_count": None,
            "size_bytes": None,
            "registered_at": None,
            "last_updated": None,
            "description": None,
        }
        with patch("api.routers.catalog.CatalogRepository") as repo_cls:
            repo_cls.return_value.list_tables.return_value = [row]
            resp = client.get("/catalog/tables", headers=_HEADERS)
        assert resp.status_code == 200
        assert resp.json()[0]["table_name"] == "silver.orders"
        assert resp.json()[0]["location"] is None  # s3_location is nullable in the DDL

    def test_unknown_table_returns_404(self, client):
        # Override DB to return no row for catalog.tables
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.__enter__ = MagicMock(return_value=mock_cur)
        mock_cur.__exit__ = MagicMock(return_value=False)
        mock_cur.fetchone = MagicMock(return_value=None)
        mock_conn.cursor = MagicMock(return_value=mock_cur)

        def _override_db_none():
            yield mock_conn

        app.dependency_overrides[get_db] = _override_db_none
        resp = client.get("/catalog/tables/does_not_exist", headers=_HEADERS)
        app.dependency_overrides[get_db] = _override_db
        assert resp.status_code == 404
