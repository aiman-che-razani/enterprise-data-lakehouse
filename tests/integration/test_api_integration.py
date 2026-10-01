"""
API integration tests — requires a live docker-compose stack.

Run with:
    TEST_DATABASE_URL=postgresql://lakehouse:lakehouse@localhost:5432/lakehouse \
    TEST_API_URL=http://localhost:8000 \
    TEST_API_KEY=dev-api-key-retailnexus-2026 \
    pytest tests/integration/test_api_integration.py -m integration -v

All tests skip automatically when the required env vars are absent.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


# ─────────────────────────────────────────────────────────────────────────────
# Health — unauthenticated
# ─────────────────────────────────────────────────────────────────────────────
class TestHealthLive:
    def test_liveness(self, api_client):
        resp = api_client.get("/health/")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_readiness_db_connected(self, api_client):
        resp = api_client.get("/health/ready")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ready"

    def test_startup_platform_info(self, api_client):
        resp = api_client.get("/health/startup")
        assert resp.status_code == 200
        data = resp.json()
        assert "python" in data
        assert "platform" in data

    def test_root_service_links(self, api_client):
        resp = api_client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert "/docs" in data.get("docs", "")
        assert "/metrics" in data.get("metrics", "")


# ─────────────────────────────────────────────────────────────────────────────
# Auth enforcement
# ─────────────────────────────────────────────────────────────────────────────
class TestAuthLive:
    def test_pipeline_without_key_returns_401(self, api_base_url):
        import httpx

        resp = httpx.get(f"{api_base_url}/pipeline/status")
        assert resp.status_code in (401, 422)

    def test_pipeline_with_invalid_key_returns_401(self, api_base_url):
        import httpx

        resp = httpx.get(
            f"{api_base_url}/pipeline/status",
            headers={"X-API-Key": "totally-invalid-key-xyz"},
        )
        assert resp.status_code == 401

    def test_valid_key_returns_200(self, api_client):
        resp = api_client.get("/pipeline/status")
        assert resp.status_code == 200


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline endpoints
# ─────────────────────────────────────────────────────────────────────────────
class TestPipelineEndpointsLive:
    def test_status_schema(self, api_client):
        resp = api_client.get("/pipeline/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_domains" in data
        assert "domains_ok" in data
        assert "sla_miss_count_24h" in data
        assert "domains" in data
        assert isinstance(data["domains"], list)

    def test_watermarks_returns_list(self, api_client):
        resp = api_client.get("/pipeline/watermarks")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_watermarks_domain_filter_orders(self, api_client, pg_conn):
        # Seed a watermark row so the test is repeatable
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.watermarks (domain, updated_at)
                VALUES ('orders', NOW())
                ON CONFLICT (domain) DO UPDATE SET updated_at = NOW()
                """
            )
        resp = api_client.get("/pipeline/watermarks/orders")
        assert resp.status_code == 200
        assert resp.json()["domain"] == "orders"

    def test_watermarks_unknown_domain_404(self, api_client):
        resp = api_client.get("/pipeline/watermarks/does_not_exist_xyz")
        assert resp.status_code == 404

    def test_sla_misses_returns_list(self, api_client):
        resp = api_client.get("/pipeline/sla-misses")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_sla_misses_hours_param(self, api_client):
        resp = api_client.get("/pipeline/sla-misses?hours=48&layer=bronze")
        assert resp.status_code == 200

    def test_pipeline_runs_list(self, api_client):
        resp = api_client.get("/pipeline/runs")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


# ─────────────────────────────────────────────────────────────────────────────
# Quality endpoints
# ─────────────────────────────────────────────────────────────────────────────
class TestQualityEndpointsLive:
    def test_summary_returns_list(self, api_client):
        resp = api_client.get("/quality/summary")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_checks_paginated_schema(self, api_client):
        resp = api_client.get("/quality/checks?limit=10&offset=0")
        assert resp.status_code == 200
        data = resp.json()
        assert "total" in data
        assert "items" in data
        assert "limit" in data
        assert "offset" in data
        assert isinstance(data["items"], list)

    def test_checks_status_filter(self, api_client):
        for status in ("passed", "failed", "warning", "error"):
            resp = api_client.get(f"/quality/checks?status={status}&limit=5")
            assert resp.status_code == 200

    def test_reports_returns_list(self, api_client):
        resp = api_client.get("/quality/reports?hours=24")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_trend_returns_list(self, api_client):
        resp = api_client.get("/quality/trend?hours=48&layer=bronze")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


# ─────────────────────────────────────────────────────────────────────────────
# Catalog endpoints
# ─────────────────────────────────────────────────────────────────────────────
class TestCatalogEndpointsLive:
    def test_tables_returns_list(self, api_client):
        resp = api_client.get("/catalog/tables")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_tables_layer_filter(self, api_client):
        for layer in ("bronze", "silver", "gold"):
            resp = api_client.get(f"/catalog/tables?layer={layer}")
            assert resp.status_code == 200

    def test_unknown_table_returns_404(self, api_client):
        resp = api_client.get("/catalog/tables/does_not_exist_xyz_abc")
        assert resp.status_code == 404


# ─────────────────────────────────────────────────────────────────────────────
# Prometheus metrics endpoint
# ─────────────────────────────────────────────────────────────────────────────
class TestMetricsEndpointLive:
    def test_metrics_returns_prometheus_text(self, api_client):
        resp = api_client.get("/metrics")
        assert resp.status_code == 200
        # Prometheus text format starts with "# HELP" or metric lines
        assert b"retailnexus" in resp.content or b"python_" in resp.content

    def test_custom_gauges_present(self, api_client):
        resp = api_client.get("/metrics")
        text = resp.text
        assert "retailnexus_sla_misses_24h" in text
        assert "retailnexus_dq_pass_rate" in text


# ─────────────────────────────────────────────────────────────────────────────
# DB-side assertions (via pg_conn)
# ─────────────────────────────────────────────────────────────────────────────
class TestDatabaseStateLive:
    def test_api_keys_table_has_dev_key(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM api.api_keys WHERE is_active = TRUE")
            count = cur.fetchone()[0]
        assert count >= 1, "At least one active API key must exist (seeded by init_postgres.sql)"

    def test_watermarks_table_exists(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'pipeline' AND table_name = 'watermarks'"
            )
            assert cur.fetchone()[0] == 1

    def test_quality_schemas_exist(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'quality' ORDER BY table_name"
            )
            tables = {row[0] for row in cur.fetchall()}
        assert "check_results" in tables
        assert "reports" in tables
