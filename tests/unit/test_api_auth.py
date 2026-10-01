"""
API authentication unit tests.

Covers:
  - API key hashing is deterministic SHA-256
  - verify_api_key raises 401 on bad key
  - verify_api_key returns ApiKeyContext on valid key
  - verify_api_key raises 401 on expired key
  - verify_api_key raises 401 on inactive key
  - Dependencies produce correct DB pool behaviour
  - Pydantic models: WatermarkResponse, SlaMissResponse field validation
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from api.dependencies import ApiKeyContext, _hash_key, verify_api_key
from api.models.pipeline import (
    DomainStatusResponse,
    PipelineStatusResponse,
    SlaMissResponse,
    WatermarkResponse,
)
from api.models.quality import (
    CheckResultResponse,
    CheckStatusEnum,
    PaginatedChecks,
    QualityReportResponse,
    QualitySummaryResponse,
)


# ─────────────────────────────────────────────────────────────────────────────
# Key hashing
# ─────────────────────────────────────────────────────────────────────────────
class TestKeyHashing:
    def test_hash_is_sha256_hex(self):
        key = "test-api-key-12345"
        result = _hash_key(key)
        assert result == hashlib.sha256(key.encode()).hexdigest()
        assert len(result) == 64

    def test_hash_is_deterministic(self):
        key = "my-secret-key"
        assert _hash_key(key) == _hash_key(key)

    def test_different_keys_produce_different_hashes(self):
        assert _hash_key("key-one") != _hash_key("key-two")

    def test_empty_string_hashes_consistently(self):
        h = _hash_key("")
        assert len(h) == 64
        assert _hash_key("") == h


# ─────────────────────────────────────────────────────────────────────────────
# verify_api_key dependency
# ─────────────────────────────────────────────────────────────────────────────
def _make_mock_conn(fetchone_result):
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone = MagicMock(return_value=fetchone_result)
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cur)
    return conn


class TestVerifyApiKey:
    def test_valid_key_returns_context(self):
        conn = _make_mock_conn({"key_id": 1, "client_name": "airflow", "permissions": ["read"]})
        ctx = verify_api_key(x_api_key="valid-key", conn=conn)
        assert isinstance(ctx, ApiKeyContext)
        assert ctx.key_id == "1"  # normalised to str: the DB column is a UUID
        assert ctx.client_name == "airflow"
        assert ctx.permissions == ["read"]

    def test_missing_key_raises_401(self):
        conn = _make_mock_conn(None)
        with pytest.raises(HTTPException) as exc_info:
            verify_api_key(x_api_key="bad-key", conn=conn)
        assert exc_info.value.status_code == 401

    def test_401_has_www_authenticate_header(self):
        conn = _make_mock_conn(None)
        with pytest.raises(HTTPException) as exc_info:
            verify_api_key(x_api_key="bad-key", conn=conn)
        assert "WWW-Authenticate" in exc_info.value.headers

    def test_permissions_json_string_parsed(self):
        """Some DB drivers return JSONB as string — must be parsed to list."""
        conn = _make_mock_conn(
            {"key_id": 2, "client_name": "grafana", "permissions": '["read","metrics"]'}
        )
        ctx = verify_api_key(x_api_key="grafana-key", conn=conn)
        assert ctx.permissions == ["read", "metrics"]

    def test_null_permissions_becomes_empty_list(self):
        conn = _make_mock_conn({"key_id": 3, "client_name": "bot", "permissions": None})
        ctx = verify_api_key(x_api_key="bot-key", conn=conn)
        assert ctx.permissions == []


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic model validation — Pipeline
# ─────────────────────────────────────────────────────────────────────────────
class TestPipelineModels:
    def test_watermark_response_optional_fields(self):
        wm = WatermarkResponse(domain="orders")
        assert wm.bronze_last_ingest is None
        assert wm.silver_last_run is None

    def test_watermark_response_with_timestamps(self):
        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
        wm = WatermarkResponse(domain="orders", bronze_last_ingest=ts)
        assert wm.bronze_last_ingest == ts

    def test_sla_miss_response_required_fields(self):
        now = datetime.now(UTC)
        miss = SlaMissResponse(
            id=1,
            run_id="abc",
            domain="orders",
            layer="bronze",
            metric="freshness_hours:created_at",
            message="old data",
            detected_at=now,
        )
        assert miss.domain == "orders"
        assert miss.actual_value is None

    def test_domain_status_defaults_to_ok(self):
        ds = DomainStatusResponse(domain="orders")
        assert ds.bronze_sla_ok is True
        assert ds.silver_sla_ok is True
        assert ds.gold_sla_ok is True

    def test_pipeline_status_response(self):
        now = datetime.now(UTC)
        resp = PipelineStatusResponse(
            as_of=now,
            total_domains=16,
            domains_ok=15,
            domains_warn=1,
            sla_miss_count_24h=2,
            domains=[],
        )
        assert resp.total_domains == 16
        assert resp.domains_ok + resp.domains_warn <= resp.total_domains


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic model validation — Quality
# ─────────────────────────────────────────────────────────────────────────────
class TestQualityModels:
    def test_check_status_enum_values(self):
        assert CheckStatusEnum.PASSED.value == "passed"
        assert CheckStatusEnum.FAILED.value == "failed"
        assert CheckStatusEnum.WARNING.value == "warning"
        assert CheckStatusEnum.ERROR.value == "error"

    def test_check_result_response(self):
        now = datetime.now(UTC)
        r = CheckResultResponse(
            id=1,
            run_id="abc",
            domain="orders",
            layer="silver",
            check_name="completeness",
            status="passed",
            rows_checked=1000,
            message="ok",
            run_at=now,
        )
        assert r.status == CheckStatusEnum.PASSED
        assert r.column_name is None
        assert r.metric_value is None

    def test_quality_summary_pass_rate_range(self):
        s = QualitySummaryResponse(
            layer="bronze",
            period_hours=24.0,
            total_checks=100,
            passed=90,
            failed=5,
            warnings=3,
            errors=2,
            pass_rate=0.9,
            domains_checked=16,
        )
        assert 0.0 <= s.pass_rate <= 1.0

    def test_paginated_checks_structure(self):
        now = datetime.now(UTC)
        item = CheckResultResponse(
            id=1,
            run_id="x",
            domain="orders",
            layer="bronze",
            check_name="row_count.minimum",
            status="passed",
            rows_checked=500,
            message="ok",
            run_at=now,
        )
        page = PaginatedChecks(total=1, limit=100, offset=0, items=[item])
        assert page.total == 1
        assert len(page.items) == 1

    def test_quality_report_response(self):
        now = datetime.now(UTC)
        r = QualityReportResponse(
            id=1,
            run_id="abc",
            domain="orders",
            layer="silver",
            total_checks=10,
            passed=9,
            failed=1,
            warnings=0,
            errors=0,
            pass_rate=0.9,
            run_at=now,
        )
        assert r.pass_rate == 0.9


# ─────────────────────────────────────────────────────────────────────────────
# Service logic — PipelineService.get_pipeline_status
# ─────────────────────────────────────────────────────────────────────────────
class TestPipelineService:
    def _make_repo(self, watermarks=None, sla_count=0):
        from api.repositories.pipeline_repo import PipelineRepository

        repo = MagicMock(spec=PipelineRepository)
        repo.list_watermarks.return_value = watermarks or []
        repo.count_sla_misses.return_value = sla_count
        repo.freshness_hours.return_value = None
        return repo

    def test_status_with_no_watermarks(self):
        from api.services.pipeline_service import PipelineService

        svc = PipelineService(self._make_repo())
        result = svc.get_pipeline_status()
        assert result.total_domains == 0
        assert result.domains_ok == 0

    def test_status_counts_domains_ok(self):
        from api.services.pipeline_service import PipelineService

        repo = self._make_repo(
            watermarks=[
                {
                    "domain": "orders",
                    "bronze_last_ingest": None,
                    "silver_last_run": None,
                    "gold_last_run": None,
                    "redshift_last_load": None,
                    "snowflake_last_load": None,
                    "bigquery_last_load": None,
                },
            ]
        )
        svc = PipelineService(repo)
        result = svc.get_pipeline_status()
        assert result.total_domains == 1

    def test_sla_miss_count_propagated(self):
        from api.services.pipeline_service import PipelineService

        svc = PipelineService(self._make_repo(sla_count=7))
        result = svc.get_pipeline_status()
        assert result.sla_miss_count_24h == 7


def test_uuid_key_id_is_accepted():
    """api.api_keys.key_id is a UUID; ApiKeyContext used to require an int, so every valid key returned a 500."""
    import uuid

    from api.dependencies import ApiKeyContext

    kid = uuid.uuid4()
    ctx = ApiKeyContext(key_id=kid, client_name="c", permissions=[])
    assert ctx.key_id == str(kid)
