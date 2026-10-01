"""
The code/DDL contract, exercised against a real PostgreSQL.

Every SQL statement the pipeline, quality framework, API and catalog issue against the
metadata database is run here after `scripts/setup/init_postgres.sql`, so a column rename
or a DDL/code mismatch fails loudly instead of being swallowed by the `try/except`
blocks that guard the (non-fatal) watermark writes.

    TEST_DATABASE_URL="host=localhost port=5432 dbname=lakehouse user=lakehouse password=lakehouse" \
        pytest tests/integration/test_postgres_contract.py -m integration -v

Skips when TEST_DATABASE_URL is unset. Runs against a scratch schema state: every test
cleans up the rows it creates.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import psycopg2.extras
import pytest

pytestmark = pytest.mark.integration

_DOMAIN = "zz_contract_test"


@pytest.fixture()
def dsn(pg_conn):
    return os.environ["TEST_DATABASE_URL"]


@pytest.fixture()
def settings(dsn):
    """Settings stand-in exposing what the pipeline classes read."""
    params = psycopg2.extensions.parse_dsn(dsn)
    return SimpleNamespace(
        postgres_dsn=dsn,
        postgres_host=params.get("host", "localhost"),
        postgres_port=int(params.get("port", 5432)),
        postgres_db=params["dbname"],
        postgres_user=params["user"],
        postgres_password=SimpleNamespace(get_secret_value=lambda: params.get("password", "")),
    )


@pytest.fixture(autouse=True)
def _cleanup(pg_conn):
    yield
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline.watermarks WHERE domain LIKE 'zz_%'")
        cur.execute("DELETE FROM quality.check_results WHERE domain LIKE 'zz_%'")
        cur.execute("DELETE FROM quality.reports WHERE domain LIKE 'zz_%'")
        cur.execute("DELETE FROM quality.contract_violations WHERE domain LIKE 'zz_%'")
        cur.execute("DELETE FROM pipeline.sla_misses WHERE domain LIKE 'zz_%'")
        cur.execute("DELETE FROM pipeline.runs WHERE run_id LIKE 'zz_%'")
        cur.execute(
            "DELETE FROM catalog.lineage WHERE source_table LIKE 'zz_%' OR target_table LIKE 'zz_%'"
        )
        cur.execute("DELETE FROM catalog.tables WHERE domain LIKE 'zz_%'")


def _watermark(pg_conn, domain=_DOMAIN):
    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM pipeline.watermarks WHERE domain = %s", (domain,))
        return cur.fetchone()


# ─── Watermarks: every layer writes only its own columns to the same row ──────
class TestWatermarks:
    def test_bronze_silver_gold_and_loader_share_one_row(self, pg_conn, settings):
        from src.loading.base_loader import BaseWarehouseLoader
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor
        from src.processing.gold.base_builder import BaseGoldBuilder
        from src.processing.silver.base_processor import BaseSilverProcessor

        log = MagicMock()

        bronze = SimpleNamespace(settings=settings, log=log, run_timestamp=datetime.now(UTC))
        BronzeBatchIngestor._update_watermark(bronze, _DOMAIN, 1234)

        silver = SimpleNamespace(settings=settings, log=log, domain=_DOMAIN)
        BaseSilverProcessor._update_watermark(silver, 7, 900)
        assert BaseSilverProcessor._get_last_bronze_version(silver) == 7

        gold = SimpleNamespace(settings=settings, log=log, table_name=_DOMAIN)
        BaseGoldBuilder._update_watermark(gold, 55)

        for target in ("redshift", "snowflake", "bigquery"):
            loader = SimpleNamespace(settings=settings, log=log, target=target)
            BaseWarehouseLoader._update_watermark(loader, _DOMAIN)

        row = _watermark(pg_conn)
        assert row["bronze_rows_written"] == 1234 and row["bronze_last_status"] == "success"
        assert row["bronze_last_ingest"] is not None and row["bronze_watermark"] is not None
        assert row["silver_bronze_version"] == 7 and row["silver_rows_written"] == 900
        assert row["gold_rows_written"] == 55 and row["gold_last_run"] is not None
        assert all(row[f"{t}_last_load"] for t in ("redshift", "snowflake", "bigquery"))

    def test_bronze_watermark_is_an_upsert_so_first_run_works(self, pg_conn, settings):
        from src.processing.bronze.batch_ingestor import BronzeBatchIngestor

        assert _watermark(pg_conn) is None
        bronze = SimpleNamespace(
            settings=settings, log=MagicMock(), run_timestamp=datetime.now(UTC)
        )
        BronzeBatchIngestor._update_watermark(bronze, _DOMAIN, 1)
        BronzeBatchIngestor._update_watermark(bronze, _DOMAIN, 2)
        assert _watermark(pg_conn)["bronze_rows_written"] == 2

    def test_updated_at_trigger_fires(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline.watermarks (domain, updated_at) VALUES (%s, NOW() - INTERVAL '1 day')",
                (_DOMAIN,),
            )
            cur.execute(
                "UPDATE pipeline.watermarks SET gold_rows_written = 1 WHERE domain = %s", (_DOMAIN,)
            )
            cur.execute(
                "SELECT NOW() - updated_at < INTERVAL '1 minute' FROM pipeline.watermarks WHERE domain = %s",
                (_DOMAIN,),
            )
            assert cur.fetchone()[0] is True

    def test_api_repository_reads_watermarks(self, pg_conn, settings):
        from api.repositories.pipeline_repo import PipelineRepository
        from src.processing.gold.base_builder import BaseGoldBuilder

        BaseGoldBuilder._update_watermark(
            SimpleNamespace(settings=settings, log=MagicMock(), table_name=_DOMAIN), 5
        )
        repo = PipelineRepository(pg_conn)
        assert repo.get_watermark(_DOMAIN)["gold_last_run"] is not None
        assert _DOMAIN in {w["domain"] for w in repo.list_watermarks()}

    def test_every_domain_is_seeded(self, pg_conn):
        from src.processing.silver.domain_meta import SILVER_PROCESSING_ORDER

        with pg_conn.cursor() as cur:
            cur.execute("SELECT domain FROM pipeline.watermarks")
            assert set(SILVER_PROCESSING_ORDER) <= {r[0] for r in cur.fetchall()}


# ─── Quality framework: reporter → tables → API repository → z-score history ──
class TestQualityRoundTrip:
    @staticmethod
    def _result(status, check="completeness", metric=None):
        from src.quality.checks.base_check import CheckResult

        return CheckResult(
            domain=_DOMAIN,
            layer="bronze",
            check_name=check,
            status=status,
            rows_checked=100,
            message="m",
            column_name="c",
            metric_value=metric,
            threshold=1.0,
        )

    def test_reporter_persists_and_api_repository_reads_it_back(self, pg_conn):
        from api.repositories.quality_repo import QualityRepository
        from src.quality.checks.base_check import CheckStatus
        from src.quality.reporters.pg_reporter import PostgresReporter

        results = [
            self._result(CheckStatus.PASSED),
            self._result(CheckStatus.PASSED, "uniqueness"),
            self._result(CheckStatus.FAILED, "freshness", metric=30.0),
            self._result(CheckStatus.WARNING, "value_range"),
        ]
        run_id = str(uuid.uuid4())
        reporter = PostgresReporter(pg_conn)
        reporter.save_results(results, run_id)
        reporter.save_report(results, run_id)
        reporter.save_sla_misses(results, run_id)

        repo = QualityRepository(pg_conn)
        total, rows = repo.list_checks(domain=_DOMAIN, limit=50, offset=0)
        assert total == 4 and {r["status"] for r in rows} == {"passed", "failed", "warning"}

        (report,) = (r for r in repo.list_reports(hours=1) if r["domain"] == _DOMAIN)
        assert (report["total_checks"], report["passed"], report["failed"], report["warnings"]) == (
            4,
            2,
            1,
            1,
        )
        assert report["pass_rate"] == 0.5

        layers = {r["layer"]: r for r in repo.summary_by_layer(hours=1)}
        assert "bronze" in layers

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT metric, actual_value FROM pipeline.sla_misses WHERE domain = %s", (_DOMAIN,)
            )
            assert cur.fetchall() == [("freshness_hours:c", 30.0)]

    def test_pipeline_repository_reads_sla_misses(self, pg_conn):
        from api.repositories.pipeline_repo import PipelineRepository
        from src.quality.checks.base_check import CheckStatus
        from src.quality.reporters.pg_reporter import PostgresReporter

        PostgresReporter(pg_conn).save_sla_misses(
            [self._result(CheckStatus.FAILED, "freshness", 9.0)], "r1"
        )
        repo = PipelineRepository(pg_conn)
        assert repo.count_sla_misses(hours=1) >= 1
        assert any(m["domain"] == _DOMAIN for m in repo.list_sla_misses(hours=1))

    def test_row_count_zscore_history_query_runs_and_flags_outlier(self, pg_conn):
        """The history query used ORDER BY + LIMIT on an aggregate, which Postgres rejects — the
        resulting exception was swallowed, so anomaly detection silently never ran."""
        from src.quality.checks.base_check import CheckStatus
        from src.quality.checks.row_count import RowCountCheck

        with pg_conn.cursor() as cur:
            for i in range(10):
                cur.execute(
                    "INSERT INTO quality.check_results (run_id, domain, layer, check_name, status, metric_value, run_at) "
                    "VALUES (%s, %s, 'bronze', 'row_count.minimum', 'passed', %s, NOW() - (%s * INTERVAL '1 hour'))",
                    (f"h{i}", _DOMAIN, 1000 + (i % 3), i),
                )
        check = RowCountCheck(min_count=1, max_z_score=3.0, history_conn=pg_conn)
        anomaly = check._zscore_check(50_000, _DOMAIN, "bronze")
        assert anomaly is not None and anomaly.status is CheckStatus.WARNING
        normal = check._zscore_check(1001, _DOMAIN, "bronze")
        assert normal is not None and normal.status is CheckStatus.PASSED

    def test_contract_violations_persist(self, pg_conn, settings):
        from src.quality.contracts import ContractEnforcer

        class _DF:
            dtypes: list = []
            columns: list = []

        import src.quality.contracts.enforcer as enforcer_mod

        enforcer_mod.check_data, original = (lambda df, c: (1, [])), enforcer_mod.check_data
        try:
            report = ContractEnforcer(settings=settings, mode="warn").check(
                _DF(), "orders", run_id="zz_run"
            )
        finally:
            enforcer_mod.check_data = original
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM quality.contract_violations WHERE run_id = 'zz_run'")
            assert cur.fetchone()[0] == len(report.violations) > 0
            cur.execute("DELETE FROM quality.contract_violations WHERE run_id = 'zz_run'")


# ─── Catalog + lineage ────────────────────────────────────────────────────────
class TestCatalogAndLineage:
    def test_delta_catalog_register_and_read(self, pg_conn, settings):
        from src.common.catalog.delta_catalog import DeltaCatalogManager

        mgr = DeltaCatalogManager(settings=MagicMock())
        mgr._conn = lambda: psycopg2.connect(os.environ["TEST_DATABASE_URL"])  # type: ignore[method-assign]
        mgr.register_table(
            "bronze", _DOMAIN, "s3a://b/bronze/x", row_count=10, schema_json='[{"name": "a"}]'
        )
        mgr.register_table("bronze", _DOMAIN, "s3a://b/bronze/x", row_count=20)  # upsert
        mgr.update_row_count("bronze", _DOMAIN, 30)
        row = mgr.get_table("bronze", _DOMAIN)
        assert row["row_count"] == 30 and row["s3_location"] == "s3a://b/bronze/x"
        assert row["schema_json"] == [
            {"name": "a"}
        ]  # COALESCE keeps the earlier schema on re-register

    def test_api_catalog_query_shape_executes(self, pg_conn):
        from src.common.catalog.delta_catalog import DeltaCatalogManager

        mgr = DeltaCatalogManager(settings=MagicMock())
        mgr._conn = lambda: psycopg2.connect(os.environ["TEST_DATABASE_URL"])  # type: ignore[method-assign]
        mgr.register_table("silver", _DOMAIN, "s3a://b/silver/x", row_count=1)
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT table_name, layer, domain, s3_location AS location,
                       COALESCE(format, 'delta') AS format, row_count, size_bytes,
                       created_at::text AS registered_at, updated_at::text AS last_updated, description
                FROM catalog.tables WHERE domain = %s
                """,
                (_DOMAIN,),
            )
            (row,) = cur.fetchall()
        assert row["table_name"] == f"silver.{_DOMAIN}" and row["format"] == "delta"

    def test_catalog_columns_join(self, pg_conn):
        from src.catalog.sync import column_rows

        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO catalog.tables (schema_name, table_name, layer, domain) VALUES ('gold', %s, 'gold', %s) RETURNING table_id",
                (f"gold.{_DOMAIN}", _DOMAIN),
            )
            table_id = cur.fetchone()[0]
            for c in column_rows([("a", "string", False), ("email", "string", True)]):
                cur.execute(
                    "INSERT INTO catalog.columns (table_id, column_name, data_type, ordinal_position, is_nullable, is_pii, is_pci) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        table_id,
                        c["column_name"],
                        c["data_type"],
                        c["ordinal_position"],
                        c["is_nullable"],
                        c["is_pii"],
                        c["is_pci"],
                    ),
                )
            cur.execute(
                """
                SELECT c.column_name, COALESCE(c.is_pii, FALSE) AS is_pii
                FROM catalog.columns c JOIN catalog.tables t ON t.table_id = c.table_id
                WHERE t.table_name = %s ORDER BY c.ordinal_position, c.column_name
                """,
                (f"gold.{_DOMAIN}",),
            )
            assert cur.fetchall() == [("a", False), ("email", True)]

    def test_static_lineage_sync_is_idempotent_and_queryable(self, pg_conn, settings):
        from src.lineage.static_graph import build_static_edges
        from src.lineage.tracker import LineageTracker

        tracker = LineageTracker(settings=settings, conn=pg_conn)
        n = tracker.sync_static()
        assert n == len(build_static_edges())
        assert tracker.sync_static() == n  # second run upserts, no duplicate-key error
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM catalog.lineage")
            assert cur.fetchone()[0] == n
        upstream = {x.table for x in tracker.trace("gold.fact_sales", "upstream")}
        assert {"silver.orders", "bronze.orders"} <= upstream

    def test_lineage_run_id_kept_on_reupsert(self, pg_conn, settings):
        from src.lineage.models import LineageEdge
        from src.lineage.tracker import LineageTracker

        edge = LineageEdge("zz_a", "zz_b", "t", "job")
        tracker = LineageTracker(settings=settings, conn=pg_conn)
        tracker.record([edge], run_id="run-1")
        tracker.record([edge])  # no run_id -> COALESCE keeps run-1
        with pg_conn.cursor() as cur:
            cur.execute("SELECT run_id FROM catalog.lineage WHERE source_table = 'zz_a'")
            assert cur.fetchone()[0] == "run-1"


# ─── API auth against the seeded dev key ──────────────────────────────────────
class TestApiKeys:
    def test_seeded_dev_key_authenticates(self, pg_conn):
        from api.dependencies import verify_api_key

        ctx = verify_api_key("dev-api-key-retailnexus-2026", pg_conn)
        assert ctx.client_name == "Local Development Key"
        assert ctx.allows("read:pipeline") and ctx.allows("read:catalog")

    def test_unknown_key_rejected(self, pg_conn):
        from fastapi import HTTPException

        from api.dependencies import verify_api_key

        with pytest.raises(HTTPException) as exc:
            verify_api_key("nope", pg_conn)
        assert exc.value.status_code == 401

    def test_expired_and_inactive_keys_rejected(self, pg_conn):
        from fastapi import HTTPException

        from api.dependencies import verify_api_key

        with pg_conn.cursor() as cur:
            for name, plain, extra in (
                ("expired", "zz-expired", "expires_at = NOW() - INTERVAL '1 day'"),
                ("inactive", "zz-inactive", "is_active = FALSE"),
            ):
                digest = hashlib.sha256(plain.encode()).hexdigest()
                cur.execute(
                    "INSERT INTO api.api_keys (key_hash, key_prefix, client_name) VALUES (%s, 'zz-', %s)",
                    (digest, name),
                )
                cur.execute(f"UPDATE api.api_keys SET {extra} WHERE key_hash = %s", (digest,))
        try:
            for plain in ("zz-expired", "zz-inactive"):
                with pytest.raises(HTTPException):
                    verify_api_key(plain, pg_conn)
        finally:
            with pg_conn.cursor() as cur:
                cur.execute("DELETE FROM api.api_keys WHERE key_prefix = 'zz-'")

    def test_metrics_refresh_queries_run_against_real_schema(self, pg_conn, dsn, monkeypatch):
        import api.main as api_main

        real_connect = psycopg2.connect
        monkeypatch.setattr("psycopg2.connect", lambda *_a, **_k: real_connect(dsn))
        monkeypatch.setattr("src.common.config.Settings", lambda: SimpleNamespace(postgres_dsn=dsn))
        raised = []
        monkeypatch.setattr(
            api_main.logger, "warning", lambda event, **kw: raised.append((event, kw))
        )
        api_main._refresh_pipeline_metrics()
        assert not raised, f"metrics refresh failed against the real schema: {raised}"


class TestPipelineRuns:
    def test_bronze_dag_run_insert_and_api_read(self, pg_conn):
        from api.repositories.pipeline_repo import PipelineRepository

        with pg_conn.cursor() as cur:
            insert = (
                "INSERT INTO pipeline.runs (run_id, dag_id, execution_date, status, started_at, ended_at) "
                "VALUES (%s, %s, %s, 'success', NOW() - INTERVAL '2 hours', NOW()) "
                "ON CONFLICT (run_id) DO UPDATE SET status = 'success', ended_at = NOW()"
            )
            cur.execute(insert, ("zz_run_1", "bronze_batch_ingestion", "2026-01-01"))
            cur.execute(
                insert, ("zz_run_1", "bronze_batch_ingestion", "2026-01-01")
            )  # re-run: upsert
        runs = PipelineRepository(pg_conn).list_runs(limit=500)
        assert any(r["run_id"] == "zz_run_1" for r in runs)
