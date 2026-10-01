"""Catalog endpoints added/repaired for schema alignment: lineage + column/table SQL shape."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api.dependencies import ApiKeyContext, get_db, verify_api_key
from api.main import app

_HEADERS = {"X-API-Key": "test-key"}


def _cursor(conn: MagicMock) -> MagicMock:
    return conn.cursor.return_value.__enter__.return_value


@pytest.fixture()
def db():
    conn = MagicMock()

    def _override_db():
        yield conn

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[verify_api_key] = lambda: ApiKeyContext(
        key_id="1", client_name="test", permissions=["*"]
    )
    yield conn
    app.dependency_overrides.clear()


@pytest.fixture()
def client(db):
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


_EDGES = [
    {
        "source_table": "bronze.orders",
        "target_table": "silver.orders",
        "transformation_type": "silver_transform",
        "job_name": "j1",
    },
    {
        "source_table": "silver.orders",
        "target_table": "gold.fact_sales",
        "transformation_type": "gold_build",
        "job_name": "j2",
    },
    {
        "source_table": "gold.fact_sales",
        "target_table": "redshift.fact_sales",
        "transformation_type": "warehouse_load",
        "job_name": "j3",
    },
]


class TestLineageEndpoint:
    def test_upstream_default(self, client, db):
        _cursor(db).fetchall.return_value = _EDGES
        body = client.get("/catalog/lineage/gold.fact_sales", headers=_HEADERS).json()
        assert body["direction"] == "upstream"
        assert [(n["table"], n["depth"]) for n in body["nodes"]] == [
            ("silver.orders", 1),
            ("bronze.orders", 2),
        ]

    def test_downstream_with_max_depth(self, client, db):
        _cursor(db).fetchall.return_value = _EDGES
        resp = client.get(
            "/catalog/lineage/bronze.orders?direction=downstream&max_depth=2", headers=_HEADERS
        )
        assert [n["table"] for n in resp.json()["nodes"]] == ["silver.orders", "gold.fact_sales"]

    def test_unknown_table_is_404(self, client, db):
        _cursor(db).fetchall.return_value = _EDGES
        assert client.get("/catalog/lineage/gold.nope", headers=_HEADERS).status_code == 404

    def test_invalid_direction_is_422(self, client, db):
        _cursor(db).fetchall.return_value = _EDGES
        assert (
            client.get(
                "/catalog/lineage/gold.fact_sales?direction=sideways", headers=_HEADERS
            ).status_code
            == 422
        )

    def test_max_depth_bounds(self, client, db):
        assert client.get("/catalog/lineage/x?max_depth=0", headers=_HEADERS).status_code == 422
        assert client.get("/catalog/lineage/x?max_depth=99", headers=_HEADERS).status_code == 422


class TestCatalogSqlMatchesDdl:
    """The router used to select columns (location, registered_at, ...) that the DDL never had."""

    def test_list_tables_uses_ddl_columns(self, client, db):
        _cursor(db).fetchall.return_value = []
        client.get("/catalog/tables", headers=_HEADERS)
        sql = _cursor(db).execute.call_args[0][0]
        for expected in (
            "s3_location AS location",
            "created_at::text AS registered_at",
            "updated_at::text AS last_updated",
        ):
            assert expected in sql
        assert "size_bytes" in sql

    def test_columns_join_through_table_id(self, client, db):
        _cursor(db).fetchall.return_value = []
        client.get("/catalog/columns/silver.orders", headers=_HEADERS)
        sql, params = _cursor(db).execute.call_args[0]
        assert "JOIN catalog.tables t ON t.table_id = c.table_id" in sql
        assert params == ("silver.orders",)
