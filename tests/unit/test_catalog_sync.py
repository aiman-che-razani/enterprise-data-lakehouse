"""Catalog synchroniser — pure helpers plus the sync loop against a stubbed Spark/Postgres."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from src.catalog.sync import (
    PCI_COLUMNS,
    PII_COLUMNS,
    CatalogSynchronizer,
    TableRef,
    all_tables,
    column_rows,
)
from src.processing.gold.domain_meta import GOLD_DIM_ORDER, GOLD_FACT_ORDER
from src.processing.silver.domain_meta import SILVER_PII_HANDLING, SILVER_PROCESSING_ORDER


class TestAllTables:
    def test_counts(self):
        refs = all_tables()
        n_dom = len(SILVER_PROCESSING_ORDER)
        assert len(refs) == 2 * n_dom + len(GOLD_DIM_ORDER) + len(GOLD_FACT_ORDER)

    def test_layer_filter(self):
        assert {r.layer for r in all_tables(["gold"])} == {"gold"}

    def test_qualified_names_are_unique(self):
        names = [r.qualified for r in all_tables()]
        assert len(names) == len(set(names))

    def test_primary_keys_resolved(self):
        by_name = {r.qualified: r.primary_key for r in all_tables()}
        assert by_name["bronze.orders"] == "order_id"
        assert by_name["gold.fact_sales"] == "nk_order_line_id"
        assert by_name["gold.dim_customer"] == "nk_customer_id"


class TestColumnRows:
    def test_rows_and_flags(self):
        rows = column_rows(
            [
                ("order_id", "string", False),
                ("email", "string", True),
                ("card_last4", "string", True),
            ]
        )
        assert [r["ordinal_position"] for r in rows] == [1, 2, 3]
        assert rows[0]["is_nullable"] is False
        assert rows[1]["is_pii"] is True and rows[1]["is_pci"] is False
        assert rows[2]["is_pci"] is True

    def test_type_truncated_to_column_width(self):
        (row,) = column_rows([("c", "struct<" + "x:string," * 30 + ">", True)])
        assert len(row["data_type"]) == 64

    def test_every_tokenised_pii_column_is_classified_as_pii(self):
        for domain, handling in SILVER_PII_HANDLING.items():
            assert (
                set(handling) <= PII_COLUMNS | PCI_COLUMNS
            ), f"{domain}: {set(handling) - PII_COLUMNS - PCI_COLUMNS} not flagged"

    def test_pii_and_pci_sets_are_disjoint(self):
        assert not (PII_COLUMNS & PCI_COLUMNS)


class _FakeField:
    def __init__(self, name, type_str, nullable=True):
        self.name, self.nullable = name, nullable
        self.dataType = SimpleNamespace(simpleString=lambda: type_str)


def _spark(existing: set[str]):
    """A Spark stub where only paths containing a name in *existing* can be read."""
    spark = MagicMock()

    def load(path):
        if not any(path.endswith("/" + n) for n in existing):
            raise RuntimeError("Path does not exist")
        df = MagicMock()
        df.schema.fields = [_FakeField("order_id", "string", False), _FakeField("email", "string")]
        df.count.return_value = 42
        return df

    spark.read.format.return_value.load.side_effect = load
    detail = {"sizeInBytes": 2048, "partitionColumns": ["_gold_date"]}
    spark.sql.return_value.first.return_value = detail
    return spark


class TestSynchronizer:
    def _settings(self):
        s = MagicMock(postgres_dsn="dsn")
        s.s3_path.side_effect = lambda layer, name: f"s3a://bucket/{layer}/{name}"
        return s

    def test_sync_all_reports_missing_and_synced(self, monkeypatch):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ("11111111-1111-1111-1111-111111111111",)
        monkeypatch.setattr("psycopg2.connect", MagicMock(return_value=conn))
        monkeypatch.setattr("psycopg2.extras.execute_values", MagicMock())

        sync = CatalogSynchronizer(_spark({"orders"}), self._settings())
        result = sync.sync_all(["bronze"])

        assert result["synced"] == ["bronze.orders"]
        assert (
            "bronze.customers" in result["missing"]
            and len(result["missing"]) == len(SILVER_PROCESSING_ORDER) - 1
        )
        conn.commit.assert_called_once()
        conn.close.assert_called_once()

    def test_upsert_targets_schema_aligned_columns(self, monkeypatch):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ("id",)
        values = MagicMock()
        monkeypatch.setattr("psycopg2.connect", MagicMock(return_value=conn))
        monkeypatch.setattr("psycopg2.extras.execute_values", values)

        CatalogSynchronizer(_spark({"orders"}), self._settings(), count_rows=True).sync_all(
            ["bronze"]
        )

        sql, params = cur.execute.call_args_list[0][0]
        assert "ON CONFLICT (schema_name, table_name)" in sql and "s3_location" in sql
        assert params[:5] == (
            "bronze",
            "bronze.orders",
            "bronze",
            "orders",
            "s3a://bucket/bronze/orders",
        )
        assert params[7] == 42  # row_count from count()
        assert params[8] == 2048  # size_bytes from DESCRIBE DETAIL
        col_rows = values.call_args[0][2]
        assert [r[1] for r in col_rows] == ["order_id", "email"]
        assert col_rows[1][5] is True  # email flagged PII

    def test_one_failing_table_does_not_stop_the_sweep(self, monkeypatch):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = ("id",)
        monkeypatch.setattr("psycopg2.connect", MagicMock(return_value=conn))
        monkeypatch.setattr(
            "psycopg2.extras.execute_values",
            MagicMock(side_effect=[RuntimeError("boom"), None, None]),
        )

        result = CatalogSynchronizer(
            _spark({"orders", "payments", "customers"}), self._settings()
        ).sync_all(["bronze"])
        assert len(result["synced"]) == 2
        conn.rollback.assert_called()

    def test_tableref_qualified(self):
        assert TableRef("gold", "dim_date", None).qualified == "gold.dim_date"
