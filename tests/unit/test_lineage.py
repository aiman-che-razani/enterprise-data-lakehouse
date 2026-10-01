"""Table-level lineage: graph traversal, the declared platform graph, and the tracker."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.lineage.models import LineageEdge, walk
from src.lineage.static_graph import GOLD_FACT_SOURCES, build_static_edges, gold_sources
from src.lineage.tracker import LineageTracker
from src.processing.gold.domain_meta import GOLD_DIM_ORDER, GOLD_FACT_ORDER
from src.processing.silver.domain_meta import SILVER_PROCESSING_ORDER

_ROOT = Path(__file__).resolve().parents[2]


def _e(src: str, dst: str) -> LineageEdge:
    return LineageEdge(src, dst, "t", "j")


class TestWalk:
    edges = [_e("a", "b"), _e("b", "c"), _e("b", "d"), _e("c", "e"), _e("d", "e")]

    def test_upstream_orders_by_depth_then_name(self):
        nodes = walk(self.edges, "e", "upstream")
        assert [(n.table, n.depth) for n in nodes] == [("c", 1), ("d", 1), ("b", 2), ("a", 3)]

    def test_downstream(self):
        nodes = walk(self.edges, "a", "downstream")
        assert [(n.table, n.depth) for n in nodes] == [("b", 1), ("c", 2), ("d", 2), ("e", 3)]

    def test_max_depth_limits_traversal(self):
        assert [n.table for n in walk(self.edges, "e", "upstream", max_depth=1)] == ["c", "d"]

    def test_diamond_reports_each_table_once_at_shortest_depth(self):
        assert [n.table for n in walk(self.edges, "a", "downstream")].count("e") == 1

    def test_unknown_table_yields_nothing(self):
        assert walk(self.edges, "zzz") == []

    def test_cycles_terminate(self):
        cyc = [_e("x", "y"), _e("y", "x")]
        assert [n.table for n in walk(cyc, "x", "downstream")] == ["y"]

    def test_invalid_direction(self):
        with pytest.raises(ValueError):
            walk([], "a", "sideways")  # type: ignore[arg-type]


class TestStaticGraph:
    edges = build_static_edges()

    def test_no_duplicates_and_deterministic(self):
        assert len(self.edges) == len(set(self.edges))
        assert self.edges == build_static_edges()

    def test_every_silver_domain_fed_by_its_bronze_table(self):
        pairs = {(e.source_table, e.target_table) for e in self.edges}
        for d in SILVER_PROCESSING_ORDER:
            assert (f"bronze.{d}", f"silver.{d}") in pairs

    def test_streaming_topics_feed_bronze(self):
        assert (
            LineageEdge(
                "kafka.retailnexus.orders",
                "bronze.orders",
                "stream_ingest",
                "spark.streaming.orders",
            )
            in self.edges
        )

    def test_every_gold_table_reaches_all_three_warehouses(self):
        pairs = {(e.source_table, e.target_table) for e in self.edges}
        for t in list(GOLD_DIM_ORDER) + list(GOLD_FACT_ORDER):
            for wh in ("redshift", "snowflake", "bigquery"):
                assert (f"gold.{t}", f"{wh}.{t}") in pairs

    def test_fact_sales_full_upstream_chain(self):
        upstream = {n.table for n in walk(self.edges, "gold.fact_sales", "upstream")}
        assert {
            "silver.orders",
            "silver.order_lines",
            "bronze.orders",
            "kafka.retailnexus.orders",
        } <= upstream

    def test_customer_data_downstream_reaches_warehouses(self):
        down = {n.table for n in walk(self.edges, "bronze.customers", "downstream")}
        assert {
            "silver.customers",
            "gold.dim_customer",
            "gold.dim_location",
            "redshift.dim_customer",
        } <= down

    def test_dim_date_has_no_silver_source(self):
        assert "dim_date" not in gold_sources()

    def test_gold_fact_sources_match_read_silver_calls(self):
        """Drift guard: declared sources must equal the read_silver() calls in the builders."""
        for fact, declared in GOLD_FACT_SOURCES.items():
            src = (_ROOT / "src" / "processing" / "gold" / "facts" / f"{fact}.py").read_text(
                encoding="utf-8"
            )
            actual = set(re.findall(r'read_silver\("(\w+)"\)', src))
            assert actual == set(
                declared
            ), f"{fact}: builder reads {sorted(actual)}, lineage declares {sorted(declared)}"

    def test_dimension_sources_match_read_silver_calls(self):
        for dim, declared in gold_sources().items():
            if not dim.startswith("dim_"):
                continue
            path = _ROOT / "src" / "processing" / "gold" / "dimensions" / f"{dim}.py"
            actual = set(
                re.findall(r'read_silver(?:_current)?\("(\w+)"\)', path.read_text(encoding="utf-8"))
            )
            assert actual == set(
                declared
            ), f"{dim}: builder reads {sorted(actual)}, lineage declares {sorted(declared)}"


class TestTracker:
    @staticmethod
    def _conn():
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        return conn, cur

    def test_record_empty_is_noop(self):
        conn, cur = self._conn()
        assert LineageTracker(settings=MagicMock(), conn=conn).record([]) == 0
        conn.cursor.assert_not_called()

    def test_record_upserts_and_commits(self, monkeypatch):
        conn, cur = self._conn()
        captured = {}
        monkeypatch.setattr(
            "psycopg2.extras.execute_values",
            lambda c, sql, rows: captured.update(sql=sql, rows=rows),
        )
        n = LineageTracker(settings=MagicMock(), conn=conn).record(
            [_e("a", "b"), _e("b", "c")], run_id="r9"
        )
        assert n == 2
        assert "ON CONFLICT (source_table, target_table, job_name)" in captured["sql"]
        assert captured["rows"][0] == ("a", "b", "t", "j", "r9")
        conn.commit.assert_called_once()
        conn.close.assert_not_called()  # injected connections belong to the caller

    def test_trace_reads_edges_from_db(self):
        conn, cur = self._conn()
        cur.fetchall.return_value = [("a", "b", "t", "j"), ("b", "c", "t", "j")]
        nodes = LineageTracker(settings=MagicMock(), conn=conn).trace("c", "upstream")
        assert [n.table for n in nodes] == ["b", "a"]

    def test_own_connection_is_closed(self, monkeypatch):
        conn, cur = self._conn()
        cur.fetchall.return_value = []
        monkeypatch.setattr("psycopg2.connect", MagicMock(return_value=conn))
        LineageTracker(settings=MagicMock(postgres_dsn="dsn")).edges()
        conn.close.assert_called_once()
