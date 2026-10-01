"""Prometheus gauge refresh: per-domain DQ gauges and the periodic background loop."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

import api.main as api_main


def _fake_db(monkeypatch, per_query_rows):
    """Patch psycopg2.connect so successive execute()/fetchall() calls return *per_query_rows* in order."""
    cur = MagicMock()
    cur.fetchall.side_effect = [r for r in per_query_rows if isinstance(r, list)]
    cur.fetchone.return_value = (3,)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    monkeypatch.setattr("psycopg2.connect", MagicMock(return_value=conn))
    return conn, cur


def test_refresh_sets_freshness_layer_and_domain_gauges(monkeypatch):
    _fake_db(
        monkeypatch,
        [
            [("orders", 1.5)],  # freshness
            [("bronze", 0.97)],  # layer pass rate
            [
                ("bronze", "orders", 0.9, 2),
                ("silver", "orders", 1.0, 0),
            ],  # latest per (layer, domain)
        ],
    )
    api_main._refresh_pipeline_metrics()

    assert api_main._PIPELINE_FRESHNESS.labels(domain="orders")._value.get() == 1.5
    assert api_main._DQ_PASS_RATE.labels(layer="bronze")._value.get() == 0.97
    assert api_main._DQ_DOMAIN_PASS_RATE.labels(layer="bronze", domain="orders")._value.get() == 0.9
    assert api_main._DQ_DOMAIN_FAILED.labels(layer="bronze", domain="orders")._value.get() == 2
    assert api_main._DQ_DOMAIN_PASS_RATE.labels(layer="silver", domain="orders")._value.get() == 1.0
    assert api_main._SLA_MISSES._value.get() == 3


def test_latest_per_domain_query_takes_newest_report(monkeypatch):
    _, cur = _fake_db(monkeypatch, [[], [], []])
    api_main._refresh_pipeline_metrics()
    sql = " ".join(c.args[0] for c in cur.execute.call_args_list)
    assert "DISTINCT ON (layer, domain)" in sql and "ORDER BY layer, domain, run_at DESC" in sql


def test_refresh_swallows_database_errors(monkeypatch):
    monkeypatch.setattr("psycopg2.connect", MagicMock(side_effect=RuntimeError("db down")))
    api_main._refresh_pipeline_metrics()  # must not raise — /metrics keeps serving


@pytest.mark.asyncio
async def test_refresh_loop_runs_repeatedly_off_the_event_loop(monkeypatch):
    calls: list[int] = []
    monkeypatch.setattr(api_main, "_METRICS_REFRESH_SECONDS", 0.01)
    monkeypatch.setattr(api_main, "_refresh_pipeline_metrics", lambda: calls.append(1))

    task = asyncio.create_task(api_main._metrics_refresh_loop())
    await asyncio.sleep(0.15)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(calls) >= 2
