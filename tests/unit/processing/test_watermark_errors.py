"""Silver/Gold checkpoint failures must fail the task; the warehouse loader's must not."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.common.utils.errors import WatermarkError
from src.loading.base_loader import BaseWarehouseLoader
from src.processing.gold.base_builder import BaseGoldBuilder
from src.processing.silver.base_processor import BaseSilverProcessor


@pytest.fixture()
def db_down(monkeypatch):
    monkeypatch.setattr(
        "psycopg2.connect", MagicMock(side_effect=RuntimeError("connection refused"))
    )


def _stub(**kw):
    return SimpleNamespace(settings=SimpleNamespace(postgres_dsn="dsn"), log=MagicMock(), **kw)


class TestSilver:
    def test_write_failure_raises(self, db_down):
        stub = _stub(domain="orders")
        with pytest.raises(WatermarkError, match="orders"):
            BaseSilverProcessor._update_watermark(stub, 7, 100)
        stub.log.error.assert_called_once()

    def test_read_failure_raises_instead_of_restarting_from_version_zero(self, db_down):
        with pytest.raises(WatermarkError, match="orders"):
            BaseSilverProcessor._get_last_bronze_version(_stub(domain="orders"))

    def test_nothing_to_write_is_a_noop(self, db_down):
        BaseSilverProcessor._update_watermark(
            _stub(domain="orders"), None, 0
        )  # no DB access, no error

    def test_first_run_returns_none(self, monkeypatch):
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value.fetchone.return_value = None
        monkeypatch.setattr("psycopg2.connect", MagicMock(return_value=conn))
        assert BaseSilverProcessor._get_last_bronze_version(_stub(domain="orders")) is None


class TestGold:
    def test_write_failure_raises(self, db_down):
        with pytest.raises(WatermarkError, match="fact_sales"):
            BaseGoldBuilder._update_watermark(_stub(table_name="fact_sales"), 5)

    def test_error_is_a_runtime_error_with_the_cause_chained(self, db_down):
        with pytest.raises(WatermarkError) as exc:
            BaseGoldBuilder._update_watermark(_stub(table_name="fact_sales"), 5)
        assert isinstance(exc.value, RuntimeError) and isinstance(exc.value.__cause__, RuntimeError)


class TestLoader:
    def test_failure_is_logged_at_error_level_but_not_raised(self, db_down):
        stub = _stub(target="redshift")
        BaseWarehouseLoader._update_watermark(
            stub, "fact_sales"
        )  # must not raise: re-running would duplicate rows
        stub.log.error.assert_called_once()
        stub.log.warning.assert_not_called()


class TestGoldMerge:
    def test_partition_columns_are_never_overwritten_on_match(self):
        from src.common.utils.delta import merge_update_set

        update = merge_update_set(
            ["nk_order_line_id", "revenue", "_gold_date", "_batch_id"], ["_gold_date"]
        )
        assert "_gold_date" not in update
        assert (
            update["revenue"] == "source.`revenue`" and update["_batch_id"] == "source.`_batch_id`"
        )

    def test_business_date_partitions_are_also_protected(self):
        from src.common.utils.delta import merge_update_set

        assert "snapshot_date" not in merge_update_set(
            ["nk_inventory_id", "snapshot_date", "qty"], ["snapshot_date"]
        )

    def test_every_fact_has_a_partition_and_primary_key_in_its_metadata(self):
        from src.processing.gold.domain_meta import (
            FACT_PARTITION_COLS,
            FACT_PRIMARY_KEYS,
            GOLD_FACT_ORDER,
        )

        assert set(GOLD_FACT_ORDER) == set(FACT_PRIMARY_KEYS) == set(FACT_PARTITION_COLS)

    def test_merge_no_longer_uses_update_all(self):
        from pathlib import Path

        text = (
            Path(__file__).resolve().parents[3] / "src" / "processing" / "gold" / "base_builder.py"
        ).read_text(encoding="utf-8")
        assert "whenMatchedUpdateAll" not in text and "merge_update_set(df.columns, keep)" in text

    def test_only_facts_with_a_stable_business_date_prune_the_merge_by_partition(self):
        from src.processing.gold.domain_meta import FACT_PARTITION_COLS, FACT_STABLE_PARTITION

        assert set(FACT_PARTITION_COLS) >= FACT_STABLE_PARTITION
        # a shipment's date moves from "created" to "shipped": matching on the partition would duplicate it
        assert "fact_shipments" not in FACT_STABLE_PARTITION
