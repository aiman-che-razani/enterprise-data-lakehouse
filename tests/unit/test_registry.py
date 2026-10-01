"""Unit tests for datasets/generators/registries.py."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from datasets.generators.registries import Registry

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture()
def populated_registry() -> Registry:
    reg = Registry()
    reg.supplier_ids.extend(["SUP-000001", "SUP-000002"])
    reg.supplier_tiers["SUP-000001"] = "A"
    reg.supplier_tiers["SUP-000002"] = "B"
    reg.store_ids.extend(["STR-00001", "STR-00002", "STR-00003"])
    reg.customer_ids.extend([f"CUST-{i:08d}" for i in range(1, 11)])
    for i, cid in enumerate(reg.customer_ids):
        reg.customer_segments[cid] = ["platinum", "gold", "silver", "bronze"][i % 4]
        reg.customer_countries[cid] = "United States"
    reg.product_skus.extend([f"SKU-{i:08d}" for i in range(1, 21)])
    reg.active_skus.extend(reg.product_skus[:15])
    for sku in reg.product_skus:
        reg.product_prices[sku] = round(float(i * 9.99), 2)
        reg.product_names[sku] = f"Product {sku}"
        reg.product_categories[sku] = "Electronics"
    reg.order_ids.extend([f"ORD-{i:010d}" for i in range(1, 6)])
    for oid in reg.order_ids:
        reg.order_customers[oid] = reg.customer_ids[0]
        reg.order_statuses[oid] = "delivered"
        reg.order_totals[oid] = 99.99
        reg.order_dates[oid] = "2024-06-15T14:00:00Z"
    return reg


class TestRegistryDefaults:
    def test_all_lists_empty_on_init(self):
        reg = Registry()
        assert reg.supplier_ids == []
        assert reg.order_ids == []
        assert reg.paid_order_ids == []

    def test_all_dicts_empty_on_init(self):
        reg = Registry()
        assert reg.order_customers == {}
        assert reg.product_prices == {}

    def test_generation_stats_empty(self):
        reg = Registry()
        assert reg.generation_stats == {}


class TestRegistryRecordStat:
    def test_records_stat_stores_values(self):
        reg = Registry()
        reg.record_stat("orders", 1_000_000, 45.0)
        stat = reg.generation_stats["orders"]
        assert stat["count"] == 1_000_000
        assert stat["duration_seconds"] == 45.0
        assert stat["records_per_second"] > 0

    def test_records_per_second_calculated(self):
        reg = Registry()
        reg.record_stat("customers", 500_000, 10.0)
        assert reg.generation_stats["customers"]["records_per_second"] == 50_000.0

    def test_near_zero_duration_doesnt_divide_by_zero(self):
        reg = Registry()
        reg.record_stat("stores", 250, 0.0)
        assert reg.generation_stats["stores"]["records_per_second"] > 0


class TestRegistrySummary:
    def test_summary_contains_domain_names(self, populated_registry: Registry):
        populated_registry.record_stat("suppliers", 2, 0.1)
        populated_registry.record_stat("customers", 10, 0.5)
        summary = populated_registry.summary()
        assert "suppliers" in summary
        assert "customers" in summary

    def test_summary_contains_total(self, populated_registry: Registry):
        populated_registry.record_stat("suppliers", 100, 1.0)
        populated_registry.record_stat("orders", 900, 2.0)
        summary = populated_registry.summary()
        assert "TOTAL" in summary


class TestRegistrySerialization:
    def test_save_and_load_roundtrip(self, populated_registry: Registry, tmp_path: Path):
        path = tmp_path / "registry.json"
        populated_registry.save(path)
        loaded = Registry.load(path)
        assert loaded.supplier_ids == populated_registry.supplier_ids
        assert loaded.order_ids == populated_registry.order_ids
        assert loaded.customer_segments == populated_registry.customer_segments
        assert loaded.product_prices == populated_registry.product_prices

    def test_save_creates_parent_dirs(self, populated_registry: Registry, tmp_path: Path):
        path = tmp_path / "nested" / "dir" / "registry.json"
        populated_registry.save(path)
        assert path.exists()

    def test_save_produces_valid_json(self, populated_registry: Registry, tmp_path: Path):
        path = tmp_path / "registry.json"
        populated_registry.save(path)
        data = json.loads(path.read_text())
        assert "supplier_ids" in data
        assert "order_ids" in data

    def test_load_empty_registry(self, tmp_path: Path):
        path = tmp_path / "empty.json"
        Registry().save(path)
        loaded = Registry.load(path)
        assert loaded.order_ids == []
        assert loaded.generation_stats == {}
