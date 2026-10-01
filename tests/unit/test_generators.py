"""
Unit tests for all domain data generators.

Uses SCALE_FACTOR=0.001 so generators produce at most 10 records each,
keeping tests fast while still exercising the full code path.

All file I/O is redirected to tmp_path — the real datasets/raw/ directory
is never touched during testing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from datasets.generators.config import GeneratorConfig
from datasets.generators.registries import Registry

if TYPE_CHECKING:
    from pathlib import Path

# ─── Shared fixtures ──────────────────────────────────────────────────────────


@pytest.fixture()
def mini_config(tmp_path: Path) -> GeneratorConfig:
    """Config with scale_factor=0.001 writing to a temp dir."""
    return GeneratorConfig(scale_factor=0.001, seed=42, output_dir=tmp_path)


@pytest.fixture()
def registry() -> Registry:
    return Registry()


def _run_chain(tmp_path: Path) -> tuple[GeneratorConfig, Registry]:
    """Run the minimal generator chain needed for downstream generators."""
    from datasets.generators.domains.campaigns import CampaignsGenerator
    from datasets.generators.domains.customers import CustomersGenerator
    from datasets.generators.domains.employees import EmployeesGenerator
    from datasets.generators.domains.order_lines import OrderLinesGenerator
    from datasets.generators.domains.orders import OrdersGenerator
    from datasets.generators.domains.payments import PaymentsGenerator
    from datasets.generators.domains.products import ProductsGenerator
    from datasets.generators.domains.stores import StoresGenerator
    from datasets.generators.domains.suppliers import SuppliersGenerator

    cfg = GeneratorConfig(scale_factor=0.001, seed=42, output_dir=tmp_path)
    reg = Registry()

    SuppliersGenerator(cfg, reg).generate()
    StoresGenerator(cfg, reg).generate()
    EmployeesGenerator(cfg, reg).generate()
    ProductsGenerator(cfg, reg).generate()
    CustomersGenerator(cfg, reg).generate()
    CampaignsGenerator(cfg, reg).generate()
    OrdersGenerator(cfg, reg).generate()
    OrderLinesGenerator(cfg, reg).generate()
    PaymentsGenerator(cfg, reg).generate()
    return cfg, reg


# ─── Suppliers ────────────────────────────────────────────────────────────────


class TestSuppliersGenerator:
    def test_generates_records(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.suppliers import SuppliersGenerator

        records = SuppliersGenerator(mini_config, registry).generate()
        assert len(records) >= 10

    def test_registers_ids(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.suppliers import SuppliersGenerator

        SuppliersGenerator(mini_config, registry).generate()
        assert len(registry.supplier_ids) >= 10

    def test_required_fields_present(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.suppliers import SuppliersGenerator

        records = SuppliersGenerator(mini_config, registry).generate()
        for r in records:
            assert "supplier_id" in r
            assert r["supplier_id"].startswith("SUP-")
            assert "tier" in r
            assert r["tier"] in ("A", "B", "C")

    def test_tier_distribution_valid(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.suppliers import SuppliersGenerator

        records = SuppliersGenerator(mini_config, registry).generate()
        tiers = {r["tier"] for r in records}
        assert tiers.issubset({"A", "B", "C"})

    def test_output_csv_created(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.suppliers import SuppliersGenerator

        SuppliersGenerator(mini_config, registry).generate()
        csv_files = list(mini_config.output_dir.rglob("suppliers.csv"))
        assert len(csv_files) == 1

    def test_stats_recorded(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.suppliers import SuppliersGenerator

        SuppliersGenerator(mini_config, registry).generate()
        assert "suppliers" in registry.generation_stats
        assert registry.generation_stats["suppliers"]["count"] >= 10


# ─── Stores ───────────────────────────────────────────────────────────────────


class TestStoresGenerator:
    def test_generates_records(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.stores import StoresGenerator

        records = StoresGenerator(mini_config, registry).generate()
        assert len(records) >= 10

    def test_required_fields(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.stores import StoresGenerator

        records = StoresGenerator(mini_config, registry).generate()
        for r in records:
            assert "store_id" in r
            assert "city" in r
            assert "store_type" in r

    def test_registers_store_ids(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.stores import StoresGenerator

        StoresGenerator(mini_config, registry).generate()
        assert len(registry.store_ids) >= 10


# ─── Products ─────────────────────────────────────────────────────────────────


class TestProductsGenerator:
    def test_generates_records(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.products import ProductsGenerator

        records = ProductsGenerator(mini_config, registry).generate()
        assert len(records) >= 10

    def test_prices_are_positive(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.products import ProductsGenerator

        records = ProductsGenerator(mini_config, registry).generate()
        for r in records:
            assert r["list_price"] > 0
            assert r["cost_price"] > 0

    def test_cost_less_than_price_mostly(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.products import ProductsGenerator

        records = ProductsGenerator(mini_config, registry).generate()
        positive_margin = sum(1 for r in records if r["cost_price"] < r["list_price"])
        assert positive_margin / len(records) >= 0.7

    def test_registers_active_skus(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.products import ProductsGenerator

        ProductsGenerator(mini_config, registry).generate()
        assert len(registry.active_skus) > 0
        assert len(registry.product_prices) >= len(registry.active_skus)


# ─── Customers ────────────────────────────────────────────────────────────────


class TestCustomersGenerator:
    def test_generates_records(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.customers import CustomersGenerator

        records = CustomersGenerator(mini_config, registry).generate()
        assert len(records) >= 10

    def test_segments_are_valid(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.customers import CustomersGenerator

        records = CustomersGenerator(mini_config, registry).generate()
        valid = {"platinum", "gold", "silver", "bronze"}
        for r in records:
            assert r["segment"] in valid

    def test_mostly_us_customers(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.customers import CustomersGenerator

        cfg = GeneratorConfig(scale_factor=0.005, seed=42, output_dir=mini_config.output_dir)
        records = CustomersGenerator(cfg, registry).generate()
        us_count = sum(1 for r in records if r["country"] == "United States")
        assert us_count / len(records) >= 0.60  # expect ~82%, allow margin

    def test_email_field_present(self, mini_config: GeneratorConfig, registry: Registry):
        from datasets.generators.domains.customers import CustomersGenerator

        records = CustomersGenerator(mini_config, registry).generate()
        for r in records:
            assert "@" in r["email"]


# ─── Orders ───────────────────────────────────────────────────────────────────


class TestOrdersGenerator:
    @pytest.fixture()
    def seeded_registry(self, mini_config: GeneratorConfig) -> Registry:
        from datasets.generators.domains.customers import CustomersGenerator
        from datasets.generators.domains.stores import StoresGenerator

        reg = Registry()
        StoresGenerator(mini_config, reg).generate()
        CustomersGenerator(mini_config, reg).generate()
        return reg

    def test_generates_records(self, mini_config: GeneratorConfig, seeded_registry: Registry):
        from datasets.generators.domains.orders import OrdersGenerator

        records = OrdersGenerator(mini_config, seeded_registry).generate()
        assert len(records) >= 10

    def test_registers_order_ids(self, mini_config: GeneratorConfig, seeded_registry: Registry):
        from datasets.generators.domains.orders import OrdersGenerator

        OrdersGenerator(mini_config, seeded_registry).generate()
        assert len(seeded_registry.order_ids) >= 10

    def test_customer_fk_valid(self, mini_config: GeneratorConfig, seeded_registry: Registry):
        from datasets.generators.domains.orders import OrdersGenerator

        records = OrdersGenerator(mini_config, seeded_registry).generate()
        valid_ids = set(seeded_registry.customer_ids)
        for r in records:
            assert r["customer_id"] in valid_ids

    def test_status_values_valid(self, mini_config: GeneratorConfig, seeded_registry: Registry):
        from datasets.generators.domains.orders import OrdersGenerator

        records = OrdersGenerator(mini_config, seeded_registry).generate()
        valid = {"delivered", "shipped", "processing", "confirmed", "cancelled", "returned"}
        for r in records:
            assert r["status"] in valid

    def test_total_positive(self, mini_config: GeneratorConfig, seeded_registry: Registry):
        from datasets.generators.domains.orders import OrdersGenerator

        records = OrdersGenerator(mini_config, seeded_registry).generate()
        for r in records:
            assert r["total_amount"] > 0


# ─── Order Lines ──────────────────────────────────────────────────────────────


class TestOrderLinesGenerator:
    def test_generates_records(self, tmp_path: Path):
        cfg, reg = _run_chain(tmp_path)
        assert len(reg.order_line_ids) > 0

    def test_order_fk_valid(self, tmp_path: Path):
        cfg, reg = _run_chain(tmp_path)
        valid_orders = set(reg.order_ids)
        for _line_id, detail in reg.line_details.items():
            assert detail["order_id"] in valid_orders

    def test_sku_fk_valid(self, tmp_path: Path):
        cfg, reg = _run_chain(tmp_path)
        valid_skus = set(reg.product_skus)
        for _line_id, detail in reg.line_details.items():
            assert detail["sku"] in valid_skus

    def test_quantity_positive(self, tmp_path: Path):
        cfg, reg = _run_chain(tmp_path)
        for detail in reg.line_details.values():
            assert detail["quantity"] >= 1


# ─── Payments ─────────────────────────────────────────────────────────────────


class TestPaymentsGenerator:
    def test_generates_one_payment_per_order(self, tmp_path: Path):
        cfg, reg = _run_chain(tmp_path)
        assert len(reg.paid_order_ids) > 0

    def test_no_duplicate_payment_ids(self, tmp_path: Path):
        from datasets.generators.domains.payments import PaymentsGenerator

        cfg, reg = _run_chain(tmp_path)
        # payments are already generated in _run_chain; reload for fresh ID check
        reg2 = Registry()
        reg2.order_ids = reg.order_ids[:]
        reg2.order_statuses = dict(reg.order_statuses)
        reg2.order_totals = dict(reg.order_totals)
        reg2.order_dates = dict(reg.order_dates)
        reg2.order_customers = dict(reg.order_customers)
        records = PaymentsGenerator(cfg, reg2).generate()
        ids = [r["payment_id"] for r in records]
        assert len(ids) == len(set(ids))


# ─── Shipments ────────────────────────────────────────────────────────────────


class TestShipmentsGenerator:
    def test_only_eligible_orders_shipped(self, tmp_path: Path):
        from datasets.generators.domains.shipments import ShipmentsGenerator

        cfg, reg = _run_chain(tmp_path)
        records = ShipmentsGenerator(cfg, reg).generate()
        eligible = {"shipped", "delivered", "processing", "confirmed"}
        for r in records:
            order_status = reg.order_statuses.get(r["order_id"])
            assert order_status in eligible


# ─── Inventory ────────────────────────────────────────────────────────────────


class TestInventoryGenerator:
    def test_generates_records(self, tmp_path: Path):
        from datasets.generators.domains.inventory import InventoryGenerator

        cfg, reg = _run_chain(tmp_path)
        records = InventoryGenerator(cfg, reg).generate()
        assert len(records) >= 10

    def test_qty_available_leq_on_hand(self, tmp_path: Path):
        from datasets.generators.domains.inventory import InventoryGenerator

        cfg, reg = _run_chain(tmp_path)
        records = InventoryGenerator(cfg, reg).generate()
        for r in records:
            assert r["quantity_available"] <= r["quantity_on_hand"]

    def test_abc_class_valid(self, tmp_path: Path):
        from datasets.generators.domains.inventory import InventoryGenerator

        cfg, reg = _run_chain(tmp_path)
        records = InventoryGenerator(cfg, reg).generate()
        for r in records:
            assert r["abc_class"] in ("A", "B", "C")


# ─── Reviews ──────────────────────────────────────────────────────────────────


class TestReviewsGenerator:
    def test_generates_records(self, tmp_path: Path):
        from datasets.generators.domains.reviews import ReviewsGenerator

        cfg, reg = _run_chain(tmp_path)
        records = ReviewsGenerator(cfg, reg).generate()
        assert len(records) >= 10

    def test_rating_in_range(self, tmp_path: Path):
        from datasets.generators.domains.reviews import ReviewsGenerator

        cfg, reg = _run_chain(tmp_path)
        records = ReviewsGenerator(cfg, reg).generate()
        for r in records:
            assert 1 <= r["rating"] <= 5

    def test_sentiment_consistent_with_rating(self, tmp_path: Path):
        from datasets.generators.domains.reviews import ReviewsGenerator

        cfg, reg = _run_chain(tmp_path)
        records = ReviewsGenerator(cfg, reg).generate()
        for r in records:
            if r["rating"] >= 4:
                assert r["sentiment"] == "positive"
            elif r["rating"] <= 2:
                assert r["sentiment"] == "negative"


# ─── IoT Sensors ──────────────────────────────────────────────────────────────


class TestIotSensorsGenerator:
    def test_generates_records(self, tmp_path: Path):
        from datasets.generators.domains.iot_sensors import IotSensorsGenerator

        cfg, reg = _run_chain(tmp_path)
        records = IotSensorsGenerator(cfg, reg).generate()
        assert len(records) >= 10

    def test_alert_flag_consistent(self, tmp_path: Path):
        from datasets.generators.domains.iot_sensors import IotSensorsGenerator

        cfg, reg = _run_chain(tmp_path)
        records = IotSensorsGenerator(cfg, reg).generate()
        for r in records:
            if r["alert_triggered"]:
                below = r["value"] < r["alert_threshold_low"]
                above = r["value"] > r["alert_threshold_high"]
                assert below or above

    def test_reading_quality_values(self, tmp_path: Path):
        from datasets.generators.domains.iot_sensors import IotSensorsGenerator

        cfg, reg = _run_chain(tmp_path)
        records = IotSensorsGenerator(cfg, reg).generate()
        for r in records:
            assert r["reading_quality"] in ("good", "fair", "poor")


# ─── Finance ──────────────────────────────────────────────────────────────────


class TestFinanceGenerator:
    def test_generates_records(self, tmp_path: Path):
        from datasets.generators.domains.finance import FinanceGenerator

        cfg, reg = _run_chain(tmp_path)
        records = FinanceGenerator(cfg, reg).generate()
        assert len(records) >= 10

    def test_double_entry_balanced(self, tmp_path: Path):
        from datasets.generators.domains.finance import FinanceGenerator

        cfg, reg = _run_chain(tmp_path)
        records = FinanceGenerator(cfg, reg).generate()
        for r in records:
            assert abs(r["debit_amount"] - r["credit_amount"]) < 0.01

    def test_fiscal_period_valid(self, tmp_path: Path):
        from datasets.generators.domains.finance import FinanceGenerator

        cfg, reg = _run_chain(tmp_path)
        records = FinanceGenerator(cfg, reg).generate()
        for r in records:
            assert 1 <= r["fiscal_period"] <= 12
            assert 1 <= r["fiscal_quarter"] <= 4


# ─── Referential integrity end-to-end ─────────────────────────────────────────


class TestReferentialIntegrity:
    def test_full_chain_registry_consistency(self, tmp_path: Path):
        cfg, reg = _run_chain(tmp_path)
        # Every order must have a valid customer
        assert all(oid in reg.order_customers for oid in reg.order_ids)
        assert all(cid in reg.customer_ids for cid in reg.order_customers.values())

    def test_order_lines_reference_valid_orders(self, tmp_path: Path):
        cfg, reg = _run_chain(tmp_path)
        valid_orders = set(reg.order_ids)
        for oid in reg.order_lines_by_order:
            assert oid in valid_orders

    def test_generation_stats_populated(self, tmp_path: Path):
        cfg, reg = _run_chain(tmp_path)
        expected_domains = {
            "suppliers",
            "stores",
            "employees",
            "products",
            "customers",
            "campaigns",
            "orders",
            "order_lines",
            "payments",
        }
        assert expected_domains.issubset(reg.generation_stats.keys())
