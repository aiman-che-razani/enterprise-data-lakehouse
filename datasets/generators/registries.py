"""
Shared entity registry for referential integrity across all data generators.

Generators run in dependency order and register their primary keys here.
Later generators draw from the registry to create valid foreign key relationships.

The registry is serialised to JSON after each generator completes, so the
generate_all script can resume from a checkpoint if interrupted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


@dataclass
class Registry:
    """
    In-memory store of generated primary keys and key attributes
    needed for referential integrity.

    Population order:
        suppliers → stores → employees (ref suppliers)
        → products (ref suppliers) → customers
        → campaigns → orders (ref customers, stores)
        → order_lines (ref orders, products)
        → payments (ref orders) → shipments (ref orders)
        → returns (ref orders, order_lines, products, customers)
        → inventory (ref products, stores)
        → reviews (ref customers, products)
        → clickstream (ref customers)
        → iot_sensors (ref stores)
        → finance (standalone GL)
    """

    # ─── Suppliers ────────────────────────────────────────────────────────────
    supplier_ids: list[str] = field(default_factory=list)
    supplier_names: dict[str, str] = field(default_factory=dict)  # id → name
    supplier_tiers: dict[str, str] = field(default_factory=dict)  # id → A/B/C

    # ─── Stores ───────────────────────────────────────────────────────────────
    store_ids: list[str] = field(default_factory=list)
    store_types: dict[str, str] = field(default_factory=dict)  # id → type
    store_cities: dict[str, str] = field(default_factory=dict)  # id → city
    store_regions: dict[str, str] = field(default_factory=dict)  # id → region

    # ─── Employees ────────────────────────────────────────────────────────────
    employee_ids: list[str] = field(default_factory=list)
    employee_departments: dict[str, str] = field(default_factory=dict)

    # ─── Products ─────────────────────────────────────────────────────────────
    product_skus: list[str] = field(default_factory=list)
    product_names: dict[str, str] = field(default_factory=dict)  # sku → name
    product_prices: dict[str, float] = field(default_factory=dict)  # sku → list_price
    product_costs: dict[str, float] = field(default_factory=dict)  # sku → cost_price
    product_categories: dict[str, str] = field(default_factory=dict)  # sku → category_l1
    product_supplier: dict[str, str] = field(default_factory=dict)  # sku → supplier_id
    active_skus: list[str] = field(default_factory=list)  # non-discontinued

    # ─── Customers ────────────────────────────────────────────────────────────
    customer_ids: list[str] = field(default_factory=list)
    customer_segments: dict[str, str] = field(default_factory=dict)  # id → segment
    customer_countries: dict[str, str] = field(default_factory=dict)  # id → country

    # ─── Campaigns ────────────────────────────────────────────────────────────
    campaign_ids: list[str] = field(default_factory=list)
    campaign_types: dict[str, str] = field(default_factory=dict)  # id → type

    # ─── Orders ───────────────────────────────────────────────────────────────
    order_ids: list[str] = field(default_factory=list)
    order_customers: dict[str, str] = field(default_factory=dict)  # order_id → customer_id
    order_stores: dict[str, str | None] = field(default_factory=dict)  # order_id → store_id
    order_dates: dict[str, str] = field(default_factory=dict)  # order_id → created_at ISO
    order_totals: dict[str, float] = field(default_factory=dict)  # order_id → total_amount
    order_statuses: dict[str, str] = field(default_factory=dict)  # order_id → status

    # ─── Order Lines ──────────────────────────────────────────────────────────
    order_line_ids: list[str] = field(default_factory=list)
    order_lines_by_order: dict[str, list[str]] = field(default_factory=dict)
    # line_id → {order_id, sku, quantity, unit_price}
    line_details: dict[str, dict[str, Any]] = field(default_factory=dict)

    # ─── Payments ─────────────────────────────────────────────────────────────
    paid_order_ids: list[str] = field(default_factory=list)  # orders with successful payment

    # ─── Shipments ────────────────────────────────────────────────────────────
    shipped_order_ids: list[str] = field(default_factory=list)

    # ─── Returns ──────────────────────────────────────────────────────────────
    returned_order_ids: list[str] = field(default_factory=list)

    # ─── Statistics (written at runtime) ──────────────────────────────────────
    generation_stats: dict[str, dict[str, Any]] = field(default_factory=dict)

    # ─── Serialisation ────────────────────────────────────────────────────────
    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self._to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> Registry:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return cls(**data)

    def _to_dict(self) -> dict[str, Any]:
        import dataclasses

        return dataclasses.asdict(self)

    def record_stat(self, domain: str, count: int, duration_s: float) -> None:
        self.generation_stats[domain] = {
            "count": count,
            "duration_seconds": round(duration_s, 2),
            "records_per_second": round(count / max(duration_s, 0.001), 0),
        }

    def summary(self) -> str:
        lines = ["", "─" * 60, "  Data Generation Summary", "─" * 60]
        total = 0
        for domain, stats in self.generation_stats.items():
            cnt = stats["count"]
            total += cnt
            lines.append(f"  {domain:<22} {cnt:>10,}  ({stats['duration_seconds']:.1f}s)")
        lines += ["─" * 60, f"  {'TOTAL':<22} {total:>10,}", "─" * 60, ""]
        return "\n".join(lines)
