"""Inventory snapshot generator — product × store stock levels (full scale: ~500K)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator

_INVENTORY_CLASSES = {
    "A": 0.20,  # Fast-moving: <30 days supply on hand
    "B": 0.50,  # Medium: 30-90 days
    "C": 0.30,  # Slow-moving: 90+ days
}


class InventoryGenerator(BaseGenerator):
    domain = "inventory"
    output_format = "json"

    def record_count(self) -> int:
        return self.config.counts.inventory

    def _generate_record(self, index: int) -> dict[str, Any]:
        """Each record = one SKU stocked at one store on a snapshot date."""
        skus = self.registry.product_skus or [f"SKU-{index:08d}"]
        stores = self.registry.store_ids or ["STR-00001"]

        sku = self._choose(skus)
        store_id = self._choose(stores)
        snapshot_date = self._random_date()

        cost = self.registry.product_costs.get(sku, float(self.rng.uniform(2.0, 80.0)))
        abc_class = self._weighted_choice(_INVENTORY_CLASSES)

        # Stock levels vary by ABC class
        if abc_class == "A":
            qty_on_hand = int(self.rng.integers(5, 150))
            reorder_point = int(self.rng.integers(20, 50))
            reorder_qty = int(self.rng.integers(50, 200))
        elif abc_class == "B":
            qty_on_hand = int(self.rng.integers(10, 300))
            reorder_point = int(self.rng.integers(10, 30))
            reorder_qty = int(self.rng.integers(25, 100))
        else:
            qty_on_hand = int(self.rng.integers(0, 500))
            reorder_point = int(self.rng.integers(5, 20))
            reorder_qty = int(self.rng.integers(10, 50))

        qty_reserved = min(qty_on_hand, int(self.rng.integers(0, max(1, qty_on_hand // 4))))
        qty_available = qty_on_hand - qty_reserved
        qty_on_order = (
            int(self.rng.integers(reorder_qty // 2, reorder_qty * 2))
            if qty_available < reorder_point
            else 0
        )

        last_received = snapshot_date - timedelta(days=int(self.rng.integers(1, 90)))
        last_counted = snapshot_date - timedelta(days=int(self.rng.integers(0, 30)))

        aisle = self._choose(list("ABCDEFGHIJKLMNOPQRST"))
        shelf = int(self.rng.integers(1, 6))
        bin_pos = int(self.rng.integers(1, 20))

        return {
            "inventory_id": f"INV-{index + 1:010d}",
            "sku": sku,
            "store_id": store_id,
            "snapshot_date": self._iso_date(snapshot_date),
            "abc_class": abc_class,
            "quantity_on_hand": qty_on_hand,
            "quantity_reserved": qty_reserved,
            "quantity_available": qty_available,
            "quantity_on_order": qty_on_order,
            "reorder_point": reorder_point,
            "reorder_quantity": reorder_qty,
            "days_of_supply": round(qty_available / max(1, float(self.rng.uniform(0.5, 10))), 1),
            "cost_per_unit": round(cost, 4),
            "total_inventory_value": round(qty_on_hand * cost, 2),
            "bin_location": f"{aisle}{shelf:02d}-{bin_pos:03d}",
            "is_below_reorder_point": qty_available < reorder_point,
            "is_out_of_stock": qty_available == 0,
            "last_received_date": self._iso_date(last_received),
            "last_count_date": self._iso_date(last_counted),
            "created_at": self._iso_dt(self._random_datetime(end=snapshot_date)),
            "updated_at": self._iso_dt(
                self._random_datetime(start=last_received, end=snapshot_date)
            ),
        }
