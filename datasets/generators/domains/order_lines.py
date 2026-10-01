"""
Order line items generator — 2,500,000 lines (full scale).
Average 2.5 lines per order; distribution follows negative binomial.
"""

from __future__ import annotations

from typing import Any

from datasets.generators.base_generator import BaseGenerator


class OrderLinesGenerator(BaseGenerator):
    domain = "order_lines"
    output_format = "json"

    def record_count(self) -> int:
        return self.config.counts.order_lines

    def generate(self) -> list[dict[str, Any]]:
        """Override: generate lines per order, not as a flat loop."""
        import time

        from src.common.logging import get_logger

        log = get_logger(__name__)

        t0 = time.perf_counter()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        all_records: list[dict[str, Any]] = []
        line_global_index = 0
        orders = self.registry.order_ids

        if not orders:
            log.warning("no_orders_in_registry_skipping_order_lines")
            return []

        log.info("generator_started", domain=self.domain, orders=len(orders))

        for order_id in orders:
            n_lines = int(
                self.rng.choice(
                    [1, 2, 3, 4, 5, 6, 7, 8],
                    p=[0.30, 0.28, 0.20, 0.10, 0.06, 0.03, 0.02, 0.01],
                )
            )
            skus_used: set[str] = set()
            order_lines_for_this: list[str] = []

            for _ in range(n_lines):
                line_id = f"LINE-{line_global_index + 1:012d}"
                line_global_index += 1

                # Pick an active SKU not already in this order
                pool = self.registry.active_skus or self.registry.product_skus
                attempts = 0
                sku = self._choose(pool)
                while sku in skus_used and attempts < 10:
                    sku = self._choose(pool)
                    attempts += 1
                skus_used.add(sku)

                list_price = self.registry.product_prices.get(
                    sku, float(self.rng.uniform(9.99, 99.99))
                )
                quantity = int(
                    self.rng.choice(
                        [1, 1, 1, 2, 2, 3, 4, 5], p=[0.40, 0.20, 0.15, 0.10, 0.07, 0.04, 0.03, 0.01]
                    )
                )
                discount_pct = float(
                    self.rng.choice(
                        [0.0, 0.0, 0.05, 0.10, 0.15, 0.20], p=[0.50, 0.20, 0.12, 0.10, 0.05, 0.03]
                    )
                )
                unit_price = round(list_price * (1 - discount_pct), 2)
                line_total = round(unit_price * quantity, 2)

                record: dict[str, Any] = {
                    "order_line_id": line_id,
                    "order_id": order_id,
                    "sku": sku,
                    "product_name": self.registry.product_names.get(sku, f"Product {sku}"),
                    "category": self.registry.product_categories.get(sku, "Unknown"),
                    "quantity": quantity,
                    "unit_list_price": round(list_price, 2),
                    "unit_price": unit_price,
                    "discount_pct": discount_pct,
                    "line_total": line_total,
                    "is_gift_wrap": bool(self.rng.random() < 0.03),
                    "created_at": self.registry.order_dates.get(
                        order_id, self._iso_dt(self._random_datetime())
                    ),
                }

                all_records.append(record)
                order_lines_for_this.append(line_id)
                self.registry.order_line_ids.append(line_id)
                self.registry.line_details[line_id] = {
                    "order_id": order_id,
                    "sku": sku,
                    "quantity": quantity,
                    "unit_price": unit_price,
                }

            self.registry.order_lines_by_order[order_id] = order_lines_for_this

        # Write in chunks
        self._write_json(all_records)

        duration = time.perf_counter() - t0
        self.registry.record_stat(self.domain, len(all_records), duration)
        log.info(
            "generator_complete",
            domain=self.domain,
            count=len(all_records),
            duration_s=round(duration, 2),
        )
        return all_records

    def _generate_record(self, index: int) -> dict[str, Any]:
        raise NotImplementedError("OrderLinesGenerator overrides generate() directly")
