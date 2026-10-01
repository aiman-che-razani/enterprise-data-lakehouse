"""Returns data generator — ~150,000 return line items (full scale)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator
from src.common.logging import get_logger

_RETURN_STATUSES = {
    "approved": 0.50,
    "received": 0.25,
    "processed": 0.15,
    "refunded": 0.08,
    "rejected": 0.02,
}

_CONDITIONS = {
    "unopened": 0.20,
    "like_new": 0.30,
    "good": 0.25,
    "acceptable": 0.15,
    "damaged": 0.10,
}

# 15% of delivered orders have at least one return
_RETURN_RATE = 0.15


class ReturnsGenerator(BaseGenerator):
    domain = "returns"
    output_format = "csv"

    def record_count(self) -> int:
        return self.config.counts.returns

    def generate(self) -> list[dict[str, Any]]:
        log = get_logger(__name__)
        t0 = time.perf_counter()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Draw from paid/delivered orders — partial returns are common
        source_orders = self.registry.paid_order_ids or [
            oid for oid, s in self.registry.order_statuses.items() if s in ("delivered", "returned")
        ]

        target = self.config.counts.returns
        records: list[dict[str, Any]] = []
        return_idx = 1

        for order_id in self.rng.permutation(source_orders):  # type: ignore[call-overload]
            if len(records) >= target:
                break
            if self.rng.random() > _RETURN_RATE:
                continue

            lines = self.registry.order_lines_by_order.get(order_id, [])
            n_lines = min(
                len(lines),
                int(self.rng.choice([1, 2, 3], p=[0.75, 0.20, 0.05])) if lines else 1,
            )
            sampled_lines = list(self.rng.choice(lines, size=n_lines, replace=False)) if lines else [None]  # type: ignore[call-overload]

            order_dt_str = self.registry.order_dates.get(order_id)
            order_dt = (
                datetime.strptime(order_dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
                if order_dt_str
                else self._random_datetime()
            )

            for line_id in sampled_lines:
                detail = self.registry.line_details.get(str(line_id), {}) if line_id else {}
                sku = detail.get("sku") or (
                    self._choose(self.registry.active_skus) if self.registry.active_skus else "UNK"
                )
                quantity = min(detail.get("quantity", 1), int(self.rng.integers(1, 3)))
                unit_price = detail.get("unit_price") or float(self.rng.uniform(9.99, 199.99))
                refund_amount = round(unit_price * quantity, 2)

                # Return initiated 1-45 days after order
                initiated_offset = int(self.rng.integers(1, 46))
                initiated_at = order_dt + timedelta(days=initiated_offset)
                received_offset = int(self.rng.integers(2, 10))
                received_at = initiated_at + timedelta(days=received_offset)

                status = self._weighted_choice(_RETURN_STATUSES)
                condition = self._weighted_choice(_CONDITIONS)

                records.append(
                    {
                        "return_id": f"RET-{return_idx:010d}",
                        "order_id": order_id,
                        "order_line_id": line_id or "",
                        "customer_id": self.registry.order_customers.get(order_id, ""),
                        "sku": sku,
                        "product_name": self.registry.product_names.get(sku, f"Product {sku}"),
                        "quantity": quantity,
                        "unit_price": round(unit_price, 2),
                        "refund_amount": refund_amount,
                        "reason": self._weighted_choice(self.config.return_reason_weights),
                        "status": status,
                        "condition": condition,
                        "restocking_fee": (
                            round(refund_amount * 0.15, 2) if condition == "damaged" else 0.0
                        ),
                        "is_restockable": condition not in ("damaged",),
                        "initiated_at": self._iso_dt(initiated_at),
                        "received_at": self._iso_dt(received_at),
                        "processed_at": (
                            self._iso_dt(received_at + timedelta(days=int(self.rng.integers(1, 5))))
                            if status in ("processed", "refunded")
                            else None
                        ),
                        "refunded_at": (
                            self._iso_dt(received_at + timedelta(days=int(self.rng.integers(2, 8))))
                            if status == "refunded"
                            else None
                        ),
                        "created_at": self._iso_dt(initiated_at),
                    }
                )
                self.registry.returned_order_ids.append(order_id)
                return_idx += 1

        self._write_csv(records)
        duration = time.perf_counter() - t0
        self.registry.record_stat(self.domain, len(records), duration)
        log.info("generator_complete", domain=self.domain, count=len(records))
        return records

    def _generate_record(self, index: int) -> dict[str, Any]:
        raise NotImplementedError("ReturnsGenerator overrides generate() directly")
