"""Payment data generator — 1 payment per order (full scale)."""

from __future__ import annotations

import time
from typing import Any

from datasets.generators.base_generator import BaseGenerator
from src.common.logging import get_logger

_PROCESSORS = {
    "Stripe": 0.40,
    "Adyen": 0.25,
    "Braintree": 0.15,
    "Square": 0.10,
    "PayPal": 0.10,
}

_CARD_NETWORKS = {
    "Visa": 0.45,
    "Mastercard": 0.30,
    "Amex": 0.15,
    "Discover": 0.10,
}

_FAILURE_CODES = [
    "insufficient_funds",
    "card_declined",
    "expired_card",
    "do_not_honor",
    "stolen_card",
    "processing_error",
]


class PaymentsGenerator(BaseGenerator):
    domain = "payments"
    output_format = "json"

    def record_count(self) -> int:
        return self.config.counts.payments

    def generate(self) -> list[dict[str, Any]]:
        log = get_logger(__name__)
        t0 = time.perf_counter()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        records: list[dict[str, Any]] = []
        for order_id in self.registry.order_ids:
            records.append(self._payment_for_order(order_id))

        self._write_json(records)
        duration = time.perf_counter() - t0
        self.registry.record_stat(self.domain, len(records), duration)
        log.info("generator_complete", domain=self.domain, count=len(records))
        return records

    def _payment_for_order(self, order_id: str) -> dict[str, Any]:
        payment_id = f"PAY-{order_id.split('-', 1)[1]}"
        status = self.registry.order_statuses.get(order_id, "delivered")
        amount = self.registry.order_totals.get(
            order_id, round(float(self.rng.uniform(10, 500)), 2)
        )
        order_dt_str = self.registry.order_dates.get(order_id)

        payment_method = self._weighted_choice(self.config.payment_method_weights)

        # Payment typically processed within minutes of order creation
        # For failed orders (cancelled), payment was rejected
        is_failed = status == "cancelled" and self.rng.random() > 0.3
        payment_status = (
            "failed"
            if is_failed
            else (
                "refunded"
                if status == "returned"
                else (
                    "partially_refunded"
                    if status in ("returned",) and self.rng.random() > 0.5
                    else "paid"
                )
            )
        )

        refund_amount = (
            round(amount * float(self.rng.uniform(0.5, 1.0)), 2)
            if payment_status in ("refunded", "partially_refunded")
            else 0.0
        )

        card_last4 = (
            str(int(self.rng.integers(1000, 9999)))
            if payment_method in ("credit_card", "debit_card")
            else None
        )
        card_network = self._weighted_choice(_CARD_NETWORKS) if card_last4 else None
        card_exp = (
            f"{int(self.rng.integers(1, 13)):02d}/{int(self.rng.integers(25, 31))}"
            if card_last4
            else None
        )

        record: dict[str, Any] = {
            "payment_id": payment_id,
            "order_id": order_id,
            "customer_id": self.registry.order_customers.get(order_id, ""),
            "payment_method": payment_method,
            "payment_processor": self._weighted_choice(_PROCESSORS),
            "amount": amount,
            "currency": "USD",
            "status": payment_status,
            "card_last4": card_last4,
            "card_network": card_network,
            "card_exp": card_exp,
            "refund_amount": refund_amount,
            "refund_reason": (
                self._weighted_choice(
                    {
                        "customer_request": 0.40,
                        "fraud": 0.05,
                        "item_not_received": 0.25,
                        "item_not_as_described": 0.20,
                        "duplicate": 0.10,
                    }
                )
                if refund_amount > 0
                else None
            ),
            "failure_code": (self._choose(_FAILURE_CODES) if is_failed else None),
            "processor_transaction_id": (
                self.fake.lexify("??" * 16).upper() if not is_failed else None
            ),
            "created_at": order_dt_str or self._iso_dt(self._random_datetime()),
            "updated_at": order_dt_str or self._iso_dt(self._random_datetime()),
        }

        if payment_status == "paid":
            self.registry.paid_order_ids.append(order_id)

        return record

    def _generate_record(self, index: int) -> dict[str, Any]:
        raise NotImplementedError("PaymentsGenerator overrides generate() directly")
