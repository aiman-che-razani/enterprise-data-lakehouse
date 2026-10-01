"""
Order data generator — 1,000,000 orders (full scale).

Orders are distributed across the date range with:
  - Weekday bias (Mon–Thu peak, weekend trough)
  - Seasonal spikes: Black Friday/Cyber Monday (Nov), Valentine's (Feb),
    Mother's Day (May), Back-to-School (Aug), Holiday (Dec)
  - Customer-segment-weighted order frequency
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator

_ORDER_STATUSES = {
    "delivered": 0.68,
    "shipped": 0.12,
    "processing": 0.07,
    "confirmed": 0.05,
    "cancelled": 0.06,
    "returned": 0.02,
}

_PAYMENT_STATUSES = {
    "delivered": "paid",
    "shipped": "paid",
    "processing": "paid",
    "confirmed": "paid",
    "cancelled": "failed",
    "returned": "partially_refunded",
}

_CARRIERS = {
    "FedEx": 0.35,
    "UPS": 0.30,
    "USPS": 0.20,
    "DHL": 0.10,
    "OnTrac": 0.05,
}

# Seasonal multiplier by month (1.0 = average)
_MONTHLY_WEIGHTS = [
    0.75,
    0.80,
    0.90,
    0.85,
    0.90,
    0.85,  # Jan–Jun
    0.85,
    0.90,
    0.95,
    1.0,
    1.4,
    1.7,  # Jul–Dec (Nov–Dec spike)
]


def _seasonal_weight(d: date) -> float:
    """Return a relative order probability weight for a given date."""
    base = _MONTHLY_WEIGHTS[d.month - 1]
    # Black Friday week boost
    if d.month == 11 and 24 <= d.day <= 30:
        base *= 2.5
    # Cyber Monday
    if d.month == 12 and d.day <= 3:
        base *= 2.0
    # Christmas week
    if d.month == 12 and 20 <= d.day <= 24:
        base *= 1.8
    # Valentine's Day
    if d.month == 2 and 10 <= d.day <= 14:
        base *= 1.4
    # Mother's Day (second Sunday in May — approx May 8–14)
    if d.month == 5 and 8 <= d.day <= 14:
        base *= 1.3
    # Weekday bias (Mon=0, Sun=6)
    dow = d.weekday()
    weekday_mult = [1.1, 1.1, 1.05, 1.05, 1.0, 0.85, 0.8][dow]
    return base * weekday_mult


class OrdersGenerator(BaseGenerator):
    domain = "orders"
    output_format = "json"

    def record_count(self) -> int:
        return self.config.counts.orders

    def _build_date_weights(self) -> tuple[list[date], list[float]]:
        """Pre-compute CDF of date weights for the entire date range."""
        dates: list[date] = []
        weights: list[float] = []
        cur = self.config.start_date
        while cur <= self.config.end_date:
            dates.append(cur)
            weights.append(_seasonal_weight(cur))
            cur += timedelta(days=1)
        total = sum(weights)
        probs = [w / total for w in weights]
        return dates, probs

    def generate(self) -> list[dict[str, Any]]:
        self._dates, self._date_probs = self._build_date_weights()
        # The customer registry is complete before orders start: index it by segment once. Filtering all
        # customers for every order made generation O(orders x customers) - infeasible at full scale.
        self._customers_by_segment: dict[str, list[str]] = {}
        for cid, seg in self.registry.customer_segments.items():
            self._customers_by_segment.setdefault(seg, []).append(cid)
        return super().generate()

    def _generate_record(self, index: int) -> dict[str, Any]:
        order_id = f"ORD-{index + 1:010d}"

        # Pick customer — higher segments order more
        if not self.registry.customer_ids:
            customer_id = f"CUST-{int(self.rng.integers(1, 1000)):08d}"
        else:
            # Weight customers by segment order frequency
            weights = self.config.orders_per_year_by_segment
            # Pick a segment proportionally to expected orders
            seg_choice = self._weighted_choice(weights)
            seg_customers = self._customers_by_segment.get(seg_choice, [])
            customer_id = (
                self._choose(seg_customers)
                if seg_customers
                else self._choose(self.registry.customer_ids)
            )

        # Pick date from seasonal distribution
        date_idx = int(self.rng.choice(len(self._dates), p=self._date_probs))
        order_date = self._dates[date_idx]

        # Order time: peak in evening (17–21h), sparse overnight
        hour_weights = [
            0.3,
            0.2,
            0.15,
            0.1,
            0.1,
            0.15,
            0.4,
            0.7,
            1.0,
            1.1,
            1.1,
            1.0,
            1.0,
            1.0,
            0.95,
            0.95,
            1.0,
            1.2,
            1.3,
            1.3,
            1.2,
            1.0,
            0.8,
            0.5,
        ]
        hour = int(self.rng.choice(24, p=[w / sum(hour_weights) for w in hour_weights]))
        minute = int(self.rng.integers(0, 60))
        created_at = datetime(
            order_date.year,
            order_date.month,
            order_date.day,
            hour,
            minute,
            int(self.rng.integers(0, 60)),
            tzinfo=UTC,
        )

        channel = self._weighted_choice(self.config.channel_weights)
        store_id = (
            self._choose(self.registry.store_ids)
            if channel == "store" and self.registry.store_ids
            else None
        )

        # Financials — filled in by order_lines generator, approximate here
        subtotal = round(float(self.rng.lognormal(mean=3.5, sigma=1.0)), 2)
        subtotal = max(4.99, subtotal)
        discount_pct = float(
            self.rng.choice(
                [0, 0, 0, 0.05, 0.10, 0.15, 0.20, 0.25],
                p=[0.5, 0.1, 0.1, 0.1, 0.07, 0.05, 0.05, 0.03],
            )
        )
        discount_amount = round(subtotal * discount_pct, 2)
        tax_rate = float(self.rng.uniform(0.06, 0.11))
        tax_amount = round((subtotal - discount_amount) * tax_rate, 2)
        shipping = round(
            float(
                self.rng.choice(
                    [0, 0, 4.99, 7.99, 12.99, 19.99], p=[0.30, 0.20, 0.20, 0.15, 0.10, 0.05]
                )
            ),
            2,
        )
        total = round(subtotal - discount_amount + tax_amount + shipping, 2)

        status = self._weighted_choice(_ORDER_STATUSES)
        payment_status = _PAYMENT_STATUSES[status]

        record: dict[str, Any] = {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": self._iso_date(order_date),
            "channel": channel,
            "store_id": store_id,
            "status": status,
            "payment_status": payment_status,
            "subtotal": subtotal,
            "discount_amount": discount_amount,
            "tax_amount": tax_amount,
            "shipping_amount": shipping,
            "total_amount": total,
            "currency": "USD",
            "promo_code": (
                f"PROMO{int(self.rng.integers(1000, 9999))}"
                if discount_pct > 0 and self.rng.random() > 0.5
                else None
            ),
            "is_gift": bool(self.rng.random() < 0.08),
            "created_at": self._iso_dt(created_at),
            "updated_at": self._iso_dt(created_at + timedelta(hours=int(self.rng.integers(0, 72)))),
            "source_system": "ecommerce",
        }

        # Register
        self.registry.order_ids.append(order_id)
        self.registry.order_customers[order_id] = customer_id
        self.registry.order_stores[order_id] = store_id
        self.registry.order_dates[order_id] = record["created_at"]
        self.registry.order_totals[order_id] = total
        self.registry.order_statuses[order_id] = status

        return record
