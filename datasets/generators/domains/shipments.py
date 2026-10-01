"""Shipment data generator — 1 shipment per shipped/delivered order."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator
from src.common.logging import get_logger

_CARRIERS = {
    "FedEx": 0.35,
    "UPS": 0.30,
    "USPS": 0.20,
    "DHL": 0.10,
    "OnTrac": 0.05,
}

_ELIGIBLE_STATUSES = {"shipped", "delivered", "processing", "confirmed"}

_WAREHOUSES = [
    "WH-LAX-01",
    "WH-ORD-01",
    "WH-JFK-01",
    "WH-DFW-01",
    "WH-SEA-01",
    "WH-ATL-01",
    "WH-PHX-01",
    "WH-MIA-01",
]

_SHIPPING_SERVICES = {
    "FedEx": ["FedEx Ground", "FedEx Express Saver", "FedEx 2Day", "FedEx Overnight"],
    "UPS": ["UPS Ground", "UPS 3 Day Select", "UPS 2nd Day Air", "UPS Next Day Air"],
    "USPS": ["USPS Priority Mail", "USPS Ground Advantage", "USPS Priority Mail Express"],
    "DHL": ["DHL Ground", "DHL Express"],
    "OnTrac": ["OnTrac Ground", "OnTrac Sunrise"],
}


class ShipmentsGenerator(BaseGenerator):
    domain = "shipments"
    output_format = "json"

    def record_count(self) -> int:
        return self.config.counts.shipments

    def generate(self) -> list[dict[str, Any]]:
        log = get_logger(__name__)
        t0 = time.perf_counter()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        eligible = [
            oid
            for oid in self.registry.order_ids
            if self.registry.order_statuses.get(oid) in _ELIGIBLE_STATUSES
        ]

        records: list[dict[str, Any]] = []
        for order_id in eligible:
            records.append(self._shipment_for_order(order_id))
            self.registry.shipped_order_ids.append(order_id)

        self._write_json(records)
        duration = time.perf_counter() - t0
        self.registry.record_stat(self.domain, len(records), duration)
        log.info("generator_complete", domain=self.domain, count=len(records))
        return records

    def _shipment_for_order(self, order_id: str) -> dict[str, Any]:
        order_status = self.registry.order_statuses.get(order_id, "shipped")
        order_dt_str = self.registry.order_dates.get(order_id)

        if order_dt_str:
            order_dt = datetime.strptime(order_dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        else:
            order_dt = self._random_datetime()

        # Shipped 1-3 days after order
        ship_offset = int(self.rng.integers(1, 4))
        ship_dt = order_dt + timedelta(days=ship_offset)

        carrier = self._weighted_choice(_CARRIERS)
        service = self._choose(_SHIPPING_SERVICES[carrier])

        # Estimated delivery: 1-7 days depending on service tier
        express = any(kw in service for kw in ("Overnight", "Next Day", "Express", "2Day", "2nd"))
        est_days = int(self.rng.integers(1, 3)) if express else int(self.rng.integers(3, 8))
        est_delivery = ship_dt + timedelta(days=est_days)

        # Actual delivery: within ±1 day of estimate for most, late for some
        actual_delivery: datetime | None = None
        if order_status == "delivered":
            delay = int(
                self.rng.choice([-1, 0, 0, 0, 1, 2], p=[0.05, 0.40, 0.25, 0.15, 0.10, 0.05])
            )
            actual_delivery = est_delivery + timedelta(days=delay)

        shipment_status = {
            "delivered": "delivered",
            "shipped": "in_transit",
            "processing": "label_created",
            "confirmed": "label_created",
        }.get(order_status, "in_transit")

        # Small chance of exception
        if shipment_status == "in_transit" and self.rng.random() < 0.03:
            shipment_status = "exception"

        weight_kg = round(float(self.rng.uniform(0.1, 15.0)), 3)
        dims_cm = (
            round(float(self.rng.uniform(5, 60)), 1),
            round(float(self.rng.uniform(5, 40)), 1),
            round(float(self.rng.uniform(2, 30)), 1),
        )

        return {
            "shipment_id": f"SHIP-{order_id.split('-', 1)[1]}",
            "order_id": order_id,
            "customer_id": self.registry.order_customers.get(order_id, ""),
            "warehouse_id": self._choose(_WAREHOUSES),
            "carrier": carrier,
            "service": service,
            "tracking_number": self.fake.lexify("1Z" + "?" * 16).upper(),
            "status": shipment_status,
            "weight_kg": weight_kg,
            "length_cm": dims_cm[0],
            "width_cm": dims_cm[1],
            "height_cm": dims_cm[2],
            "shipping_cost": round(float(self.rng.uniform(2.99, 24.99)), 2),
            "is_signature_required": bool(self.rng.random() < 0.12),
            "ship_date": self._iso_dt(ship_dt),
            "estimated_delivery_date": self._iso_dt(est_delivery),
            "actual_delivery_date": self._iso_dt(actual_delivery) if actual_delivery else None,
            "created_at": self._iso_dt(order_dt),
            "updated_at": self._iso_dt(actual_delivery or ship_dt),
        }

    def _generate_record(self, index: int) -> dict[str, Any]:
        raise NotImplementedError("ShipmentsGenerator overrides generate() directly")
