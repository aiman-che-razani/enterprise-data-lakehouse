"""
Generator configuration for the RetailNexus synthetic dataset.

ScaleFactor controls the size of generated datasets:
  - 0.01 = dev/CI (thousands of records, fast)
  - 0.1  = integration testing (tens of thousands)
  - 1.0  = full portfolio demo (millions of records)

All random operations are seeded for reproducibility.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path


@dataclass(frozen=True)
class RecordCounts:
    """Full-scale record counts (scale_factor=1.0)."""

    suppliers: int = 2_000
    employees: int = 5_000
    stores: int = 250
    products: int = 50_000
    customers: int = 500_000
    campaigns: int = 10_000
    orders: int = 1_000_000
    order_lines: int = 2_500_000
    payments: int = 1_000_000
    shipments: int = 800_000
    returns: int = 150_000
    inventory: int = 500_000
    reviews: int = 200_000
    clickstream: int = 5_000_000
    iot_sensors: int = 2_000_000
    finance: int = 500_000


_FULL_COUNTS = RecordCounts()


@dataclass
class GeneratorConfig:
    """
    Top-level configuration for all data generators.

    All generators share a single config instance so they produce
    a consistent, internally consistent synthetic dataset.
    """

    # ─── Reproducibility ──────────────────────────────────────────────────────
    seed: int = 42

    # ─── Scale ────────────────────────────────────────────────────────────────
    scale_factor: float = field(default_factory=lambda: float(os.getenv("SCALE_FACTOR", "1.0")))

    # ─── Date range (2 years of operational history) ──────────────────────────
    start_date: date = date(2024, 1, 1)
    end_date: date = date(2026, 6, 30)

    # ─── Output ───────────────────────────────────────────────────────────────
    output_dir: Path = Path("datasets/raw")
    chunk_size: int = 50_000  # records per JSON file chunk
    compress_json: bool = False  # True for high-volume (clickstream, IoT)

    # ─── Geography ────────────────────────────────────────────────────────────
    us_customer_pct: float = 0.82  # 82% US customers, 18% international
    countries_international: list[str] = field(
        default_factory=lambda: [
            "Canada",
            "United Kingdom",
            "Germany",
            "Australia",
            "France",
            "Japan",
            "Mexico",
            "Brazil",
            "India",
            "Netherlands",
        ]
    )

    # ─── Business rules ───────────────────────────────────────────────────────
    # Customer segment distribution (must sum to 1.0)
    segment_weights: dict[str, float] = field(
        default_factory=lambda: {
            "platinum": 0.05,
            "gold": 0.15,
            "silver": 0.30,
            "bronze": 0.50,
        }
    )

    # Orders per customer per year (Poisson mean, by segment)
    orders_per_year_by_segment: dict[str, float] = field(
        default_factory=lambda: {
            "platinum": 24.0,  # ~2/month
            "gold": 12.0,  # ~1/month
            "silver": 5.0,  # ~quarterly
            "bronze": 2.0,  # ~twice a year
        }
    )

    # Payment method mix
    payment_method_weights: dict[str, float] = field(
        default_factory=lambda: {
            "credit_card": 0.45,
            "debit_card": 0.25,
            "paypal": 0.15,
            "apple_pay": 0.07,
            "gift_card": 0.05,
            "buy_now_pay_later": 0.03,
        }
    )

    # Order channel mix
    channel_weights: dict[str, float] = field(
        default_factory=lambda: {
            "web": 0.45,
            "mobile_app": 0.30,
            "store": 0.20,
            "phone": 0.05,
        }
    )

    # Return reason mix
    return_reason_weights: dict[str, float] = field(
        default_factory=lambda: {
            "size_issue": 0.30,
            "changed_mind": 0.20,
            "wrong_item": 0.18,
            "defective": 0.15,
            "not_as_described": 0.10,
            "arrived_too_late": 0.07,
        }
    )

    # ─── Helpers ──────────────────────────────────────────────────────────────
    def scaled_count(self, full_count: int) -> int:
        """Apply scale_factor, with a minimum of 10 records."""
        return max(10, int(full_count * self.scale_factor))

    @property
    def counts(self) -> RecordCounts:
        """Scaled record counts for all domains."""
        return RecordCounts(
            suppliers=self.scaled_count(_FULL_COUNTS.suppliers),
            employees=self.scaled_count(_FULL_COUNTS.employees),
            stores=self.scaled_count(_FULL_COUNTS.stores),
            products=self.scaled_count(_FULL_COUNTS.products),
            customers=self.scaled_count(_FULL_COUNTS.customers),
            campaigns=self.scaled_count(_FULL_COUNTS.campaigns),
            orders=self.scaled_count(_FULL_COUNTS.orders),
            order_lines=self.scaled_count(_FULL_COUNTS.order_lines),
            payments=self.scaled_count(_FULL_COUNTS.payments),
            shipments=self.scaled_count(_FULL_COUNTS.shipments),
            returns=self.scaled_count(_FULL_COUNTS.returns),
            inventory=self.scaled_count(_FULL_COUNTS.inventory),
            reviews=self.scaled_count(_FULL_COUNTS.reviews),
            clickstream=self.scaled_count(_FULL_COUNTS.clickstream),
            iot_sensors=self.scaled_count(_FULL_COUNTS.iot_sensors),
            finance=self.scaled_count(_FULL_COUNTS.finance),
        )

    @property
    def total_days(self) -> int:
        return (self.end_date - self.start_date).days

    @property
    def run_timestamp(self) -> str:
        return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


# Singleton default config (override via env vars)
DEFAULT_CONFIG = GeneratorConfig()
