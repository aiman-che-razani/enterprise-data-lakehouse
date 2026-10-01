"""Customer data generator — 500,000 records (full scale)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from datasets.generators.base_generator import BaseGenerator

_LOYALTY_TIERS = {
    "diamond": 0.02,
    "platinum": 0.08,
    "gold": 0.20,
    "silver": 0.35,
    "bronze": 0.35,
}

_CHANNEL_PREFS = {"email": 0.45, "sms": 0.25, "push": 0.20, "none": 0.10}

_ACQUISITION_CHANNELS = {
    "organic_search": 0.28,
    "paid_search": 0.18,
    "social_media": 0.15,
    "email_campaign": 0.12,
    "referral": 0.10,
    "direct": 0.10,
    "affiliate": 0.05,
    "in_store": 0.02,
}

_US_STATES = [
    "CA",
    "TX",
    "FL",
    "NY",
    "PA",
    "IL",
    "OH",
    "GA",
    "NC",
    "MI",
    "NJ",
    "VA",
    "WA",
    "AZ",
    "MA",
    "TN",
    "IN",
    "MD",
    "CO",
    "MN",
    "SC",
    "AL",
    "LA",
    "KY",
    "OR",
    "OK",
    "CT",
    "UT",
    "NV",
    "IA",
]

_INTL_COUNTRIES = [
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


class CustomersGenerator(BaseGenerator):
    domain = "customers"
    output_format = "json"

    def record_count(self) -> int:
        return self.config.counts.customers

    def _generate_record(self, index: int) -> dict[str, Any]:
        customer_id = f"CUST-{index + 1:08d}"
        segment = self._weighted_choice(self.config.segment_weights)
        loyalty_tier = self._weighted_choice(_LOYALTY_TIERS)

        is_us = bool(self.rng.random() < self.config.us_customer_pct)
        country = "United States" if is_us else self._choose(_INTL_COUNTRIES)

        # Loyalty points: higher tier = more points
        base_points = {
            "diamond": 50_000,
            "platinum": 20_000,
            "gold": 8_000,
            "silver": 2_000,
            "bronze": 500,
        }[loyalty_tier]
        loyalty_points = int(base_points * self.rng.uniform(0.5, 2.5))

        created_at = self._random_datetime(end=self.config.end_date)
        last_order = created_at + timedelta(days=int(self.rng.integers(0, 180)))
        if last_order.year > self.config.end_date.year:
            last_order = created_at

        gender = self._weighted_choice({"M": 0.46, "F": 0.50, "Non-binary": 0.03, "Unknown": 0.01})
        if gender == "M":
            first_name = self.fake.first_name_male()
        elif gender == "F":
            first_name = self.fake.first_name_female()
        else:
            first_name = self.fake.first_name()

        record: dict[str, Any] = {
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": self.fake.last_name(),
            "email": self.fake.email(),
            "phone": self.fake.phone_number() if self.rng.random() > 0.1 else None,
            "date_of_birth": self._iso_date(
                self.fake.date_of_birth(minimum_age=18, maximum_age=80)
            ),
            "gender": gender,
            "segment": segment,
            "loyalty_tier": loyalty_tier,
            "loyalty_points": loyalty_points,
            "country": country,
            "state": self._choose(_US_STATES) if is_us else None,
            "city": self.fake.city(),
            "postal_code": self.fake.postcode(),
            "address_line1": self.fake.street_address(),
            "channel_preference": self._weighted_choice(_CHANNEL_PREFS),
            "acquisition_channel": self._weighted_choice(_ACQUISITION_CHANNELS),
            "is_email_verified": bool(self.rng.random() > 0.05),
            "is_sms_opted_in": bool(self.rng.random() > 0.60),
            "is_active": bool(self.rng.random() > 0.07),
            "last_order_at": self._iso_dt(last_order) if self.rng.random() > 0.1 else None,
            "lifetime_order_count": int(self.rng.integers(1, 80)),
            "lifetime_spend": round(float(self.rng.uniform(10.0, 15_000.0)), 2),
            "created_at": self._iso_dt(created_at),
            "updated_at": self._iso_dt(
                self._random_datetime(start=created_at.date(), end=self.config.end_date)
            ),
            "source_system": "crm",
        }

        self.registry.customer_ids.append(customer_id)
        self.registry.customer_segments[customer_id] = segment
        self.registry.customer_countries[customer_id] = country

        return record
