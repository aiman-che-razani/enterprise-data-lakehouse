"""Supplier data generator — 2,000 records (full scale)."""

from __future__ import annotations

from typing import Any

from datasets.generators.base_generator import BaseGenerator

_TIER_WEIGHTS = {"A": 0.20, "B": 0.45, "C": 0.35}

_PAYMENT_TERMS = ["Net 30", "Net 60", "Net 90", "2/10 Net 30", "COD", "Net 15"]

_CATEGORIES = [
    "Electronics",
    "Clothing & Apparel",
    "Home & Garden",
    "Sports & Outdoors",
    "Books & Media",
    "Beauty & Personal Care",
    "Food & Beverage",
    "Toys & Games",
    "Automotive",
    "Office Supplies",
]

_COUNTRIES_BY_WEIGHT = {
    "United States": 0.30,
    "China": 0.25,
    "India": 0.10,
    "Germany": 0.07,
    "Vietnam": 0.06,
    "Bangladesh": 0.05,
    "South Korea": 0.04,
    "Taiwan": 0.04,
    "Mexico": 0.04,
    "Japan": 0.03,
    "Italy": 0.02,
}

_CERTIFICATIONS = [
    "ISO 9001",
    "ISO 14001",
    "SA8000",
    "OEKO-TEX",
    "Fair Trade",
    "BSCI",
    "SMETA",
    "RoHS",
    "CE",
    "UL",
]


_BRONZE_COLUMNS = (
    "supplier_id",
    "name",
    "tier",
    "country",
    "city",
    "contact_email",
    "contact_phone",
    "lead_time_days",
    "on_time_rate",
    "quality_score",
    "payment_terms",
    "certifications",
    "is_active",
    "onboarded_date",
    "created_at",
    "updated_at",
)


class SuppliersGenerator(BaseGenerator):
    domain = "suppliers"
    output_format = "csv"

    def record_count(self) -> int:
        return self.config.counts.suppliers

    def _generate_record(self, index: int) -> dict[str, Any]:
        supplier_id = f"SUP-{index + 1:06d}"
        tier = self._weighted_choice(_TIER_WEIGHTS)
        country = self._weighted_choice(_COUNTRIES_BY_WEIGHT)
        lead_time = {
            "A": int(self.rng.integers(3, 15)),
            "B": int(self.rng.integers(14, 45)),
            "C": int(self.rng.integers(30, 90)),
        }[tier]

        # Pick 0-3 certifications
        n_certs = int(self.rng.integers(0, 4))
        certifications = sorted(self._choose_n(_CERTIFICATIONS, n_certs))

        # On-time delivery rate correlates with tier
        base_rate = {"A": 0.97, "B": 0.88, "C": 0.75}[tier]
        on_time_rate = round(float(self.rng.normal(base_rate, 0.03)), 3)
        on_time_rate = max(0.50, min(1.0, on_time_rate))

        created_at = self._random_datetime(end=self.config.start_date)

        record: dict[str, Any] = {
            "supplier_id": supplier_id,
            "supplier_name": f"{self.fake.company()} {self._choose(['Ltd', 'Inc', 'Co.', 'Group', 'Corp'])}",
            "tier": tier,
            "country": country,
            "city": self.fake.city(),
            "contact_name": self.fake.name(),
            "contact_email": self.fake.company_email(),
            "contact_phone": self.fake.phone_number(),
            "primary_category": self._choose(_CATEGORIES),
            "payment_terms": self._choose(_PAYMENT_TERMS),
            "lead_time_days": lead_time,
            "min_order_quantity": int(self.rng.choice([10, 25, 50, 100, 250, 500])),
            "on_time_delivery_rate": on_time_rate,
            "quality_score": round(
                float(
                    self.rng.uniform(
                        {"A": 4.0, "B": 3.0, "C": 2.0}[tier],
                        {"A": 5.0, "B": 4.5, "C": 4.0}[tier],
                    )
                ),
                2,
            ),
            "certifications": "|".join(certifications) if certifications else "",
            "is_active": bool(self.rng.random() > 0.05),  # 5% inactive
            "created_at": self._iso_dt(created_at),
            "updated_at": self._iso_dt(
                self._random_datetime(start=created_at.date(), end=self.config.end_date)
            ),
        }

        # Register
        self.registry.supplier_ids.append(supplier_id)
        self.registry.supplier_names[supplier_id] = record["supplier_name"]
        self.registry.supplier_tiers[supplier_id] = tier

        # Emit exactly the Bronze contract (spark/schemas/bronze_schemas.py): names and column order.
        record["name"] = record.pop("supplier_name")
        record["on_time_rate"] = record.pop("on_time_delivery_rate")
        record["onboarded_date"] = record["created_at"][:10]
        return {k: record[k] for k in _BRONZE_COLUMNS}
