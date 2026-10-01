"""Store / location generator — 250 stores (full scale)."""

from __future__ import annotations

from typing import Any

from datasets.generators.base_generator import BaseGenerator

_STORE_TYPES = {
    "flagship": 0.08,
    "standard": 0.45,
    "express": 0.25,
    "outlet": 0.12,
    "warehouse_club": 0.10,
}

_REGIONS = {
    "Northeast": ["New York", "Boston", "Philadelphia", "Newark", "Hartford"],
    "Southeast": ["Atlanta", "Miami", "Charlotte", "Nashville", "Orlando"],
    "Midwest": ["Chicago", "Detroit", "Columbus", "Indianapolis", "Milwaukee"],
    "Southwest": ["Dallas", "Houston", "Phoenix", "San Antonio", "Austin"],
    "West": ["Los Angeles", "Seattle", "San Francisco", "Denver", "Portland"],
}

_SQ_FOOTAGE = {
    "flagship": (25_000, 80_000),
    "standard": (8_000, 25_000),
    "express": (1_500, 5_000),
    "outlet": (10_000, 30_000),
    "warehouse_club": (80_000, 150_000),
}


_BRONZE_COLUMNS = (
    "store_id",
    "name",
    "store_type",
    "address",
    "city",
    "state",
    "country",
    "postal_code",
    "region",
    "sq_footage",
    "num_employees",
    "is_active",
    "opened_date",
    "closed_date",
    "created_at",
    "updated_at",
)


class StoresGenerator(BaseGenerator):
    domain = "stores"
    output_format = "csv"

    def record_count(self) -> int:
        return self.config.counts.stores

    def _generate_record(self, index: int) -> dict[str, Any]:
        store_id = f"STR-{index + 1:05d}"
        store_type = self._weighted_choice(_STORE_TYPES)
        region = self._choose(list(_REGIONS.keys()))
        city = self._choose(_REGIONS[region])

        sq_lo, sq_hi = _SQ_FOOTAGE[store_type]
        sq_footage = int(self.rng.integers(sq_lo, sq_hi + 1))

        # Employee count scales with square footage
        employee_count = int(sq_footage / 500 * self.rng.uniform(0.8, 1.2))

        opened = self._random_datetime(end=self.config.start_date)

        record: dict[str, Any] = {
            "store_id": store_id,
            "store_name": f"RetailNexus {city} {store_type.replace('_', ' ').title()} #{index + 1}",
            "store_type": store_type,
            "region": region,
            "city": city,
            "state": self.fake.state(),
            "country": "United States",
            "postal_code": self.fake.postcode(),
            "address": self.fake.street_address(),
            "sq_footage": sq_footage,
            "employee_count": max(5, employee_count),
            "has_pharmacy": bool(
                store_type in ("standard", "flagship", "warehouse_club") and self.rng.random() > 0.4
            ),
            "has_grocery": bool(
                store_type in ("flagship", "warehouse_club") and self.rng.random() > 0.3
            ),
            "is_24h": bool(store_type == "warehouse_club" and self.rng.random() > 0.5),
            "is_active": bool(self.rng.random() > 0.03),
            "opened_at": self._iso_dt(opened),
            "created_at": self._iso_dt(opened),
            "updated_at": self._iso_dt(
                self._random_datetime(start=opened.date(), end=self.config.end_date)
            ),
        }

        self.registry.store_ids.append(store_id)
        self.registry.store_types[store_id] = store_type
        self.registry.store_cities[store_id] = city
        self.registry.store_regions[store_id] = region

        # Emit exactly the Bronze contract (spark/schemas/bronze_schemas.py): names and column order.
        record["name"] = record.pop("store_name")
        record["num_employees"] = record.pop("employee_count")
        record["opened_date"] = record["opened_at"][:10]
        record["closed_date"] = None
        return {k: record[k] for k in _BRONZE_COLUMNS}
