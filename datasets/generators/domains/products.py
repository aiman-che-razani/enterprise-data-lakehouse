"""Product catalog generator — 50,000 SKUs (full scale)."""

from __future__ import annotations

from typing import Any

from datasets.generators.base_generator import BaseGenerator

# L1 category → (L2 subcategories, typical price range USD, weight range kg)
_CATALOG: dict[str, tuple[list[str], tuple[float, float], tuple[float, float]]] = {
    "Electronics": (
        [
            "Smartphones",
            "Laptops",
            "Tablets",
            "Audio",
            "Cameras",
            "Accessories",
            "Gaming",
            "Wearables",
        ],
        (9.99, 2_499.99),
        (0.05, 3.5),
    ),
    "Clothing & Apparel": (
        [
            "Men's Tops",
            "Women's Tops",
            "Bottoms",
            "Outerwear",
            "Footwear",
            "Activewear",
            "Accessories",
        ],
        (9.99, 349.99),
        (0.1, 1.5),
    ),
    "Home & Garden": (
        ["Furniture", "Bedding", "Kitchen", "Decor", "Garden Tools", "Lighting", "Storage"],
        (4.99, 1_999.99),
        (0.2, 25.0),
    ),
    "Sports & Outdoors": (
        ["Fitness", "Camping", "Cycling", "Team Sports", "Water Sports", "Winter Sports"],
        (9.99, 999.99),
        (0.1, 15.0),
    ),
    "Books & Media": (
        ["Fiction", "Non-Fiction", "Children's", "Academic", "Audiobooks", "Music", "Movies"],
        (4.99, 99.99),
        (0.1, 1.2),
    ),
    "Beauty & Personal Care": (
        ["Skincare", "Haircare", "Makeup", "Fragrance", "Men's Grooming", "Oral Care"],
        (3.99, 299.99),
        (0.05, 1.0),
    ),
    "Food & Beverage": (
        ["Snacks", "Beverages", "Coffee & Tea", "Health Foods", "International", "Condiments"],
        (1.99, 79.99),
        (0.1, 5.0),
    ),
    "Toys & Games": (
        ["Action Figures", "Board Games", "Educational", "Outdoor Play", "Building Sets", "Dolls"],
        (4.99, 299.99),
        (0.1, 5.0),
    ),
}

_L1_WEIGHTS = {
    "Electronics": 0.18,
    "Clothing & Apparel": 0.22,
    "Home & Garden": 0.15,
    "Sports & Outdoors": 0.10,
    "Books & Media": 0.08,
    "Beauty & Personal Care": 0.12,
    "Food & Beverage": 0.10,
    "Toys & Games": 0.05,
}

_BRANDS: dict[str, list[str]] = {
    "Electronics": ["TechPrime", "NovaByte", "QuantumEdge", "SwiftTech", "DigiCore"],
    "Clothing & Apparel": ["UrbanThread", "StyleForge", "ComfortWear", "TrendLine", "PeakFit"],
    "Home & Garden": ["HomeHaven", "CraftSpace", "GreenLeaf", "CozyNest", "UrbanBloom"],
    "Sports & Outdoors": ["PeakPerform", "TrailBlaze", "AquaGear", "IronCore", "SwiftRun"],
    "Books & Media": ["PageTurn", "MindVault", "ClearSound", "VisualEdge", "NovaMind"],
    "Beauty & Personal Care": ["GlowLab", "PureEssence", "LuxSkin", "NaturaCare", "BeautyVault"],
    "Food & Beverage": ["NatureBite", "FreshFarm", "GoldBlend", "HarvestPick", "TasteWell"],
    "Toys & Games": ["PlayForge", "FunZone", "BrightKids", "GameMaster", "TinyWorld"],
}


_BRONZE_COLUMNS = (
    "sku",
    "name",
    "description",
    "category_l1",
    "category_l2",
    "brand",
    "supplier_id",
    "list_price",
    "cost_price",
    "margin_pct",
    "weight_kg",
    "is_active",
    "is_discontinued",
    "reorder_threshold",
    "created_at",
    "updated_at",
)


class ProductsGenerator(BaseGenerator):
    domain = "products"
    output_format = "csv"

    def record_count(self) -> int:
        return self.config.counts.products

    def _generate_record(self, index: int) -> dict[str, Any]:
        sku = f"SKU-{index + 1:07d}"
        cat_l1 = self._weighted_choice(_L1_WEIGHTS)
        cat_l2_list, price_range, weight_range = _CATALOG[cat_l1]
        cat_l2 = self._choose(cat_l2_list)
        brand = self._choose(_BRANDS[cat_l1])

        # Price: Pareto-like — most items cheap, some expensive
        price_lo, price_hi = price_range
        # Use log-uniform distribution for more realistic spread
        log_lo = float(np.log(price_lo + 1))
        log_hi = float(np.log(price_hi + 1))
        import numpy as np_local

        log_price = float(self.rng.uniform(log_lo, log_hi))
        list_price = round(float(np_local.exp(log_price) - 1), 2)

        # Cost is 30-65% of list price depending on margin pressure
        margin = float(self.rng.uniform(0.35, 0.70))
        cost_price = round(list_price * (1 - margin), 2)

        wt_lo, wt_hi = weight_range
        weight_kg = round(float(self.rng.uniform(wt_lo, wt_hi)), 3)

        supplier_id = (
            self._choose(self.registry.supplier_ids) if self.registry.supplier_ids else None
        )
        is_active = bool(self.rng.random() > 0.05)

        created_at = self._random_datetime(end=self.config.start_date)
        product_name = f"{brand} {cat_l2} {self.fake.color_name().title()} #{index + 1}"

        record: dict[str, Any] = {
            "sku": sku,
            "product_name": product_name,
            "category_l1": cat_l1,
            "category_l2": cat_l2,
            "brand": brand,
            "supplier_id": supplier_id,
            "cost_price": cost_price,
            "list_price": list_price,
            "weight_kg": weight_kg,
            "unit_of_measure": self._choose(["each", "each", "each", "pack", "case"]),
            "is_perishable": cat_l1 == "Food & Beverage",
            "is_active": is_active,
            "is_hazmat": bool(cat_l1 in ("Electronics",) and self.rng.random() < 0.02),
            "country_of_origin": (
                self._choose(
                    self.registry.supplier_ids
                    and [self.registry.supplier_names.get(supplier_id, "China")]
                    or ["China"]
                )
                if False
                else self._choose(
                    ["China", "Vietnam", "United States", "India", "Bangladesh", "Germany"]
                )
            ),
            "reorder_point": int(self.rng.integers(5, 100)),
            "reorder_quantity": int(self.rng.integers(50, 500)),
            "created_at": self._iso_dt(created_at),
            "updated_at": self._iso_dt(
                self._random_datetime(start=created_at.date(), end=self.config.end_date)
            ),
        }

        self.registry.product_skus.append(sku)
        self.registry.product_names[sku] = product_name
        self.registry.product_prices[sku] = list_price
        self.registry.product_costs[sku] = cost_price
        self.registry.product_categories[sku] = cat_l1
        self.registry.product_supplier[sku] = supplier_id or ""
        if is_active:
            self.registry.active_skus.append(sku)

        # Emit exactly the Bronze contract (spark/schemas/bronze_schemas.py): names and column order.
        record["name"] = record.pop("product_name")
        record["description"] = f"{record['name']} by {record['brand']}"
        record["margin_pct"] = round(
            (record["list_price"] - record["cost_price"]) / record["list_price"] * 100, 2
        )
        record["is_discontinued"] = not record["is_active"]
        record["reorder_threshold"] = record.pop("reorder_point")
        return {k: record[k] for k in _BRONZE_COLUMNS}


import numpy as np  # noqa: E402 — needed for log-uniform price calc
