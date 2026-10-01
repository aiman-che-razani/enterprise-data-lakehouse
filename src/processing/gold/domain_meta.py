"""
Gold layer domain metadata.

Defines the mapping between Silver sources and Gold star-schema tables,
the partition columns, ZORDER columns, and which tables are full-refresh
vs. incremental-MERGE on each daily run.
"""

from __future__ import annotations

# ─── Dimension tables ─────────────────────────────────────────────────────────

# Silver source path key → Gold dimension table name
DIM_SOURCE_MAP: dict[str, str] = {
    "customers": "dim_customer",
    "products": "dim_product",
    "stores": "dim_store",
    "employees": "dim_employee",
    "suppliers": "dim_supplier",
    "campaigns": "dim_promotion",
    # dim_date is generated programmatically (no Silver source)
    # dim_location is derived from stores + customers Silver tables
}

# Dimension natural key columns (the business PK coming from Silver)
DIM_NATURAL_KEYS: dict[str, str] = {
    "dim_customer": "nk_customer_id",
    "dim_product": "nk_sku",
    "dim_store": "nk_store_id",
    "dim_employee": "nk_employee_id",
    "dim_supplier": "nk_supplier_id",
    "dim_promotion": "nk_campaign_id",
    "dim_date": "sk_date_id",  # date dims use YYYYMMDD int as both SK and NK
    "dim_location": "nk_location_key",
}

# Surrogate key column name per dimension table
DIM_SK_COL: dict[str, str] = {
    "dim_customer": "sk_customer_id",
    "dim_product": "sk_product_id",
    "dim_store": "sk_store_id",
    "dim_employee": "sk_employee_id",
    "dim_supplier": "sk_supplier_id",
    "dim_promotion": "sk_promotion_id",
    "dim_date": "sk_date_id",
    "dim_location": "sk_location_id",
}

# Dim tables backed by SCD2 Silver sources — Gold reads WHERE is_current = true
SCD2_DIM_TABLES: set[str] = {
    "dim_customer",
    "dim_product",
    "dim_store",
    "dim_employee",
    "dim_supplier",
}

# Full-refresh dims (overwrite entire Gold table on each run)
# All dims are full-refresh — Silver is the SOR, Gold dims are current-state snapshots
FULL_REFRESH_DIMS: set[str] = {
    "dim_customer",
    "dim_product",
    "dim_store",
    "dim_employee",
    "dim_supplier",
    "dim_promotion",
    "dim_date",
    "dim_location",
}

# ─── Fact tables ──────────────────────────────────────────────────────────────

# Fact table primary key for MERGE (natural key from Silver)
FACT_PRIMARY_KEYS: dict[str, str] = {
    "fact_sales": "nk_order_line_id",
    "fact_inventory": "nk_inventory_id",
    "fact_marketing": "nk_marketing_id",  # campaign_id + date composite
    "fact_returns": "nk_return_id",
    "fact_shipments": "nk_shipment_id",
    "fact_finance": "nk_transaction_id",
}

# Fact partition column: the business month (yyyymm, derived from sk_date_id), a Delta-only technical
# column the warehouse loaders drop. Partitioning by the processing day (_gold_date) put every run's rows
# in a new partition, so date-range queries could not prune and a MERGE had to scan the whole table.
PARTITION_COL = "_date_month"
FACT_PARTITION_COLS: dict[str, list[str]] = {
    "fact_sales": [PARTITION_COL],
    "fact_inventory": [PARTITION_COL],
    "fact_marketing": [PARTITION_COL],
    "fact_returns": [PARTITION_COL],
    "fact_shipments": [PARTITION_COL],
    "fact_finance": [PARTITION_COL],
}

# Facts whose business date never changes for a given key, so a MERGE can also match on the partition
# and skip every other month. A shipment's date moves from "created" to "shipped", so it is excluded.
FACT_STABLE_PARTITION: frozenset[str] = frozenset(
    {"fact_sales", "fact_inventory", "fact_marketing", "fact_returns", "fact_finance"}
)

# ZORDER columns for each fact table (used in OPTIMIZE)
FACT_ZORDER_COLS: dict[str, list[str]] = {
    "fact_sales": ["sk_customer_id", "sk_product_id", "sk_date_id"],
    "fact_inventory": ["sk_store_id", "sk_product_id"],
    "fact_marketing": ["sk_promotion_id", "sk_date_id"],
    "fact_returns": ["sk_customer_id", "sk_date_id"],
    "fact_shipments": ["sk_date_id", "nk_order_id"],
    "fact_finance": ["sk_date_id", "account_type"],
}

# ─── Build ordering ───────────────────────────────────────────────────────────

# Dimensions must be built before facts (facts join to dim SKs)
GOLD_DIM_ORDER: list[str] = [
    "dim_date",  # no Silver dep — generated first
    "dim_location",  # derived from stores + customers
    "dim_supplier",
    "dim_store",
    "dim_employee",
    "dim_product",
    "dim_customer",
    "dim_promotion",
]

GOLD_FACT_ORDER: list[str] = [
    "fact_sales",
    "fact_inventory",
    "fact_marketing",
    "fact_returns",
    "fact_shipments",
    "fact_finance",
]

# Date range for dim_date generation
DIM_DATE_START = "2020-01-01"
DIM_DATE_END = "2030-12-31"
