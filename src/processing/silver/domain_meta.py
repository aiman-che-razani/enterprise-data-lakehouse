"""
Silver layer domain metadata — complements Bronze domain_meta.

Defines which domains need SCD Type 2, PII tokenisation, and what
columns are "slowly changing" for hash-based change detection.
"""

from __future__ import annotations

# Domains that require SCD Type 2 treatment in Silver
SILVER_SCD2_DOMAINS: set[str] = {
    "customers",
    "products",
    "suppliers",
    "stores",
    "employees",
}

# SCD columns per domain (changes to these columns trigger a new SCD version)
SILVER_SCD_COLS: dict[str, list[str]] = {
    "customers": [
        "segment",
        "loyalty_tier",
        "loyalty_points",
        "country",
        "state",
        "channel_preference",
        "is_email_verified",
        "is_sms_opted_in",
        "is_active",
    ],
    "products": [
        "name",
        "category_l1",
        "category_l2",
        "list_price",
        "cost_price",
        "is_active",
        "is_discontinued",
        "supplier_id",
    ],
    "suppliers": [
        "tier",
        "on_time_rate",
        "quality_score",
        "payment_terms",
        "is_active",
    ],
    "stores": [
        "store_type",
        "region",
        "sq_footage",
        "num_employees",
        "is_active",
    ],
    "employees": [
        "department",
        "job_title",
        "level",
        "manager_id",
        "store_id",
        "base_salary",
        "bonus_pct",
        "is_active",
    ],
}

# PII columns per domain and their handling strategy
SILVER_PII_HANDLING: dict[str, dict[str, str]] = {
    "customers": {
        "email": "hash",  # → email_hash (SHA-256)
        "first_name": "hash",  # → first_name_hash
        "last_name": "hash",  # → last_name_hash
        "phone": "drop",
        "address_line1": "drop",
        "date_of_birth": "age_bucket",  # → age_bucket ("25-34")
    },
    "employees": {
        "email": "hash",
        "first_name": "hash",  # → first_name_hash
        "last_name": "hash",  # → last_name_hash
    },
    "suppliers": {
        "contact_email": "hash",
        "contact_phone": "drop",
    },
    "stores": {
        "address": "drop",
    },
    # PCI: only card_last4 may be retained; expiry and network never reach Silver.
    "payments": {
        "card_exp": "drop",
        "card_network": "drop",
    },
    "clickstream": {
        "ip_address": "hash",  # keeps per-visitor grouping without the raw address
    },
}

# Domains that contain PII/PCI fields requiring masking / tokenisation (derived, so a new
# rule above can never be forgotten here).
SILVER_PII_DOMAINS: set[str] = set(SILVER_PII_HANDLING)

# Timestamp columns per domain that must be cast from string to TimestampType in Silver
DOMAIN_TIMESTAMP_COLS: dict[str, list[str]] = {
    "customers": ["last_order_at", "created_at", "updated_at"],
    "orders": ["created_at", "updated_at"],
    "order_lines": ["created_at"],
    "payments": ["created_at", "updated_at"],
    "shipments": [
        "ship_date",
        "estimated_delivery_date",
        "actual_delivery_date",
        "created_at",
        "updated_at",
    ],
    "returns": ["initiated_at", "received_at", "processed_at", "refunded_at", "created_at"],
    "inventory": ["created_at", "updated_at"],
    "reviews": ["created_at", "updated_at"],
    "clickstream": ["created_at"],
    "iot_sensors": ["reading_at", "ingested_at"],
    "finance": ["posted_at", "created_at"],
    "suppliers": ["created_at", "updated_at"],
    "stores": ["created_at", "updated_at"],
    "employees": ["created_at", "updated_at"],
    "campaigns": ["start_date", "end_date", "created_at", "updated_at"],
    "products": ["created_at", "updated_at"],
}

# Date columns per domain to cast to DateType
DOMAIN_DATE_COLS: dict[str, list[str]] = {
    "customers": ["date_of_birth"],
    "orders": ["order_date"],
    "inventory": ["snapshot_date", "last_received_date", "last_count_date"],
    "employees": ["hire_date", "termination_date"],
    "suppliers": ["onboarded_date"],
    "stores": ["opened_date", "closed_date"],
    "finance": ["transaction_date"],
    "returns": [],
    "reviews": [],
}

# Order of Silver processing (same as Bronze dependency order)
SILVER_PROCESSING_ORDER: list[str] = [
    "suppliers",
    "stores",
    "employees",
    "products",
    "customers",
    "campaigns",
    "orders",
    "order_lines",
    "payments",
    "shipments",
    "returns",
    "inventory",
    "reviews",
    "clickstream",
    "iot_sensors",
    "finance",
]
