"""
Centralised metadata for all 16 Bronze domains.

Each generator already knows its output format; this module is the single
source of truth the ingestion layer reads at runtime.
"""

from __future__ import annotations

# Primary key column per domain — used for Z-ORDER and duplicate detection
DOMAIN_PKS: dict[str, str] = {
    "suppliers": "supplier_id",
    "stores": "store_id",
    "employees": "employee_id",
    "products": "sku",
    "customers": "customer_id",
    "campaigns": "campaign_id",
    "orders": "order_id",
    "order_lines": "order_line_id",
    "payments": "payment_id",
    "shipments": "shipment_id",
    "returns": "return_id",
    "inventory": "inventory_id",
    "reviews": "review_id",
    "clickstream": "event_id",
    "iot_sensors": "reading_id",
    "finance": "transaction_id",
}

# Source file format produced by each generator
DOMAIN_FORMATS: dict[str, str] = {
    "suppliers": "csv",
    "stores": "csv",
    "employees": "csv",
    "products": "csv",
    "campaigns": "csv",
    "returns": "csv",
    "customers": "json",
    "orders": "json",
    "order_lines": "json",
    "payments": "json",
    "shipments": "json",
    "inventory": "json",
    "reviews": "json",
    "clickstream": "json",  # gzip-compressed JSONL
    "iot_sensors": "json",
    "finance": "parquet",
}

# Z-ORDER column list per domain (first col = primary, rest = common join/filter keys)
DOMAIN_ZORDER: dict[str, list[str]] = {
    "suppliers": ["supplier_id"],
    "stores": ["store_id", "region"],
    "employees": ["employee_id", "department"],
    "products": ["sku", "category_l1"],
    "customers": ["customer_id", "segment"],
    "campaigns": ["campaign_id", "campaign_type"],
    "orders": ["order_id", "customer_id", "order_date"],
    "order_lines": ["order_line_id", "order_id", "sku"],
    "payments": ["payment_id", "order_id", "status"],
    "shipments": ["shipment_id", "order_id", "carrier"],
    "returns": ["return_id", "order_id", "status"],
    "inventory": ["inventory_id", "sku", "store_id"],
    "reviews": ["review_id", "sku", "customer_id"],
    "clickstream": ["event_id", "session_id", "event_type"],
    "iot_sensors": ["reading_id", "sensor_id", "sensor_type"],
    "finance": ["transaction_id", "transaction_date", "debit_account_code"],
}

# Kafka topic per streaming domain (batch-only domains omitted)
DOMAIN_KAFKA_TOPICS: dict[str, str] = {
    "orders": "retailnexus.orders",
    "payments": "retailnexus.payments",
    "clickstream": "retailnexus.clickstream",
    "inventory": "retailnexus.inventory",
    "iot_sensors": "retailnexus.iot-sensors",
}

# Event-time column used for stream watermarking
DOMAIN_EVENT_TIME: dict[str, str] = {
    "orders": "created_at",
    "payments": "created_at",
    "clickstream": "created_at",
    "inventory": "updated_at",
    "iot_sensors": "reading_at",
}

# Domains ingested in dependency order (upstream → downstream)
BATCH_INGESTION_ORDER: list[str] = [
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
