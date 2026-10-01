"""
Spark StructType schema definitions for the Bronze layer.

Each domain has an explicit schema for:
  1. Enforcing column types at ingest time (avoids schema inference)
  2. Enabling Spark schema evolution tracking
  3. Serving as the contract between generators and Bronze pipelines

All timestamps are StringType at Bronze — the Silver layer casts to TimestampType
after PII tokenisation and null handling.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _f(name: str, dtype: DataType = StringType(), nullable: bool = True) -> StructField:
    return StructField(name, dtype, nullable)


def _str(name: str, nullable: bool = True) -> StructField:
    return _f(name, StringType(), nullable)


def _int(name: str, nullable: bool = True) -> StructField:
    return _f(name, IntegerType(), nullable)


def _long(name: str, nullable: bool = True) -> StructField:
    return _f(name, LongType(), nullable)


def _dbl(name: str, nullable: bool = True) -> StructField:
    return _f(name, DoubleType(), nullable)


def _bool(name: str, nullable: bool = True) -> StructField:
    return _f(name, BooleanType(), nullable)


# ─── Domain schemas ───────────────────────────────────────────────────────────

SUPPLIERS_SCHEMA = StructType(
    [
        _str("supplier_id", nullable=False),
        _str("name"),
        _str("tier"),
        _str("country"),
        _str("city"),
        _str("contact_email"),
        _str("contact_phone"),
        _int("lead_time_days"),
        _dbl("on_time_rate"),
        _dbl("quality_score"),
        _str("payment_terms"),
        _str("certifications"),  # JSON array string at Bronze
        _bool("is_active"),
        _str("onboarded_date"),
        _str("created_at"),
        _str("updated_at"),
    ]
)

STORES_SCHEMA = StructType(
    [
        _str("store_id", nullable=False),
        _str("name"),
        _str("store_type"),
        _str("address"),
        _str("city"),
        _str("state"),
        _str("country"),
        _str("postal_code"),
        _str("region"),
        _int("sq_footage"),
        _int("num_employees"),
        _bool("is_active"),
        _str("opened_date"),
        _str("closed_date"),
        _str("created_at"),
        _str("updated_at"),
    ]
)

EMPLOYEES_SCHEMA = StructType(
    [
        _str("employee_id", nullable=False),
        _str("first_name"),
        _str("last_name"),
        _str("email"),
        _str("department"),
        _str("job_title"),
        _str("level"),
        _str("manager_id"),
        _str("store_id"),
        _dbl("base_salary"),
        _dbl("bonus_pct"),
        _str("hire_date"),
        _str("termination_date"),
        _bool("is_active"),
        _str("supplier_id"),
        _str("created_at"),
        _str("updated_at"),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        _str("sku", nullable=False),
        _str("name"),
        _str("description"),
        _str("category_l1"),
        _str("category_l2"),
        _str("brand"),
        _str("supplier_id"),
        _dbl("list_price"),
        _dbl("cost_price"),
        _dbl("margin_pct"),
        _dbl("weight_kg"),
        _bool("is_active"),
        _bool("is_discontinued"),
        _int("reorder_threshold"),
        _str("created_at"),
        _str("updated_at"),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        _str("customer_id", nullable=False),
        _str("first_name"),
        _str("last_name"),
        _str("email"),  # PII — tokenised in Silver
        _str("phone"),  # PII — dropped in Silver
        _str("date_of_birth"),  # PII — age-bucketed in Silver
        _str("gender"),
        _str("segment"),
        _str("loyalty_tier"),
        _long("loyalty_points"),
        _str("country"),
        _str("state"),
        _str("city"),
        _str("postal_code"),
        _str("address_line1"),  # PII — dropped in Silver
        _str("channel_preference"),
        _str("acquisition_channel"),
        _bool("is_email_verified"),
        _bool("is_sms_opted_in"),
        _bool("is_active"),
        _str("last_order_at"),
        _long("lifetime_order_count"),
        _dbl("lifetime_spend"),
        _str("created_at"),
        _str("updated_at"),
        _str("source_system"),
    ]
)

CAMPAIGNS_SCHEMA = StructType(
    [
        _str("campaign_id", nullable=False),
        _str("campaign_name"),
        _str("campaign_type"),
        _str("objective"),
        _str("platform"),
        _str("target_category"),
        _str("target_segment"),
        _dbl("budget"),
        _dbl("actual_spend"),
        _long("impressions"),
        _long("clicks"),
        _dbl("ctr"),
        _long("conversions"),
        _dbl("conversion_rate"),
        _dbl("total_revenue"),
        _dbl("roas"),
        _dbl("cpc"),
        _dbl("cpa"),
        _bool("is_active"),
        _str("start_date"),
        _str("end_date"),
        _str("created_at"),
        _str("updated_at"),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        _str("order_id", nullable=False),
        _str("customer_id"),
        _str("order_date"),
        _str("channel"),
        _str("store_id"),
        _str("status"),
        _str("payment_status"),
        _dbl("subtotal"),
        _dbl("discount_amount"),
        _dbl("tax_amount"),
        _dbl("shipping_amount"),
        _dbl("total_amount"),
        _str("currency"),
        _str("promo_code"),
        _bool("is_gift"),
        _str("created_at"),
        _str("updated_at"),
        _str("source_system"),
    ]
)

ORDER_LINES_SCHEMA = StructType(
    [
        _str("order_line_id", nullable=False),
        _str("order_id"),
        _str("sku"),
        _str("product_name"),
        _str("category"),
        _int("quantity"),
        _dbl("unit_list_price"),
        _dbl("unit_price"),
        _dbl("discount_pct"),
        _dbl("line_total"),
        _bool("is_gift_wrap"),
        _str("created_at"),
    ]
)

PAYMENTS_SCHEMA = StructType(
    [
        _str("payment_id", nullable=False),
        _str("order_id"),
        _str("customer_id"),
        _str("payment_method"),
        _str("payment_processor"),
        _dbl("amount"),
        _str("currency"),
        _str("status"),
        _str("card_last4"),
        _str("card_network"),
        _str("card_exp"),
        _dbl("refund_amount"),
        _str("refund_reason"),
        _str("failure_code"),
        _str("processor_transaction_id"),
        _str("created_at"),
        _str("updated_at"),
    ]
)

SHIPMENTS_SCHEMA = StructType(
    [
        _str("shipment_id", nullable=False),
        _str("order_id"),
        _str("customer_id"),
        _str("warehouse_id"),
        _str("carrier"),
        _str("service"),
        _str("tracking_number"),
        _str("status"),
        _dbl("weight_kg"),
        _dbl("length_cm"),
        _dbl("width_cm"),
        _dbl("height_cm"),
        _dbl("shipping_cost"),
        _bool("is_signature_required"),
        _str("ship_date"),
        _str("estimated_delivery_date"),
        _str("actual_delivery_date"),
        _str("created_at"),
        _str("updated_at"),
    ]
)

RETURNS_SCHEMA = StructType(
    [
        _str("return_id", nullable=False),
        _str("order_id"),
        _str("order_line_id"),
        _str("customer_id"),
        _str("sku"),
        _str("product_name"),
        _int("quantity"),
        _dbl("unit_price"),
        _dbl("refund_amount"),
        _str("reason"),
        _str("status"),
        _str("condition"),
        _dbl("restocking_fee"),
        _bool("is_restockable"),
        _str("initiated_at"),
        _str("received_at"),
        _str("processed_at"),
        _str("refunded_at"),
        _str("created_at"),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        _str("inventory_id", nullable=False),
        _str("sku"),
        _str("store_id"),
        _str("snapshot_date"),
        _str("abc_class"),
        _int("quantity_on_hand"),
        _int("quantity_reserved"),
        _int("quantity_available"),
        _int("quantity_on_order"),
        _int("reorder_point"),
        _int("reorder_quantity"),
        _dbl("days_of_supply"),
        _dbl("cost_per_unit"),
        _dbl("total_inventory_value"),
        _str("bin_location"),
        _bool("is_below_reorder_point"),
        _bool("is_out_of_stock"),
        _str("last_received_date"),
        _str("last_count_date"),
        _str("created_at"),
        _str("updated_at"),
    ]
)

REVIEWS_SCHEMA = StructType(
    [
        _str("review_id", nullable=False),
        _str("customer_id"),
        _str("sku"),
        _str("product_name"),
        _str("order_id"),
        _int("rating"),
        _str("title"),
        _str("body"),
        _str("sentiment"),
        _bool("is_verified_purchase"),
        _int("helpful_votes"),
        _int("not_helpful_votes"),
        _bool("is_visible"),
        _str("response_from_seller"),
        _int("images_count"),
        _str("created_at"),
        _str("updated_at"),
    ]
)

CLICKSTREAM_SCHEMA = StructType(
    [
        _str("event_id", nullable=False),
        _str("session_id"),
        _str("customer_id"),
        _str("anonymous_id"),
        _str("event_type"),
        _str("page_url"),
        _str("referrer_url"),
        _str("sku"),
        _str("campaign_id"),
        _str("utm_source"),
        _str("utm_medium"),
        _str("device_type"),
        _str("browser"),
        _str("os"),
        _str("ip_address"),
        _long("session_duration_ms"),
        _bool("is_bounce"),
        _str("search_query"),
        _str("created_at"),
    ]
)

IOT_SENSORS_SCHEMA = StructType(
    [
        _str("reading_id", nullable=False),
        _str("sensor_id"),
        _str("store_id"),
        _str("sensor_type"),
        _dbl("value"),
        _str("unit"),
        _bool("alert_triggered"),
        _dbl("alert_threshold_low"),
        _dbl("alert_threshold_high"),
        _bool("is_anomaly"),
        _dbl("battery_pct"),
        _int("signal_rssi_dbm"),
        _str("reading_quality"),
        _str("firmware_version"),
        _str("reading_at"),
        _str("ingested_at"),
    ]
)

FINANCE_SCHEMA = StructType(
    [
        _str("transaction_id", nullable=False),
        _str("journal_entry_id"),
        _str("transaction_type"),
        _str("debit_account_code"),
        _str("debit_account_name"),
        _str("credit_account_code"),
        _str("credit_account_name"),
        _dbl("debit_amount"),
        _dbl("credit_amount"),
        _str("description"),
        _str("reference_id"),
        _str("cost_center"),
        _int("fiscal_year"),
        _int("fiscal_period"),
        _int("fiscal_quarter"),
        _str("transaction_date"),
        _str("posted_at"),
        _str("source_system"),
        _bool("is_reconciled"),
        _bool("is_reversed"),
        _str("created_at"),
    ]
)


# ─── Schema registry ──────────────────────────────────────────────────────────

BRONZE_SCHEMAS: dict[str, StructType] = {
    "suppliers": SUPPLIERS_SCHEMA,
    "stores": STORES_SCHEMA,
    "employees": EMPLOYEES_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "customers": CUSTOMERS_SCHEMA,
    "campaigns": CAMPAIGNS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_lines": ORDER_LINES_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
    "shipments": SHIPMENTS_SCHEMA,
    "returns": RETURNS_SCHEMA,
    "inventory": INVENTORY_SCHEMA,
    "reviews": REVIEWS_SCHEMA,
    "clickstream": CLICKSTREAM_SCHEMA,
    "iot_sensors": IOT_SENSORS_SCHEMA,
    "finance": FINANCE_SCHEMA,
}


def get_bronze_schema(domain: str) -> StructType:
    """Return the Bronze StructType for a given domain name."""
    try:
        return BRONZE_SCHEMAS[domain]
    except KeyError:
        raise ValueError(
            f"No Bronze schema registered for domain '{domain}'. "
            f"Available: {sorted(BRONZE_SCHEMAS)}"
        )
