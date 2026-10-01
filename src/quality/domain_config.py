"""
Per-domain DQ check configurations for Bronze, Silver, and Gold layers.

Each DomainCheckConfig captures:
  - pk_col:           column that must be unique (UniquenessCheck)
  - required_columns: must have zero nulls (CompletenessCheck threshold=0.0)
  - optional_columns: allowed ≤5 % nulls (CompletenessCheck threshold=0.05; Gold 0.10)
  - max_null_rates:   {col: max null share} for columns that are legitimately sparse (an undelivered
                      shipment has no transit days, an online order has no store). Overrides the
                      required / optional rule for that column, so the check stays green when the data
                      is as expected and fails when the null share genuinely regresses.
  - timestamp_col:    column used for FreshnessCheck: when the pipeline last wrote the layer
                      (_ingested_at / _silver_processed_at / _gold_processed_at), not an event date
  - sla_hours:        max acceptable data age in hours
  - numeric_ranges:   {col: (lo, hi)} for ValueRangeCheck (None = no bound)
  - fk_checks:        [(fk_col, parent_domain, parent_pk)]
                      — resolved to Delta paths by the engine at runtime
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DomainCheckConfig:
    pk_col: str | None = None
    required_columns: list[str] = field(default_factory=list)
    optional_columns: list[str] = field(default_factory=list)
    timestamp_col: str | None = None
    sla_hours: float = 26.0
    numeric_ranges: dict[str, tuple[float | None, float | None]] = field(default_factory=dict)
    fk_checks: list[tuple[str, str, str]] = field(default_factory=list)
    # fk_checks element: (fk_column, parent_domain, parent_pk_column)
    max_null_rates: dict[str, float] = field(default_factory=dict)

    def completeness_plan(self, optional_threshold: float) -> list[tuple[list[str], float]]:
        """(columns, max null share) groups for the completeness checks, one group per threshold.

        Required columns allow no nulls, optional ones ``optional_threshold``; a column in
        ``max_null_rates`` gets that rate instead and is removed from the other groups.
        """
        plan: list[tuple[list[str], float]] = []
        required = [c for c in self.required_columns if c not in self.max_null_rates]
        optional = [c for c in self.optional_columns if c not in self.max_null_rates]
        if required:
            plan.append((required, 0.0))
        if optional:
            plan.append((optional, optional_threshold))
        by_rate: dict[float, list[str]] = {}
        for col, rate in self.max_null_rates.items():
            by_rate.setdefault(rate, []).append(col)
        plan.extend((cols, rate) for rate, cols in sorted(by_rate.items()))
        return plan


# ─────────────────────────────────────────────────────────────────────────────
# BRONZE — raw ingestion checks (schema, row count, freshness, uniqueness)
# ─────────────────────────────────────────────────────────────────────────────
BRONZE_CONFIGS: dict[str, DomainCheckConfig] = {
    "customers": DomainCheckConfig(
        # phone is optional at signup
        max_null_rates={"phone": 0.15},
        pk_col="customer_id",
        required_columns=["customer_id", "email", "first_name", "last_name"],
        optional_columns=["phone", "address_line1", "date_of_birth"],
        timestamp_col="_ingested_at",
        sla_hours=26.0,
    ),
    "products": DomainCheckConfig(
        pk_col="sku",
        required_columns=["sku", "name", "list_price", "cost_price"],
        optional_columns=["supplier_id", "category_l1", "category_l2"],
        timestamp_col="_ingested_at",
        sla_hours=26.0,
        numeric_ranges={"list_price": (0.01, 1_000_000), "cost_price": (0.01, 1_000_000)},
    ),
    "suppliers": DomainCheckConfig(
        pk_col="supplier_id",
        required_columns=["supplier_id", "name", "country"],
        optional_columns=["on_time_rate", "quality_score"],
        timestamp_col="_ingested_at",
        sla_hours=26.0,
        numeric_ranges={
            "on_time_rate": (0.0, 1.0),
            "quality_score": (0.0, 10.0),
        },
    ),
    "stores": DomainCheckConfig(
        pk_col="store_id",
        required_columns=["store_id", "city", "state", "country", "store_type"],
        optional_columns=["sq_footage", "num_employees"],
        timestamp_col="_ingested_at",
        sla_hours=26.0,
        numeric_ranges={
            "sq_footage": (0, 1_000_000),
            "num_employees": (1, 10_000),
        },
    ),
    "employees": DomainCheckConfig(
        # corporate and warehouse staff are not assigned to a store
        max_null_rates={"store_id": 0.98},
        pk_col="employee_id",
        required_columns=["employee_id", "first_name", "last_name", "department", "job_title"],
        optional_columns=["email", "hire_date", "base_salary", "store_id", "manager_id"],
        timestamp_col="_ingested_at",
        sla_hours=48.0,
        numeric_ranges={"base_salary": (0.0, 10_000_000)},
    ),
    "orders": DomainCheckConfig(
        # web and mobile orders have no store
        max_null_rates={"store_id": 0.90},
        pk_col="order_id",
        required_columns=["order_id", "customer_id", "order_date", "status"],
        optional_columns=["store_id", "total_amount", "discount_amount"],
        timestamp_col="_ingested_at",
        sla_hours=26.0,
        numeric_ranges={
            "total_amount": (0.0, 10_000_000),
            "discount_amount": (0.0, 1_000_000),
        },
    ),
    "order_lines": DomainCheckConfig(
        pk_col="order_line_id",
        required_columns=[
            "order_line_id",
            "order_id",
            "sku",
            "quantity",
            "unit_price",
            "line_total",
        ],
        timestamp_col="_ingested_at",
        sla_hours=26.0,
        numeric_ranges={
            "quantity": (1, 100_000),
            "unit_price": (0.01, 1_000_000),
            "line_total": (0.0, 10_000_000),
        },
    ),
    "payments": DomainCheckConfig(
        pk_col="payment_id",
        required_columns=["payment_id", "order_id", "payment_method", "amount", "status"],
        optional_columns=[],
        timestamp_col="_ingested_at",
        sla_hours=26.0,
        numeric_ranges={"amount": (0.0, 10_000_000)},
    ),
    "shipments": DomainCheckConfig(
        # not yet delivered => no delivery date (about a quarter of shipments)
        max_null_rates={"actual_delivery_date": 0.40},
        pk_col="shipment_id",
        required_columns=["shipment_id", "order_id", "carrier", "status"],
        optional_columns=[
            "tracking_number",
            "ship_date",
            "actual_delivery_date",
            "estimated_delivery_date",
        ],
        timestamp_col="_ingested_at",
        sla_hours=26.0,
    ),
    "returns": DomainCheckConfig(
        pk_col="return_id",
        required_columns=["return_id", "order_id", "reason", "status"],
        optional_columns=["initiated_at", "refund_amount"],
        timestamp_col="_ingested_at",
        sla_hours=48.0,
        numeric_ranges={"refund_amount": (0.0, 10_000_000)},
    ),
    "inventory": DomainCheckConfig(
        pk_col="inventory_id",
        required_columns=["inventory_id", "store_id", "sku", "quantity_on_hand", "snapshot_date"],
        optional_columns=["reorder_point", "reorder_quantity"],
        timestamp_col="_ingested_at",
        sla_hours=26.0,
        numeric_ranges={
            "quantity_on_hand": (0, 10_000_000),
            "reorder_point": (0, 1_000_000),
            "reorder_quantity": (0, 1_000_000),
        },
    ),
    "reviews": DomainCheckConfig(
        pk_col="review_id",
        required_columns=["review_id", "sku", "rating"],
        optional_columns=["customer_id", "body", "created_at"],
        timestamp_col="_ingested_at",
        sla_hours=48.0,
        numeric_ranges={"rating": (1.0, 5.0)},
    ),
    "clickstream": DomainCheckConfig(
        # organic (non-campaign) traffic carries no campaign id
        max_null_rates={"campaign_id": 0.90},
        pk_col="event_id",
        required_columns=["event_id", "session_id", "anonymous_id", "event_type", "created_at"],
        optional_columns=["page_url", "campaign_id", "device_type"],
        timestamp_col="_ingested_at",
        sla_hours=2.0,
    ),
    "iot_sensors": DomainCheckConfig(
        pk_col="reading_id",
        required_columns=[
            "reading_id",
            "sensor_id",
            "store_id",
            "sensor_type",
            "value",
            "reading_at",
        ],
        optional_columns=["unit", "alert_threshold_low", "alert_threshold_high"],
        timestamp_col="_ingested_at",
        sla_hours=2.0,
        numeric_ranges={"value": (-1_000.0, 100_000.0)},
    ),
    "finance": DomainCheckConfig(
        pk_col="transaction_id",
        required_columns=[
            "transaction_id",
            "debit_account_code",
            "debit_amount",
            "credit_amount",
            "transaction_date",
            "description",
        ],
        optional_columns=["cost_center"],
        timestamp_col="_ingested_at",
        sla_hours=48.0,
        numeric_ranges={
            "debit_amount": (0.0, 100_000_000),
            "credit_amount": (0.0, 100_000_000),
        },
    ),
    "campaigns": DomainCheckConfig(
        pk_col="campaign_id",
        required_columns=["campaign_id", "campaign_name", "platform", "start_date"],
        optional_columns=["end_date", "budget", "actual_spend", "impressions", "clicks"],
        timestamp_col="_ingested_at",
        sla_hours=48.0,
        numeric_ranges={
            "budget": (0.0, 100_000_000),
            "actual_spend": (0.0, 100_000_000),
            "impressions": (0, None),
            "clicks": (0, None),
        },
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# SILVER — cleansed, PII-tokenised; adds FK integrity + business-rule ranges
# ─────────────────────────────────────────────────────────────────────────────
SILVER_CONFIGS: dict[str, DomainCheckConfig] = {
    "customers": DomainCheckConfig(
        pk_col="customer_id",
        required_columns=[
            "customer_id",
            "email_hash",
            "first_name_hash",
            "last_name_hash",
            "loyalty_tier",
        ],
        optional_columns=["age_bucket", "years_as_customer", "loyalty_tier_rank"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
    ),
    "products": DomainCheckConfig(
        pk_col="sku",
        required_columns=["sku", "name", "unit_price", "cost_price", "category"],
        optional_columns=["supplier_id", "margin_amount", "margin_pct"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
        numeric_ranges={
            "unit_price": (0.01, 1_000_000),
            "margin_pct": (-1.0, 10.0),
        },
        fk_checks=[("supplier_id", "suppliers", "supplier_id")],
    ),
    "suppliers": DomainCheckConfig(
        pk_col="supplier_id",
        required_columns=["supplier_id", "name", "country", "supplier_score"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
        numeric_ranges={
            "supplier_score": (0.0, 1.0),
            "on_time_rate": (0.0, 1.0),
            "quality_score": (0.0, 10.0),
        },
    ),
    "stores": DomainCheckConfig(
        pk_col="store_id",
        required_columns=["store_id", "city", "state", "country", "store_type", "store_size_band"],
        optional_columns=["square_feet", "employee_count", "sq_ft_per_employee"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
    ),
    "employees": DomainCheckConfig(
        # corporate and warehouse staff are not assigned to a store
        max_null_rates={"store_id": 0.98},
        pk_col="employee_id",
        required_columns=[
            "employee_id",
            "first_name_hash",
            "last_name_hash",
            "department",
            "total_compensation",
        ],
        optional_columns=["store_id", "manager_id", "tenure_years"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
        numeric_ranges={
            "total_compensation": (0.0, 50_000_000),
            "tenure_years": (0.0, 60.0),
        },
        fk_checks=[("store_id", "stores", "store_id")],
    ),
    "orders": DomainCheckConfig(
        # web and mobile orders have no store
        max_null_rates={"store_id": 0.90},
        pk_col="order_id",
        required_columns=["order_id", "customer_id", "order_date", "status", "order_value_band"],
        optional_columns=["store_id", "campaign_id", "total_amount"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
        numeric_ranges={"total_amount": (0.0, 10_000_000)},
        fk_checks=[
            ("customer_id", "customers", "customer_id"),
            ("store_id", "stores", "store_id"),
        ],
    ),
    "order_lines": DomainCheckConfig(
        pk_col="order_line_id",
        required_columns=["order_line_id", "order_id", "sku", "quantity", "line_total"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
        numeric_ranges={
            "quantity": (1, 100_000),
            "effective_unit_price": (0.0, 1_000_000),
            "line_total": (0.0, 10_000_000),
        },
        fk_checks=[
            ("order_id", "orders", "order_id"),
            ("sku", "products", "sku"),
        ],
    ),
    "payments": DomainCheckConfig(
        pk_col="payment_id",
        required_columns=["payment_id", "order_id", "payment_method", "amount", "status"],
        optional_columns=["net_amount", "is_refunded"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
        numeric_ranges={"amount": (0.0, 10_000_000)},
        fk_checks=[("order_id", "orders", "order_id")],
    ),
    "shipments": DomainCheckConfig(
        # not yet delivered => no transit time / on-time verdict (about a quarter of shipments)
        max_null_rates={
            "days_in_transit": 0.40,
            "is_on_time": 0.40,
            "delivery_variance_days": 0.40,
        },
        pk_col="shipment_id",
        required_columns=["shipment_id", "order_id", "carrier", "status"],
        optional_columns=["days_in_transit", "is_on_time", "delivery_variance_days"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
        numeric_ranges={"days_in_transit": (0, 365)},
        fk_checks=[("order_id", "orders", "order_id")],
    ),
    "returns": DomainCheckConfig(
        # processed / refunded timestamps are only set once those steps happen
        max_null_rates={"days_to_process": 0.90, "days_to_refund": 0.98},
        pk_col="return_id",
        required_columns=["return_id", "order_id", "return_reason", "status"],
        optional_columns=["days_to_process", "days_to_refund", "is_fully_refunded"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
        fk_checks=[("order_id", "orders", "order_id")],
    ),
    "inventory": DomainCheckConfig(
        pk_col="inventory_id",
        required_columns=["inventory_id", "store_id", "sku", "quantity_on_hand", "stock_health"],
        optional_columns=["days_of_supply", "needs_reorder"],
        timestamp_col="_silver_processed_at",
        sla_hours=30.0,
        numeric_ranges={"quantity_on_hand": (0, 10_000_000)},
        fk_checks=[
            ("store_id", "stores", "store_id"),
            ("sku", "products", "sku"),
        ],
    ),
    "reviews": DomainCheckConfig(
        pk_col="review_id",
        required_columns=["review_id", "sku", "rating"],
        optional_columns=["sentiment_score", "body_length"],
        timestamp_col="_silver_processed_at",
        sla_hours=48.0,
        numeric_ranges={
            "rating": (1.0, 5.0),
            "sentiment_score": (-1.0, 1.0),
        },
    ),
    "clickstream": DomainCheckConfig(
        pk_col="event_id",
        required_columns=["event_id", "session_id", "user_id", "event_type"],
        optional_columns=["is_conversion", "is_cart_event", "device_category", "event_hour"],
        timestamp_col="_silver_processed_at",
        sla_hours=4.0,
    ),
    "iot_sensors": DomainCheckConfig(
        pk_col="reading_id",
        required_columns=[
            "reading_id",
            "device_id",
            "store_id",
            "metric_name",
            "value",
            "alert_priority",
        ],
        timestamp_col="_silver_processed_at",
        sla_hours=4.0,
        fk_checks=[("store_id", "stores", "store_id")],
    ),
    "finance": DomainCheckConfig(
        pk_col="transaction_id",
        required_columns=["transaction_id", "account_code", "account_type", "transaction_date"],
        optional_columns=["store_id", "fiscal_quarter", "is_revenue", "is_expense"],
        timestamp_col="_silver_processed_at",
        sla_hours=48.0,
        numeric_ranges={
            "debit_amount": (0.0, 100_000_000),
            "credit_amount": (0.0, 100_000_000),
        },
    ),
    "campaigns": DomainCheckConfig(
        pk_col="campaign_id",
        required_columns=["campaign_id", "campaign_name", "channel", "start_date"],
        optional_columns=["roi", "performance_band", "campaign_duration_days"],
        timestamp_col="_silver_processed_at",
        sla_hours=48.0,
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# GOLD — analytical star-schema checks (freshness, row count, SK completeness)
# ─────────────────────────────────────────────────────────────────────────────
GOLD_CONFIGS: dict[str, DomainCheckConfig] = {
    # Dimensions
    "dim_date": DomainCheckConfig(
        pk_col="sk_date_id",
        required_columns=["sk_date_id", "full_date", "year", "month", "day_of_week"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
        numeric_ranges={
            "year": (2020, 2031),
            "month": (1, 12),
        },
    ),
    "dim_location": DomainCheckConfig(
        # customer-derived locations carry state and country but no city
        max_null_rates={"city": 0.90},
        pk_col="sk_location_id",
        required_columns=["sk_location_id", "nk_location_key", "city", "country", "region"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
    ),
    "dim_supplier": DomainCheckConfig(
        pk_col="sk_supplier_id",
        required_columns=["sk_supplier_id", "nk_supplier_id", "supplier_name", "supplier_score"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
        numeric_ranges={"supplier_score": (0.0, 1.0)},
    ),
    "dim_store": DomainCheckConfig(
        pk_col="sk_store_id",
        required_columns=["sk_store_id", "nk_store_id", "city", "state", "country", "store_type"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
    ),
    "dim_employee": DomainCheckConfig(
        pk_col="sk_employee_id",
        required_columns=[
            "sk_employee_id",
            "nk_employee_id",
            "first_name_hash",
            "last_name_hash",
            "department",
        ],
        optional_columns=["sk_manager_id", "total_compensation"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
        numeric_ranges={"total_compensation": (0.0, 50_000_000)},
    ),
    "dim_product": DomainCheckConfig(
        pk_col="sk_product_id",
        required_columns=["sk_product_id", "nk_sku", "name", "category_l1", "list_price"],
        optional_columns=["sk_supplier_id", "margin_pct"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
        numeric_ranges={
            "list_price": (0.01, 1_000_000),
            "margin_pct": (-1.0, 10.0),
        },
    ),
    "dim_customer": DomainCheckConfig(
        pk_col="sk_customer_id",
        required_columns=[
            "sk_customer_id",
            "nk_customer_id",
            "loyalty_tier",
        ],
        optional_columns=["email_hash", "years_as_customer"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
    ),
    "dim_promotion": DomainCheckConfig(
        pk_col="sk_promotion_id",
        required_columns=["sk_promotion_id", "nk_campaign_id", "campaign_name", "channel"],
        optional_columns=["roi", "performance_band"],
        timestamp_col="_gold_processed_at",
        sla_hours=48.0,
    ),
    # Facts
    "fact_sales": DomainCheckConfig(
        # no source links an order to a campaign yet, so every sale has the "no promotion" key
        max_null_rates={"sk_promotion_id": 1.0},
        pk_col="nk_order_line_id",
        required_columns=[
            "nk_order_line_id",
            "sk_date_id",
            "sk_customer_id",
            "sk_product_id",
            "quantity",
            "line_total",
        ],
        optional_columns=["sk_store_id", "sk_promotion_id"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
        numeric_ranges={
            "quantity": (0, 100_000),
            "line_total": (0.0, 10_000_000),
        },
    ),
    "fact_inventory": DomainCheckConfig(
        pk_col="nk_inventory_id",
        required_columns=[
            "nk_inventory_id",
            "sk_date_id",
            "sk_store_id",
            "sk_product_id",
            "quantity_on_hand",
        ],
        optional_columns=["days_of_supply", "stock_health"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
        numeric_ranges={"quantity_on_hand": (0, 10_000_000)},
    ),
    "fact_marketing": DomainCheckConfig(
        pk_col="nk_marketing_id",
        required_columns=["nk_marketing_id", "sk_date_id", "sessions", "conversions"],
        optional_columns=["sk_promotion_id", "budget"],
        timestamp_col="_gold_processed_at",
        sla_hours=48.0,
        numeric_ranges={
            "sessions": (0, 100_000_000),
            "conversions": (0, 100_000_000),
        },
    ),
    "fact_returns": DomainCheckConfig(
        pk_col="nk_return_id",
        required_columns=[
            "nk_return_id",
            "sk_date_id",
            "sk_customer_id",
            "reason",
            "refund_amount",
        ],
        optional_columns=["sk_product_id", "sk_store_id"],
        timestamp_col="_gold_processed_at",
        sla_hours=48.0,
        numeric_ranges={"refund_amount": (0.0, 10_000_000)},
    ),
    "fact_shipments": DomainCheckConfig(
        # undelivered shipments have no transit time; web / mobile orders have no store
        max_null_rates={"days_in_transit": 0.40, "is_on_time": 0.40, "sk_store_id": 0.90},
        pk_col="nk_shipment_id",
        required_columns=["nk_shipment_id", "sk_date_id", "carrier", "status"],
        optional_columns=["sk_store_id", "days_in_transit", "is_on_time"],
        timestamp_col="_gold_processed_at",
        sla_hours=30.0,
        numeric_ranges={"days_in_transit": (0, 365)},
    ),
    "fact_finance": DomainCheckConfig(
        # journal entries are booked at company level, not per store
        max_null_rates={"sk_store_id": 1.0},
        pk_col="nk_transaction_id",
        required_columns=[
            "nk_transaction_id",
            "sk_date_id",
            "gl_account",
            "account_type",
            "debit_amount",
            "credit_amount",
        ],
        optional_columns=["sk_store_id", "fiscal_quarter"],
        timestamp_col="_gold_processed_at",
        sla_hours=48.0,
        numeric_ranges={
            "debit_amount": (0.0, 100_000_000),
            "credit_amount": (0.0, 100_000_000),
        },
    ),
}
