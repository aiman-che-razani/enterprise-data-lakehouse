"""
Silver-layer data contracts for all 16 domains.

Every contract is the *published guarantee* to Gold builders and downstream
consumers — not an exhaustive schema. Add a column here only when something
downstream depends on it. Bump ``version`` on any breaking change (removed
column, narrowed type, tightened enum) so consumers can pin.
"""

from __future__ import annotations

from .models import ColumnContract, DataContract, TypeFamily

CONTRACT_VERSION = "1.0.0"
OWNER = "data-engineering"


# ─── Column helpers ───────────────────────────────────────────────────────────
def _s(name: str, nullable: bool = True, allowed: tuple[str, ...] | None = None) -> ColumnContract:
    return ColumnContract(name, TypeFamily.STRING, nullable, allowed_values=allowed)


def _i(
    name: str, lo: float | None = None, hi: float | None = None, nullable: bool = True
) -> ColumnContract:
    return ColumnContract(name, TypeFamily.INTEGER, nullable, min_value=lo, max_value=hi)


def _d(
    name: str, lo: float | None = None, hi: float | None = None, nullable: bool = True
) -> ColumnContract:
    return ColumnContract(name, TypeFamily.DOUBLE, nullable, min_value=lo, max_value=hi)


def _b(name: str, nullable: bool = True) -> ColumnContract:
    return ColumnContract(name, TypeFamily.BOOLEAN, nullable)


def _ts(name: str, nullable: bool = True) -> ColumnContract:
    return ColumnContract(name, TypeFamily.TIMESTAMP, nullable)


def _dt(name: str, nullable: bool = True) -> ColumnContract:
    return ColumnContract(name, TypeFamily.DATE, nullable)


# Columns every Silver table carries (added by BaseSilverProcessor).
SILVER_METADATA_COLUMNS: tuple[ColumnContract, ...] = (
    _i("_bronze_version", nullable=False),
    _ts("_silver_processed_at", nullable=False),
    _dt("_silver_date", nullable=False),
    _s("_batch_id", nullable=False),
    _s("_pipeline_version", nullable=False),
)


def _contract(
    domain: str,
    pk: str,
    columns: list[ColumnContract],
    forbidden: tuple[str, ...] = (),
    description: str = "",
) -> DataContract:
    return DataContract(
        domain=domain,
        version=CONTRACT_VERSION,
        owner=OWNER,
        primary_key=pk,
        columns=tuple(columns) + SILVER_METADATA_COLUMNS,
        forbidden_columns=forbidden,
        description=description,
    )


_ORDER_STATUS = ("confirmed", "processing", "shipped", "delivered", "cancelled", "returned")
_PAYMENT_STATUS = ("paid", "failed", "refunded", "partially_refunded", "pending")
_SEGMENTS = ("platinum", "gold", "silver", "bronze")

CONTRACTS: dict[str, DataContract] = {
    c.domain: c
    for c in [
        _contract(
            "suppliers",
            "supplier_id",
            [
                _s("supplier_id", False),
                _s("name"),
                _s("tier"),
                _s("country"),
                _s("contact_email_hash"),
                _i("lead_time_days", 0),
                _d("on_time_rate", 0.0, 1.0),
                _d("quality_score", 0.0, 10.0),
                _b("is_active"),
                _ts("created_at"),
                _ts("updated_at"),
            ],
            forbidden=("contact_email", "contact_phone"),
        ),
        _contract(
            "stores",
            "store_id",
            [
                _s("store_id", False),
                _s("name"),
                _s("store_type"),
                _s("city"),
                _i("sq_footage", 0),
                _i("num_employees", 0),
                _b("is_active"),
                _ts("created_at"),
                _ts("updated_at"),
            ],
            forbidden=("address",),
        ),
        _contract(
            "employees",
            "employee_id",
            [
                _s("employee_id", False),
                _s("first_name_hash"),
                _s("last_name_hash"),
                _s("email_hash"),
                _s("department"),
                _s("job_title"),
                _d("base_salary", 0.0),
                _dt("hire_date"),
                _b("is_active"),
                _ts("created_at"),
                _ts("updated_at"),
            ],
            forbidden=("email", "first_name", "last_name"),
            description="Employee email and names are hashed (email_hash, first_name_hash, last_name_hash); the raw values must not reach Silver.",
        ),
        _contract(
            "products",
            "sku",
            [
                _s("sku", False),
                _s("name"),
                _s("category_l1"),
                _s("supplier_id"),
                _d("list_price", 0.0),
                _d("cost_price", 0.0),
                _b("is_active"),
                _ts("created_at"),
                _ts("updated_at"),
            ],
        ),
        _contract(
            "customers",
            "customer_id",
            [
                _s("customer_id", False),
                _s("first_name_hash"),
                _s("last_name_hash"),
                _s("email_hash", False),
                _s("age_bucket"),
                _s("segment", allowed=_SEGMENTS),
                _i("loyalty_points", 0),
                _s("country"),
                _b("is_active"),
                _ts("created_at"),
                _ts("updated_at"),
            ],
            forbidden=(
                "email",
                "first_name",
                "last_name",
                "phone",
                "address_line1",
                "date_of_birth",
            ),
            description="PII contract: email and names are hashed, phone/address dropped, DOB bucketed.",
        ),
        _contract(
            "campaigns",
            "campaign_id",
            [
                _s("campaign_id", False),
                _s("campaign_name"),
                _s("campaign_type"),
                _d("budget", 0.0),
                _d("actual_spend", 0.0),
                _b("is_active"),
                _ts("created_at"),
            ],
        ),
        _contract(
            "orders",
            "order_id",
            [
                _s("order_id", False),
                _s("customer_id", False),
                _dt("order_date"),
                _s("status", allowed=_ORDER_STATUS),
                _d("subtotal", 0.0),
                _d("discount_amount", 0.0),
                _d("tax_amount", 0.0),
                _d("total_amount", 0.0),
                _ts("created_at"),
                _ts("updated_at"),
            ],
        ),
        _contract(
            "order_lines",
            "order_line_id",
            [
                _s("order_line_id", False),
                _s("order_id", False),
                _s("sku", False),
                _i("quantity", 1),
                _d("unit_price", 0.0),
                _d("line_total", 0.0),
                _ts("created_at"),
            ],
        ),
        _contract(
            "payments",
            "payment_id",
            [
                _s("payment_id", False),
                _s("order_id", False),
                _s("status", allowed=_PAYMENT_STATUS),
                _d("amount", 0.0),
                _d("refund_amount", 0.0),
                _ts("created_at"),
                _ts("updated_at"),
            ],
            forbidden=("card_exp", "card_network"),
            description="PCI: only card_last4 may be retained.",
        ),
        _contract(
            "shipments",
            "shipment_id",
            [
                _s("shipment_id", False),
                _s("order_id", False),
                _s("carrier"),
                _s("status"),
                _d("shipping_cost", 0.0),
                _ts("ship_date"),
                _ts("created_at"),
            ],
        ),
        _contract(
            "returns",
            "return_id",
            [
                _s("return_id", False),
                _s("order_id", False),
                _s("sku"),
                _i("quantity", 1),
                _d("refund_amount", 0.0),
                _s("status"),
                _ts("initiated_at"),
            ],
        ),
        _contract(
            "inventory",
            "inventory_id",
            [
                _s("inventory_id", False),
                _s("sku", False),
                _s("store_id", False),
                _dt("snapshot_date"),
                _i("quantity_on_hand", 0),
                _i("reorder_point", 0),
            ],
        ),
        _contract(
            "reviews",
            "review_id",
            [_s("review_id", False), _s("sku", False), _i("rating", 1, 5), _ts("created_at")],
        ),
        _contract(
            "clickstream",
            "event_id",
            [
                _s("event_id", False),
                _s("session_id", False),
                _s("event_type", False),
                _s("ip_address_hash"),
                _ts("created_at"),
            ],
            forbidden=("ip_address",),
        ),
        _contract(
            "iot_sensors",
            "reading_id",
            [
                _s("reading_id", False),
                _s("sensor_id", False),
                _s("store_id"),
                _s("sensor_type"),
                _d("value"),
                _ts("reading_at"),
            ],
        ),
        _contract(
            "finance",
            "transaction_id",
            [
                _s("transaction_id", False),
                _dt("transaction_date"),
                _d("debit_amount", 0.0),
                _d("credit_amount", 0.0),
                _i("fiscal_quarter", 1, 4),
                _ts("posted_at"),
            ],
        ),
    ]
}


def get_contract(domain: str) -> DataContract | None:
    """Return the Silver contract for *domain*, or None if none is published."""
    return CONTRACTS.get(domain)
