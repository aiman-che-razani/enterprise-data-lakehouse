-- Migration V002: create the Gold star-schema tables (bigquery).
-- GENERATED from ddl/gold_schema.sql by scripts/deployment/build_migrations.py — do not edit.
-- Idempotent: every statement is CREATE ... IF NOT EXISTS, so re-running never drops data.

-- =============================================================================
-- RetailNexus Gold Star Schema — Google BigQuery
-- =============================================================================
-- Design notes:
--   * Daily partitioning: PARTITION BY _gold_date, except three facts that partition on a business
--     date (fact_inventory.snapshot_date, fact_marketing.event_date, fact_finance.transaction_date).
--     Queries should filter on the partition column to prune.
--   * CLUSTER BY on highest-cardinality join columns (up to 4 per table)
--   * BigQuery uses INT64 for all surrogate keys (BIGINT equivalent)
--   * TIMESTAMP (UTC) for all timestamps; DATE for date-only columns
--   * Fact tables are APPEND-mode with dedup on nk_* key via MERGE in ETL
-- =============================================================================

-- Create dataset (run via bq CLI or Terraform)
-- bq mk --location=US --dataset retailnexus:gold

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSIONS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `retailnexus.gold.dim_date` (
    sk_date_id          INT64         NOT NULL OPTIONS(description='YYYYMMDD integer surrogate key'),
    full_date           DATE          NOT NULL,
    day_of_week         INT64         NOT NULL OPTIONS(description='1=Sunday ... 7=Saturday'),
    day_name            STRING NOT NULL,
    day_of_month        INT64 NOT NULL,
    day_of_year         INT64 NOT NULL,
    week_of_year        INT64 NOT NULL,
    month               INT64 NOT NULL,
    month_name          STRING NOT NULL,
    quarter             INT64 NOT NULL,
    year                INT64 NOT NULL,
    is_weekend          BOOL NOT NULL,
    is_leap_year        BOOL NOT NULL,
    fiscal_quarter      STRING NOT NULL        OPTIONS(description='Q1/Q2/Q3/Q4'),
    fiscal_year         INT64 NOT NULL,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY _gold_date
CLUSTER BY year, quarter, month;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.dim_customer` (
    sk_customer_id      INT64         NOT NULL OPTIONS(description='xxhash64 surrogate key'),
    nk_customer_id      STRING        NOT NULL OPTIONS(description='UUID natural key'),
    email_hash          STRING        OPTIONS(description='SHA-256 of email — PII tokenised'),
    age_bucket          STRING        OPTIONS(description='e.g. "25-34"'),
    segment             STRING,
    loyalty_tier        STRING,
    loyalty_tier_rank   INT64,
    loyalty_points      INT64,
    country             STRING,
    state               STRING,
    channel_preference  STRING,
    is_email_verified   BOOL,
    is_sms_opted_in     BOOL,
    is_active           BOOL,
    years_as_customer   FLOAT64,
    effective_from      TIMESTAMP,
    effective_to        TIMESTAMP,
    created_at          TIMESTAMP,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY _gold_date
CLUSTER BY segment, loyalty_tier, sk_customer_id;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.dim_product` (
    sk_product_id       INT64         NOT NULL,
    sk_supplier_id      INT64,
    nk_sku              STRING        NOT NULL,
    name                STRING,
    category_l1         STRING,
    category_l2         STRING,
    list_price          NUMERIC,
    cost_price          NUMERIC,
    margin_amount       NUMERIC,
    margin_pct          NUMERIC,
    price_tier          STRING        OPTIONS(description='budget/mid/premium/luxury'),
    is_active           BOOL,
    is_discontinued     BOOL,
    effective_from      TIMESTAMP,
    effective_to        TIMESTAMP,
    created_at          TIMESTAMP,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY _gold_date
CLUSTER BY category_l1, category_l2;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.dim_store` (
    sk_store_id         INT64         NOT NULL,
    nk_store_id         STRING        NOT NULL,
    store_name          STRING,
    store_type          STRING,
    city                STRING,
    state               STRING,
    country             STRING,
    latitude            FLOAT64,
    longitude           FLOAT64,
    sq_footage          INT64,
    num_employees       INT64,
    store_size_band     STRING,
    sq_ft_per_employee  NUMERIC,
    is_active           BOOL,
    opened_date         DATE,
    closed_date         DATE,
    effective_from      TIMESTAMP,
    effective_to        TIMESTAMP,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY _gold_date
CLUSTER BY country, state;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.dim_employee` (
    sk_employee_id      INT64         NOT NULL,
    sk_store_id         INT64,
    sk_manager_id       INT64,
    nk_employee_id      STRING        NOT NULL,
    first_name_hash     STRING,
    last_name_hash      STRING,
    email_hash          STRING,
    department          STRING,
    job_title           STRING,
    level               STRING,
    hire_date           DATE,
    termination_date    DATE,
    tenure_years        NUMERIC,
    base_salary         NUMERIC,
    bonus_pct           NUMERIC,
    total_compensation  NUMERIC,
    is_active           BOOL,
    effective_from      TIMESTAMP,
    effective_to        TIMESTAMP,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY _gold_date
CLUSTER BY department;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.dim_supplier` (
    sk_supplier_id      INT64         NOT NULL,
    nk_supplier_id      STRING        NOT NULL,
    supplier_name       STRING,
    tier                STRING,
    on_time_rate        NUMERIC,
    quality_score       NUMERIC,
    supplier_score      NUMERIC,
    is_preferred        BOOL,
    payment_terms       STRING,
    country             STRING,
    is_active           BOOL,
    effective_from      TIMESTAMP,
    effective_to        TIMESTAMP,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY _gold_date
CLUSTER BY tier, is_preferred;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.dim_location` (
    sk_location_id      INT64         NOT NULL,
    nk_location_key     STRING        NOT NULL,
    city                STRING,
    state               STRING,
    country             STRING,
    region              STRING,
    latitude            FLOAT64,
    longitude           FLOAT64,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY _gold_date
CLUSTER BY region, country;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.dim_promotion` (
    sk_promotion_id        INT64    NOT NULL,
    nk_campaign_id         STRING   NOT NULL,
    campaign_name          STRING,
    campaign_type          STRING,
    channel                STRING,
    objective              STRING,
    start_date             DATE,
    end_date               DATE,
    budget                 NUMERIC,
    campaign_duration_days INT64,
    is_active              BOOL,
    roi                    NUMERIC,
    performance_band       STRING,
    _gold_processed_at     TIMESTAMP,
    _gold_date             DATE,
    _batch_id              STRING,
    _pipeline_version      STRING
)
PARTITION BY _gold_date
CLUSTER BY campaign_type, channel;


-- ─────────────────────────────────────────────────────────────────────────────
-- FACTS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `retailnexus.gold.fact_sales` (
    sk_customer_id        INT64     NOT NULL,
    sk_product_id         INT64     NOT NULL,
    sk_store_id           INT64     NOT NULL,
    sk_date_id            INT64     NOT NULL,
    sk_promotion_id       INT64,
    nk_order_id           STRING    NOT NULL,
    nk_order_line_id      STRING    NOT NULL,
    order_status          STRING,
    channel               STRING,
    sku                   STRING,
    quantity              INT64     NOT NULL,
    unit_price            NUMERIC   NOT NULL,
    effective_unit_price  NUMERIC,
    line_total            NUMERIC   NOT NULL,
    line_discount_amount  NUMERIC,
    is_discounted         BOOL,
    order_created_at      TIMESTAMP,
    _gold_processed_at    TIMESTAMP,
    _gold_date            DATE,
    _batch_id             STRING,
    _pipeline_version     STRING
)
PARTITION BY _gold_date
CLUSTER BY sk_customer_id, sk_product_id, sk_date_id;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.fact_inventory` (
    sk_store_id         INT64    NOT NULL,
    sk_product_id       INT64    NOT NULL,
    sk_date_id          INT64    NOT NULL,
    nk_inventory_id     STRING   NOT NULL,
    store_id            STRING,
    sku                 STRING,
    snapshot_date       DATE     NOT NULL,
    quantity_on_hand    INT64,
    quantity_available  INT64,
    quantity_reserved   INT64,
    reorder_point       INT64,
    max_stock_level     INT64,
    avg_daily_units     NUMERIC,
    days_of_supply      NUMERIC,
    abc_class           STRING,
    bin_location        STRING,
    stock_health        STRING,
    needs_reorder       BOOL,
    updated_at          TIMESTAMP,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY snapshot_date
CLUSTER BY sk_store_id, sk_product_id;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.fact_marketing` (
    sk_promotion_id     INT64    NOT NULL,
    sk_date_id          INT64    NOT NULL,
    nk_marketing_id     STRING   NOT NULL,
    campaign_id         STRING,
    event_date          DATE     NOT NULL,
    campaign_type       STRING,
    channel             STRING,
    sessions            INT64,
    page_views          INT64,
    add_to_carts        INT64,
    conversions         INT64,
    budget              NUMERIC,
    revenue_attributed  NUMERIC,
    ctr                 NUMERIC,
    conversion_rate     NUMERIC,
    roi                 NUMERIC,
    performance_band    STRING,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY event_date
CLUSTER BY sk_promotion_id, channel;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.fact_returns` (
    sk_customer_id      INT64    NOT NULL,
    sk_product_id       INT64,
    sk_store_id         INT64,
    sk_date_id          INT64    NOT NULL,
    nk_return_id        STRING   NOT NULL,
    nk_order_id         STRING,
    sku                 STRING,
    reason              STRING,
    status              STRING,
    return_amount       NUMERIC,
    refund_amount       NUMERIC,
    quantity_returned   INT64,
    days_to_process     INT64,
    days_to_refund      INT64,
    is_fully_refunded   BOOL,
    initiated_at        TIMESTAMP,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY _gold_date
CLUSTER BY sk_customer_id, reason;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.fact_shipments` (
    sk_store_id               INT64,
    sk_customer_id            INT64,
    sk_date_id                INT64    NOT NULL,
    nk_shipment_id            STRING   NOT NULL,
    nk_order_id               STRING,
    carrier                   STRING,
    service_level             STRING,
    tracking_number           STRING,
    shipping_cost             NUMERIC,
    ship_date                 DATE,
    estimated_delivery_date   DATE,
    actual_delivery_date      DATE,
    days_in_transit           INT64,
    is_on_time                BOOL,
    delivery_variance_days    INT64,
    status                    STRING,
    _gold_processed_at        TIMESTAMP,
    _gold_date                DATE,
    _batch_id                 STRING,
    _pipeline_version         STRING
)
PARTITION BY _gold_date
CLUSTER BY sk_store_id, carrier;


CREATE TABLE IF NOT EXISTS `retailnexus.gold.fact_finance` (
    sk_date_id          INT64    NOT NULL,
    sk_store_id         INT64,
    nk_transaction_id   STRING   NOT NULL,
    transaction_type    STRING,
    gl_account          STRING,
    account_type        STRING,
    debit_amount        NUMERIC  NOT NULL,
    credit_amount       NUMERIC  NOT NULL,
    is_revenue          BOOL,
    is_expense          BOOL,
    fiscal_quarter      STRING,
    transaction_date    DATE     NOT NULL,
    description         STRING,
    posted_at           TIMESTAMP,
    _gold_processed_at  TIMESTAMP,
    _gold_date          DATE,
    _batch_id           STRING,
    _pipeline_version   STRING
)
PARTITION BY transaction_date
CLUSTER BY account_type, transaction_type;
