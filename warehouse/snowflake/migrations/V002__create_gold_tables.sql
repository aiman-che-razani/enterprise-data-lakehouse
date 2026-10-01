-- Migration V002: create the Gold star-schema tables (snowflake).
-- GENERATED from ddl/gold_schema.sql by scripts/deployment/build_migrations.py — do not edit.
-- Idempotent: every statement is CREATE ... IF NOT EXISTS, so re-running never drops data.

-- =============================================================================
-- RetailNexus Gold Star Schema — Snowflake
-- =============================================================================
-- Design notes:
--   * Snowflake distributes data automatically — no explicit DISTKEY.
--   * CLUSTER BY provides micro-partition pruning on high-cardinality joins.
--   * Dimensions are full-refresh (OVERWRITE); facts use MERGE.
--   * All timestamps stored as TIMESTAMP_NTZ (no timezone) for consistency.
--   * Identifiers are unquoted, so Snowflake stores them upper-case; queries are case-insensitive.
--   * Every table is created with IF NOT EXISTS: re-running this file never drops data. Schema changes go in a
--     new migration (V00N__*.sql), never by editing an applied one.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS retailnexus;
CREATE SCHEMA IF NOT EXISTS retailnexus.gold;

USE SCHEMA retailnexus.gold;

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSIONS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_date (
    sk_date_id          INTEGER        NOT NULL,
    full_date           DATE           NOT NULL,
    day_of_week         SMALLINT       NOT NULL,
    day_name            VARCHAR(10)    NOT NULL,
    day_of_month        SMALLINT       NOT NULL,
    day_of_year         SMALLINT       NOT NULL,
    week_of_year        SMALLINT       NOT NULL,
    month               SMALLINT       NOT NULL,
    month_name          VARCHAR(10)    NOT NULL,
    quarter             SMALLINT       NOT NULL,
    year                SMALLINT       NOT NULL,
    is_weekend          BOOLEAN        NOT NULL,
    is_leap_year        BOOLEAN        NOT NULL,
    fiscal_quarter      VARCHAR(2)     NOT NULL,
    fiscal_year         SMALLINT       NOT NULL,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (sk_date_id)
);


CREATE TABLE IF NOT EXISTS dim_customer (
    sk_customer_id      NUMBER(19)     NOT NULL,
    nk_customer_id      VARCHAR(36)    NOT NULL,
    email_hash          VARCHAR(64),
    age_bucket          VARCHAR(10),
    segment             VARCHAR(20),
    loyalty_tier        VARCHAR(20),
    loyalty_tier_rank   SMALLINT,
    loyalty_points      INTEGER,
    country             VARCHAR(3),
    state               VARCHAR(50),
    channel_preference  VARCHAR(20),
    is_email_verified   BOOLEAN,
    is_sms_opted_in     BOOLEAN,
    is_active           BOOLEAN,
    years_as_customer   FLOAT,
    effective_from      TIMESTAMP_NTZ,
    effective_to        TIMESTAMP_NTZ,
    created_at          TIMESTAMP_NTZ,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (sk_customer_id)
)
CLUSTER BY (segment, loyalty_tier);


CREATE TABLE IF NOT EXISTS dim_product (
    sk_product_id       NUMBER(19)     NOT NULL,
    sk_supplier_id      NUMBER(19),
    nk_sku              VARCHAR(20)    NOT NULL,
    name                VARCHAR(200),
    category_l1         VARCHAR(100),
    category_l2         VARCHAR(100),
    list_price          NUMBER(10,2),
    cost_price          NUMBER(10,2),
    margin_amount       NUMBER(10,2),
    margin_pct          NUMBER(8,4),
    price_tier          VARCHAR(10),
    is_active           BOOLEAN,
    is_discontinued     BOOLEAN,
    effective_from      TIMESTAMP_NTZ,
    effective_to        TIMESTAMP_NTZ,
    created_at          TIMESTAMP_NTZ,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (sk_product_id)
)
CLUSTER BY (category_l1, category_l2);


CREATE TABLE IF NOT EXISTS dim_store (
    sk_store_id         NUMBER(19)     NOT NULL,
    nk_store_id         VARCHAR(36)    NOT NULL,
    store_name          VARCHAR(200),
    store_type          VARCHAR(50),
    city                VARCHAR(100),
    state               VARCHAR(50),
    country             VARCHAR(3),
    latitude            FLOAT,
    longitude           FLOAT,
    sq_footage          INTEGER,
    num_employees       INTEGER,
    store_size_band     VARCHAR(10),
    sq_ft_per_employee  NUMBER(8,1),
    is_active           BOOLEAN,
    opened_date         DATE,
    closed_date         DATE,
    effective_from      TIMESTAMP_NTZ,
    effective_to        TIMESTAMP_NTZ,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (sk_store_id)
)
CLUSTER BY (country, state);


CREATE TABLE IF NOT EXISTS dim_employee (
    sk_employee_id      NUMBER(19)     NOT NULL,
    sk_store_id         NUMBER(19),
    sk_manager_id       NUMBER(19),
    nk_employee_id      VARCHAR(36)    NOT NULL,
    first_name_hash     VARCHAR(64),
    last_name_hash      VARCHAR(64),
    email_hash          VARCHAR(64),
    department          VARCHAR(100),
    job_title           VARCHAR(200),
    level               VARCHAR(20),
    hire_date           DATE,
    termination_date    DATE,
    tenure_years        NUMBER(5,1),
    base_salary         NUMBER(10,2),
    bonus_pct           NUMBER(5,4),
    total_compensation  NUMBER(10,2),
    is_active           BOOLEAN,
    effective_from      TIMESTAMP_NTZ,
    effective_to        TIMESTAMP_NTZ,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (sk_employee_id)
)
CLUSTER BY (department, sk_store_id);


CREATE TABLE IF NOT EXISTS dim_supplier (
    sk_supplier_id      NUMBER(19)     NOT NULL,
    nk_supplier_id      VARCHAR(36)    NOT NULL,
    supplier_name       VARCHAR(200),
    tier                VARCHAR(20),
    on_time_rate        NUMBER(5,4),
    quality_score       NUMBER(5,2),
    supplier_score      NUMBER(5,4),
    is_preferred        BOOLEAN,
    payment_terms       VARCHAR(50),
    country             VARCHAR(3),
    is_active           BOOLEAN,
    effective_from      TIMESTAMP_NTZ,
    effective_to        TIMESTAMP_NTZ,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (sk_supplier_id)
);


CREATE TABLE IF NOT EXISTS dim_location (
    sk_location_id      NUMBER(19)     NOT NULL,
    nk_location_key     VARCHAR(300)   NOT NULL,
    city                VARCHAR(100),
    state               VARCHAR(50),
    country             VARCHAR(3),
    region              VARCHAR(20),
    latitude            FLOAT,
    longitude           FLOAT,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (sk_location_id)
)
CLUSTER BY (region, country);


CREATE TABLE IF NOT EXISTS dim_promotion (
    sk_promotion_id        NUMBER(19)     NOT NULL,
    nk_campaign_id         VARCHAR(36)    NOT NULL,
    campaign_name          VARCHAR(200),
    campaign_type          VARCHAR(50),
    channel                VARCHAR(50),
    objective              VARCHAR(50),
    start_date             DATE,
    end_date               DATE,
    budget                 NUMBER(12,2),
    campaign_duration_days INTEGER,
    is_active              BOOLEAN,
    roi                    NUMBER(8,4),
    performance_band       VARCHAR(20),
    _gold_processed_at     TIMESTAMP_NTZ,
    _gold_date             DATE,
    _batch_id              VARCHAR(50),
    _pipeline_version      VARCHAR(20),
    PRIMARY KEY (sk_promotion_id)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- FACTS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_sales (
    sk_customer_id        NUMBER(19)     NOT NULL,
    sk_product_id         NUMBER(19)     NOT NULL,
    sk_store_id           NUMBER(19)     NOT NULL,
    sk_date_id            INTEGER        NOT NULL,
    sk_promotion_id       NUMBER(19),
    nk_order_id           VARCHAR(36)    NOT NULL,
    nk_order_line_id      VARCHAR(36)    NOT NULL,
    order_status          VARCHAR(20),
    channel               VARCHAR(50),
    sku                   VARCHAR(20),
    quantity              INTEGER        NOT NULL,
    unit_price            NUMBER(10,4)   NOT NULL,
    effective_unit_price  NUMBER(10,4),
    line_total            NUMBER(12,2)   NOT NULL,
    line_discount_amount  NUMBER(10,2),
    is_discounted         BOOLEAN,
    order_created_at      TIMESTAMP_NTZ,
    _gold_processed_at    TIMESTAMP_NTZ,
    _gold_date            DATE,
    _batch_id             VARCHAR(50),
    _pipeline_version     VARCHAR(20),
    PRIMARY KEY (nk_order_line_id)
)
CLUSTER BY (sk_date_id, sk_customer_id);


CREATE TABLE IF NOT EXISTS fact_inventory (
    sk_store_id         NUMBER(19)     NOT NULL,
    sk_product_id       NUMBER(19)     NOT NULL,
    sk_date_id          INTEGER        NOT NULL,
    nk_inventory_id     VARCHAR(36)    NOT NULL,
    store_id            VARCHAR(36),
    sku                 VARCHAR(20),
    snapshot_date       DATE           NOT NULL,
    quantity_on_hand    INTEGER,
    quantity_available  INTEGER,
    quantity_reserved   INTEGER,
    reorder_point       INTEGER,
    max_stock_level     INTEGER,
    avg_daily_units     NUMBER(8,2),
    days_of_supply      NUMBER(8,1),
    abc_class           VARCHAR(1),
    bin_location        VARCHAR(20),
    stock_health        VARCHAR(20),
    needs_reorder       BOOLEAN,
    updated_at          TIMESTAMP_NTZ,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (nk_inventory_id)
)
CLUSTER BY (sk_date_id, sk_store_id);


CREATE TABLE IF NOT EXISTS fact_marketing (
    sk_promotion_id     NUMBER(19)     NOT NULL,
    sk_date_id          INTEGER        NOT NULL,
    nk_marketing_id     VARCHAR(100)   NOT NULL,
    campaign_id         VARCHAR(36),
    event_date          DATE           NOT NULL,
    campaign_type       VARCHAR(50),
    channel             VARCHAR(50),
    sessions            NUMBER(19),
    page_views          NUMBER(19),
    add_to_carts        NUMBER(19),
    conversions         NUMBER(19),
    budget              NUMBER(12,2),
    revenue_attributed  NUMBER(12,2),
    ctr                 NUMBER(6,4),
    conversion_rate     NUMBER(6,4),
    roi                 NUMBER(8,4),
    performance_band    VARCHAR(20),
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (nk_marketing_id)
)
CLUSTER BY (sk_date_id, sk_promotion_id);


CREATE TABLE IF NOT EXISTS fact_returns (
    sk_customer_id      NUMBER(19)     NOT NULL,
    sk_product_id       NUMBER(19),
    sk_store_id         NUMBER(19),
    sk_date_id          INTEGER        NOT NULL,
    nk_return_id        VARCHAR(36)    NOT NULL,
    nk_order_id         VARCHAR(36),
    sku                 VARCHAR(20),
    reason              VARCHAR(100),
    status              VARCHAR(20),
    return_amount       NUMBER(10,2),
    refund_amount       NUMBER(10,2),
    quantity_returned   INTEGER,
    days_to_process     INTEGER,
    days_to_refund      INTEGER,
    is_fully_refunded   BOOLEAN,
    initiated_at        TIMESTAMP_NTZ,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (nk_return_id)
)
CLUSTER BY (sk_date_id, sk_customer_id);


CREATE TABLE IF NOT EXISTS fact_shipments (
    sk_store_id               NUMBER(19),
    sk_customer_id            NUMBER(19),
    sk_date_id                INTEGER        NOT NULL,
    nk_shipment_id            VARCHAR(36)    NOT NULL,
    nk_order_id               VARCHAR(36),
    carrier                   VARCHAR(20),
    service_level             VARCHAR(50),
    tracking_number           VARCHAR(100),
    shipping_cost             NUMBER(8,2),
    ship_date                 DATE,
    estimated_delivery_date   DATE,
    actual_delivery_date      DATE,
    days_in_transit           INTEGER,
    is_on_time                BOOLEAN,
    delivery_variance_days    INTEGER,
    status                    VARCHAR(20),
    _gold_processed_at        TIMESTAMP_NTZ,
    _gold_date                DATE,
    _batch_id                 VARCHAR(50),
    _pipeline_version         VARCHAR(20),
    PRIMARY KEY (nk_shipment_id)
)
CLUSTER BY (sk_date_id, carrier);


CREATE TABLE IF NOT EXISTS fact_finance (
    sk_date_id          INTEGER        NOT NULL,
    sk_store_id         NUMBER(19),
    nk_transaction_id   VARCHAR(36)    NOT NULL,
    transaction_type    VARCHAR(50),
    gl_account          VARCHAR(10),
    account_type        VARCHAR(20),
    debit_amount        NUMBER(14,2)   NOT NULL,
    credit_amount       NUMBER(14,2)   NOT NULL,
    is_revenue          BOOLEAN,
    is_expense          BOOLEAN,
    fiscal_quarter      VARCHAR(2),
    transaction_date    DATE           NOT NULL,
    description         VARCHAR(500),
    posted_at           TIMESTAMP_NTZ,
    _gold_processed_at  TIMESTAMP_NTZ,
    _gold_date          DATE,
    _batch_id           VARCHAR(50),
    _pipeline_version   VARCHAR(20),
    PRIMARY KEY (nk_transaction_id)
)
CLUSTER BY (sk_date_id, account_type);
