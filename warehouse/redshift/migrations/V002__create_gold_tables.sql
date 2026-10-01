-- Migration V002: create the Gold star-schema tables (redshift).
-- GENERATED from ddl/gold_schema.sql by scripts/deployment/build_migrations.py — do not edit.
-- Idempotent: every statement is CREATE ... IF NOT EXISTS, so re-running never drops data.

-- =============================================================================
-- RetailNexus Gold Star Schema — Amazon Redshift
-- =============================================================================
-- Distribution strategy:
--   Dimensions  → DISTSTYLE ALL  (small tables; broadcast to all slices)
--                 except dim_customer (500K+ rows): DISTKEY(sk_customer_id), co-located with the facts
--   Fact tables → DISTKEY on highest-cardinality join column
--                 COMPOUND SORTKEY starting with sk_date_id (time-range pruning)
-- Encoding:
--   Integers / dates → AZ64  (Redshift's best general-purpose numeric codec)
--   Strings          → ZSTD  (high-compression, fast decompression)
--   Booleans         → RAW   (1 byte; no benefit from encoding)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSIONS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.dim_date (
    sk_date_id          INTEGER      NOT NULL ENCODE AZ64,
    full_date           DATE         NOT NULL ENCODE AZ64,
    day_of_week         SMALLINT     NOT NULL ENCODE AZ64,
    day_name            VARCHAR(10)  NOT NULL ENCODE ZSTD,
    day_of_month        SMALLINT     NOT NULL ENCODE AZ64,
    day_of_year         SMALLINT     NOT NULL ENCODE AZ64,
    week_of_year        SMALLINT     NOT NULL ENCODE AZ64,
    month               SMALLINT     NOT NULL ENCODE AZ64,
    month_name          VARCHAR(10)  NOT NULL ENCODE ZSTD,
    quarter             SMALLINT     NOT NULL ENCODE AZ64,
    year                SMALLINT     NOT NULL ENCODE AZ64,
    is_weekend          BOOLEAN      NOT NULL ENCODE RAW,
    is_leap_year        BOOLEAN      NOT NULL ENCODE RAW,
    fiscal_quarter      VARCHAR(2)   NOT NULL ENCODE ZSTD,
    fiscal_year         SMALLINT     NOT NULL ENCODE AZ64,
    _gold_processed_at  TIMESTAMP    ENCODE AZ64,
    _gold_date          DATE         ENCODE AZ64,
    _batch_id           VARCHAR(50)  ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)  ENCODE ZSTD,
    PRIMARY KEY (sk_date_id)
)
DISTSTYLE ALL
COMPOUND SORTKEY (year, month, sk_date_id);


CREATE TABLE IF NOT EXISTS gold.dim_customer (
    sk_customer_id      BIGINT       NOT NULL ENCODE AZ64,
    nk_customer_id      VARCHAR(36)  NOT NULL ENCODE ZSTD,
    email_hash          VARCHAR(64)  ENCODE ZSTD,
    age_bucket          VARCHAR(10)  ENCODE ZSTD,
    segment             VARCHAR(20)  ENCODE ZSTD,
    loyalty_tier        VARCHAR(20)  ENCODE ZSTD,
    loyalty_tier_rank   SMALLINT     ENCODE AZ64,
    loyalty_points      INTEGER      ENCODE AZ64,
    country             VARCHAR(3)   ENCODE ZSTD,
    state               VARCHAR(50)  ENCODE ZSTD,
    channel_preference  VARCHAR(20)  ENCODE ZSTD,
    is_email_verified   BOOLEAN      ENCODE RAW,
    is_sms_opted_in     BOOLEAN      ENCODE RAW,
    is_active           BOOLEAN      ENCODE RAW,
    years_as_customer   FLOAT4       ENCODE AZ64,
    effective_from      TIMESTAMP    ENCODE AZ64,
    effective_to        TIMESTAMP    ENCODE AZ64,
    created_at          TIMESTAMP    ENCODE AZ64,
    _gold_processed_at  TIMESTAMP    ENCODE AZ64,
    _gold_date          DATE         ENCODE AZ64,
    _batch_id           VARCHAR(50)  ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)  ENCODE ZSTD,
    PRIMARY KEY (sk_customer_id)
)
DISTSTYLE KEY
DISTKEY (sk_customer_id)
COMPOUND SORTKEY (segment, loyalty_tier, sk_customer_id);


CREATE TABLE IF NOT EXISTS gold.dim_product (
    sk_product_id       BIGINT        NOT NULL ENCODE AZ64,
    sk_supplier_id      BIGINT        ENCODE AZ64,
    nk_sku              VARCHAR(20)   NOT NULL ENCODE ZSTD,
    name                VARCHAR(200)  ENCODE ZSTD,
    category_l1         VARCHAR(100)  ENCODE ZSTD,
    category_l2         VARCHAR(100)  ENCODE ZSTD,
    list_price          DECIMAL(10,2) ENCODE AZ64,
    cost_price          DECIMAL(10,2) ENCODE AZ64,
    margin_amount       DECIMAL(10,2) ENCODE AZ64,
    margin_pct          DECIMAL(8,4)  ENCODE AZ64,
    price_tier          VARCHAR(10)   ENCODE ZSTD,
    is_active           BOOLEAN       ENCODE RAW,
    is_discontinued     BOOLEAN       ENCODE RAW,
    effective_from      TIMESTAMP     ENCODE AZ64,
    effective_to        TIMESTAMP     ENCODE AZ64,
    created_at          TIMESTAMP     ENCODE AZ64,
    _gold_processed_at  TIMESTAMP     ENCODE AZ64,
    _gold_date          DATE          ENCODE AZ64,
    _batch_id           VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (sk_product_id)
)
DISTSTYLE ALL
COMPOUND SORTKEY (category_l1, category_l2, sk_product_id);


CREATE TABLE IF NOT EXISTS gold.dim_store (
    sk_store_id         BIGINT        NOT NULL ENCODE AZ64,
    nk_store_id         VARCHAR(36)   NOT NULL ENCODE ZSTD,
    store_name          VARCHAR(200)  ENCODE ZSTD,
    store_type          VARCHAR(50)   ENCODE ZSTD,
    city                VARCHAR(100)  ENCODE ZSTD,
    state               VARCHAR(50)   ENCODE ZSTD,
    country             VARCHAR(3)    ENCODE ZSTD,
    latitude            FLOAT8        ENCODE AZ64,
    longitude           FLOAT8        ENCODE AZ64,
    sq_footage          INTEGER       ENCODE AZ64,
    num_employees       INTEGER       ENCODE AZ64,
    store_size_band     VARCHAR(10)   ENCODE ZSTD,
    sq_ft_per_employee  DECIMAL(8,1)  ENCODE AZ64,
    is_active           BOOLEAN       ENCODE RAW,
    opened_date         DATE          ENCODE AZ64,
    closed_date         DATE          ENCODE AZ64,
    effective_from      TIMESTAMP     ENCODE AZ64,
    effective_to        TIMESTAMP     ENCODE AZ64,
    _gold_processed_at  TIMESTAMP     ENCODE AZ64,
    _gold_date          DATE          ENCODE AZ64,
    _batch_id           VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (sk_store_id)
)
DISTSTYLE ALL
COMPOUND SORTKEY (country, state, sk_store_id);


CREATE TABLE IF NOT EXISTS gold.dim_employee (
    sk_employee_id      BIGINT        NOT NULL ENCODE AZ64,
    sk_store_id         BIGINT        ENCODE AZ64,
    sk_manager_id       BIGINT        ENCODE AZ64,
    nk_employee_id      VARCHAR(36)   NOT NULL ENCODE ZSTD,
    first_name_hash     VARCHAR(64)   ENCODE ZSTD,
    last_name_hash      VARCHAR(64)   ENCODE ZSTD,
    email_hash          VARCHAR(64)   ENCODE ZSTD,
    department          VARCHAR(100)  ENCODE ZSTD,
    job_title           VARCHAR(200)  ENCODE ZSTD,
    level               VARCHAR(20)   ENCODE ZSTD,
    hire_date           DATE          ENCODE AZ64,
    termination_date    DATE          ENCODE AZ64,
    tenure_years        DECIMAL(5,1)  ENCODE AZ64,
    base_salary         DECIMAL(10,2) ENCODE AZ64,
    bonus_pct           DECIMAL(5,4)  ENCODE AZ64,
    total_compensation  DECIMAL(10,2) ENCODE AZ64,
    is_active           BOOLEAN       ENCODE RAW,
    effective_from      TIMESTAMP     ENCODE AZ64,
    effective_to        TIMESTAMP     ENCODE AZ64,
    _gold_processed_at  TIMESTAMP     ENCODE AZ64,
    _gold_date          DATE          ENCODE AZ64,
    _batch_id           VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (sk_employee_id)
)
DISTSTYLE ALL
COMPOUND SORTKEY (department, sk_store_id, sk_employee_id);


CREATE TABLE IF NOT EXISTS gold.dim_supplier (
    sk_supplier_id      BIGINT        NOT NULL ENCODE AZ64,
    nk_supplier_id      VARCHAR(36)   NOT NULL ENCODE ZSTD,
    supplier_name       VARCHAR(200)  ENCODE ZSTD,
    tier                VARCHAR(20)   ENCODE ZSTD,
    on_time_rate        DECIMAL(5,4)  ENCODE AZ64,
    quality_score       DECIMAL(5,2)  ENCODE AZ64,
    supplier_score      DECIMAL(5,4)  ENCODE AZ64,
    is_preferred        BOOLEAN       ENCODE RAW,
    payment_terms       VARCHAR(50)   ENCODE ZSTD,
    country             VARCHAR(3)    ENCODE ZSTD,
    is_active           BOOLEAN       ENCODE RAW,
    effective_from      TIMESTAMP     ENCODE AZ64,
    effective_to        TIMESTAMP     ENCODE AZ64,
    _gold_processed_at  TIMESTAMP     ENCODE AZ64,
    _gold_date          DATE          ENCODE AZ64,
    _batch_id           VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (sk_supplier_id)
)
DISTSTYLE ALL
COMPOUND SORTKEY (tier, is_preferred, sk_supplier_id);


CREATE TABLE IF NOT EXISTS gold.dim_location (
    sk_location_id      BIGINT        NOT NULL ENCODE AZ64,
    nk_location_key     VARCHAR(300)  NOT NULL ENCODE ZSTD,
    city                VARCHAR(100)  ENCODE ZSTD,
    state               VARCHAR(50)   ENCODE ZSTD,
    country             VARCHAR(3)    ENCODE ZSTD,
    region              VARCHAR(20)   ENCODE ZSTD,
    latitude            FLOAT8        ENCODE AZ64,
    longitude           FLOAT8        ENCODE AZ64,
    _gold_processed_at  TIMESTAMP     ENCODE AZ64,
    _gold_date          DATE          ENCODE AZ64,
    _batch_id           VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (sk_location_id)
)
DISTSTYLE ALL
COMPOUND SORTKEY (region, country, state);


CREATE TABLE IF NOT EXISTS gold.dim_promotion (
    sk_promotion_id        BIGINT        NOT NULL ENCODE AZ64,
    nk_campaign_id         VARCHAR(36)   NOT NULL ENCODE ZSTD,
    campaign_name          VARCHAR(200)  ENCODE ZSTD,
    campaign_type          VARCHAR(50)   ENCODE ZSTD,
    channel                VARCHAR(50)   ENCODE ZSTD,
    objective              VARCHAR(50)   ENCODE ZSTD,
    start_date             DATE          ENCODE AZ64,
    end_date               DATE          ENCODE AZ64,
    budget                 DECIMAL(12,2) ENCODE AZ64,
    campaign_duration_days INTEGER       ENCODE AZ64,
    is_active              BOOLEAN       ENCODE RAW,
    roi                    DECIMAL(8,4)  ENCODE AZ64,
    performance_band       VARCHAR(20)   ENCODE ZSTD,
    _gold_processed_at     TIMESTAMP     ENCODE AZ64,
    _gold_date             DATE          ENCODE AZ64,
    _batch_id              VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version      VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (sk_promotion_id)
)
DISTSTYLE ALL
COMPOUND SORTKEY (campaign_type, channel, sk_promotion_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- FACTS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sk_customer_id        BIGINT        NOT NULL ENCODE AZ64,
    sk_product_id         BIGINT        NOT NULL ENCODE AZ64,
    sk_store_id           BIGINT        NOT NULL ENCODE AZ64,
    sk_date_id            INTEGER       NOT NULL ENCODE AZ64,
    sk_promotion_id       BIGINT        ENCODE AZ64,
    nk_order_id           VARCHAR(36)   NOT NULL ENCODE ZSTD,
    nk_order_line_id      VARCHAR(36)   NOT NULL ENCODE ZSTD,
    order_status          VARCHAR(20)   ENCODE ZSTD,
    channel               VARCHAR(50)   ENCODE ZSTD,
    sku                   VARCHAR(20)   ENCODE ZSTD,
    quantity              INTEGER       NOT NULL ENCODE AZ64,
    unit_price            DECIMAL(10,4) NOT NULL ENCODE AZ64,
    effective_unit_price  DECIMAL(10,4) ENCODE AZ64,
    line_total            DECIMAL(12,2) NOT NULL ENCODE AZ64,
    line_discount_amount  DECIMAL(10,2) ENCODE AZ64,
    is_discounted         BOOLEAN       ENCODE RAW,
    order_created_at      TIMESTAMP     ENCODE AZ64,
    _gold_processed_at    TIMESTAMP     ENCODE AZ64,
    _gold_date            DATE          ENCODE AZ64,
    _batch_id             VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version     VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (nk_order_line_id)
)
DISTKEY (sk_customer_id)
COMPOUND SORTKEY (sk_date_id, sk_customer_id, sk_product_id);


CREATE TABLE IF NOT EXISTS gold.fact_inventory (
    sk_store_id         BIGINT       NOT NULL ENCODE AZ64,
    sk_product_id       BIGINT       NOT NULL ENCODE AZ64,
    sk_date_id          INTEGER      NOT NULL ENCODE AZ64,
    nk_inventory_id     VARCHAR(36)  NOT NULL ENCODE ZSTD,
    store_id            VARCHAR(36)  ENCODE ZSTD,
    sku                 VARCHAR(20)  ENCODE ZSTD,
    snapshot_date       DATE         NOT NULL ENCODE AZ64,
    quantity_on_hand    INTEGER      ENCODE AZ64,
    quantity_available  INTEGER      ENCODE AZ64,
    quantity_reserved   INTEGER      ENCODE AZ64,
    reorder_point       INTEGER      ENCODE AZ64,
    max_stock_level     INTEGER      ENCODE AZ64,
    avg_daily_units     DECIMAL(8,2) ENCODE AZ64,
    days_of_supply      DECIMAL(8,1) ENCODE AZ64,
    abc_class           VARCHAR(1)   ENCODE ZSTD,
    bin_location        VARCHAR(20)  ENCODE ZSTD,
    stock_health        VARCHAR(20)  ENCODE ZSTD,
    needs_reorder       BOOLEAN      ENCODE RAW,
    updated_at          TIMESTAMP    ENCODE AZ64,
    _gold_processed_at  TIMESTAMP    ENCODE AZ64,
    _gold_date          DATE         ENCODE AZ64,
    _batch_id           VARCHAR(50)  ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)  ENCODE ZSTD,
    PRIMARY KEY (nk_inventory_id)
)
DISTKEY (sk_store_id)
COMPOUND SORTKEY (sk_date_id, sk_store_id, sk_product_id);


CREATE TABLE IF NOT EXISTS gold.fact_marketing (
    sk_promotion_id     BIGINT        NOT NULL ENCODE AZ64,
    sk_date_id          INTEGER       NOT NULL ENCODE AZ64,
    nk_marketing_id     VARCHAR(100)  NOT NULL ENCODE ZSTD,
    campaign_id         VARCHAR(36)   ENCODE ZSTD,
    event_date          DATE          NOT NULL ENCODE AZ64,
    campaign_type       VARCHAR(50)   ENCODE ZSTD,
    channel             VARCHAR(50)   ENCODE ZSTD,
    sessions            BIGINT        ENCODE AZ64,
    page_views          BIGINT        ENCODE AZ64,
    add_to_carts        BIGINT        ENCODE AZ64,
    conversions         BIGINT        ENCODE AZ64,
    budget              DECIMAL(12,2) ENCODE AZ64,
    revenue_attributed  DECIMAL(12,2) ENCODE AZ64,
    ctr                 DECIMAL(6,4)  ENCODE AZ64,
    conversion_rate     DECIMAL(6,4)  ENCODE AZ64,
    roi                 DECIMAL(8,4)  ENCODE AZ64,
    performance_band    VARCHAR(20)   ENCODE ZSTD,
    _gold_processed_at  TIMESTAMP     ENCODE AZ64,
    _gold_date          DATE          ENCODE AZ64,
    _batch_id           VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (nk_marketing_id)
)
DISTKEY (sk_promotion_id)
COMPOUND SORTKEY (sk_date_id, sk_promotion_id);


CREATE TABLE IF NOT EXISTS gold.fact_returns (
    sk_customer_id      BIGINT        NOT NULL ENCODE AZ64,
    sk_product_id       BIGINT        ENCODE AZ64,
    sk_store_id         BIGINT        ENCODE AZ64,
    sk_date_id          INTEGER       NOT NULL ENCODE AZ64,
    nk_return_id        VARCHAR(36)   NOT NULL ENCODE ZSTD,
    nk_order_id         VARCHAR(36)   ENCODE ZSTD,
    sku                 VARCHAR(20)   ENCODE ZSTD,
    reason              VARCHAR(100)  ENCODE ZSTD,
    status              VARCHAR(20)   ENCODE ZSTD,
    return_amount       DECIMAL(10,2) ENCODE AZ64,
    refund_amount       DECIMAL(10,2) ENCODE AZ64,
    quantity_returned   INTEGER       ENCODE AZ64,
    days_to_process     INTEGER       ENCODE AZ64,
    days_to_refund      INTEGER       ENCODE AZ64,
    is_fully_refunded   BOOLEAN       ENCODE RAW,
    initiated_at        TIMESTAMP     ENCODE AZ64,
    _gold_processed_at  TIMESTAMP     ENCODE AZ64,
    _gold_date          DATE          ENCODE AZ64,
    _batch_id           VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (nk_return_id)
)
DISTKEY (sk_customer_id)
COMPOUND SORTKEY (sk_date_id, sk_customer_id);


CREATE TABLE IF NOT EXISTS gold.fact_shipments (
    sk_store_id               BIGINT       ENCODE AZ64,
    sk_customer_id            BIGINT       ENCODE AZ64,
    sk_date_id                INTEGER      NOT NULL ENCODE AZ64,
    nk_shipment_id            VARCHAR(36)  NOT NULL ENCODE ZSTD,
    nk_order_id               VARCHAR(36)  ENCODE ZSTD,
    carrier                   VARCHAR(20)  ENCODE ZSTD,
    service_level             VARCHAR(50)  ENCODE ZSTD,
    tracking_number           VARCHAR(100) ENCODE ZSTD,
    shipping_cost             DECIMAL(8,2) ENCODE AZ64,
    ship_date                 DATE         ENCODE AZ64,
    estimated_delivery_date   DATE         ENCODE AZ64,
    actual_delivery_date      DATE         ENCODE AZ64,
    days_in_transit           INTEGER      ENCODE AZ64,
    is_on_time                BOOLEAN      ENCODE RAW,
    delivery_variance_days    INTEGER      ENCODE AZ64,
    status                    VARCHAR(20)  ENCODE ZSTD,
    _gold_processed_at        TIMESTAMP    ENCODE AZ64,
    _gold_date                DATE         ENCODE AZ64,
    _batch_id                 VARCHAR(50)  ENCODE ZSTD,
    _pipeline_version         VARCHAR(20)  ENCODE ZSTD,
    PRIMARY KEY (nk_shipment_id)
)
DISTKEY (sk_store_id)
COMPOUND SORTKEY (sk_date_id, carrier, sk_store_id);


CREATE TABLE IF NOT EXISTS gold.fact_finance (
    sk_date_id          INTEGER       NOT NULL ENCODE AZ64,
    sk_store_id         BIGINT        ENCODE AZ64,
    nk_transaction_id   VARCHAR(36)   NOT NULL ENCODE ZSTD,
    transaction_type    VARCHAR(50)   ENCODE ZSTD,
    gl_account          VARCHAR(10)   ENCODE ZSTD,
    account_type        VARCHAR(20)   ENCODE ZSTD,
    debit_amount        DECIMAL(14,2) NOT NULL ENCODE AZ64,
    credit_amount       DECIMAL(14,2) NOT NULL ENCODE AZ64,
    is_revenue          BOOLEAN       ENCODE RAW,
    is_expense          BOOLEAN       ENCODE RAW,
    fiscal_quarter      VARCHAR(2)    ENCODE ZSTD,
    transaction_date    DATE          NOT NULL ENCODE AZ64,
    description         VARCHAR(500)  ENCODE ZSTD,
    posted_at           TIMESTAMP     ENCODE AZ64,
    _gold_processed_at  TIMESTAMP     ENCODE AZ64,
    _gold_date          DATE          ENCODE AZ64,
    _batch_id           VARCHAR(50)   ENCODE ZSTD,
    _pipeline_version   VARCHAR(20)   ENCODE ZSTD,
    PRIMARY KEY (nk_transaction_id)
)
DISTKEY (sk_date_id)
COMPOUND SORTKEY (sk_date_id, account_type, transaction_type);
