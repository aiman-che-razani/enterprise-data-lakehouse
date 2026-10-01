-- ═══════════════════════════════════════════════════════════════════════════════
-- RetailNexus Data Lakehouse — PostgreSQL Initialisation
-- Runs automatically on first container start via /docker-entrypoint-initdb.d/
-- Creates: lakehouse DB, airflow DB, roles, schemas, pipeline state tables
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── Create additional roles ──────────────────────────────────────────────────
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
  END IF;
END$$;

-- ─── Create additional databases ──────────────────────────────────────────────
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE lakehouse_test OWNER lakehouse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'lakehouse_test')\gexec

-- ─── Connect to lakehouse DB ──────────────────────────────────────────────────
\c lakehouse

-- ─── Extensions ───────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- ─── Schemas ──────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS pipeline;
CREATE SCHEMA IF NOT EXISTS quality;
CREATE SCHEMA IF NOT EXISTS catalog;
CREATE SCHEMA IF NOT EXISTS api;

-- ─── Pipeline State — Watermark Store ─────────────────────────────────────────
-- One row per domain (or Gold table). Every layer writes only its own columns:
--   Bronze ingest  → bronze_*        Silver processors → silver_*
--   Gold builders  → gold_*          Warehouse loaders → {target}_last_load
CREATE TABLE IF NOT EXISTS pipeline.watermarks (
    domain                VARCHAR(128)  NOT NULL PRIMARY KEY,
    bronze_last_ingest    TIMESTAMPTZ,
    bronze_watermark      TIMESTAMPTZ,
    bronze_rows_written   BIGINT,
    bronze_last_status    VARCHAR(32)   DEFAULT 'never_run',
    silver_last_run       TIMESTAMPTZ,
    silver_bronze_version BIGINT,
    silver_rows_written   BIGINT,
    gold_last_run         TIMESTAMPTZ,
    gold_rows_written     BIGINT,
    redshift_last_load    TIMESTAMPTZ,
    snowflake_last_load   TIMESTAMPTZ,
    bigquery_last_load    TIMESTAMPTZ,
    created_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ─── Pipeline Runs ────────────────────────────────────────────────────────────
-- Audit log of pipeline executions (Airflow DAG runs and Spark jobs).
CREATE TABLE IF NOT EXISTS pipeline.runs (
    id                  BIGSERIAL     PRIMARY KEY,
    run_id              VARCHAR(256)  NOT NULL UNIQUE,
    dag_id              VARCHAR(256)  NOT NULL,
    task_id             VARCHAR(256),
    execution_date      VARCHAR(32),
    layer               VARCHAR(16),
    status              VARCHAR(32)   NOT NULL DEFAULT 'running',
    rows_read           BIGINT        DEFAULT 0,
    rows_written        BIGINT        DEFAULT 0,
    rows_rejected       BIGINT        DEFAULT 0,
    error_message       TEXT,
    started_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    ended_at            TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_runs_dag_id   ON pipeline.runs(dag_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status   ON pipeline.runs(status, started_at DESC);

-- ─── Pipeline SLA Tracking ────────────────────────────────────────────────────
-- One row per freshness breach detected by the DQ framework or the SLA DAG.
CREATE TABLE IF NOT EXISTS pipeline.sla_misses (
    id                  BIGSERIAL     PRIMARY KEY,
    run_id              VARCHAR(256),
    domain              VARCHAR(128)  NOT NULL,
    layer               VARCHAR(16),
    metric              VARCHAR(256)  NOT NULL,
    actual_value        DOUBLE PRECISION,
    sla_threshold       DOUBLE PRECISION,
    message             TEXT,
    detected_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sla_detected ON pipeline.sla_misses(detected_at DESC);

-- ─── Quality — Check Results ──────────────────────────────────────────────────
-- Status values are lower-case CheckStatus values: passed | failed | warning | error.
CREATE TABLE IF NOT EXISTS quality.check_results (
    id                  BIGSERIAL     PRIMARY KEY,
    run_id              VARCHAR(256)  NOT NULL,
    domain              VARCHAR(128)  NOT NULL,
    layer               VARCHAR(16)   NOT NULL,
    check_name          VARCHAR(256)  NOT NULL,
    column_name         VARCHAR(128),
    status              VARCHAR(16)   NOT NULL,
    metric_value        DOUBLE PRECISION,
    threshold           DOUBLE PRECISION,
    message             TEXT,
    rows_checked        BIGINT,
    run_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qc_run_id  ON quality.check_results(run_id);
CREATE INDEX IF NOT EXISTS idx_qc_domain  ON quality.check_results(domain, layer, check_name, run_at DESC);
CREATE INDEX IF NOT EXISTS idx_qc_status  ON quality.check_results(status, run_at DESC);

-- ─── Quality — Report Summaries ───────────────────────────────────────────────
-- One row per (run, domain, layer). pass_rate is a fraction in [0, 1].
CREATE TABLE IF NOT EXISTS quality.reports (
    id                  BIGSERIAL     PRIMARY KEY,
    run_id              VARCHAR(256)  NOT NULL,
    domain              VARCHAR(128)  NOT NULL,
    layer               VARCHAR(16)   NOT NULL,
    total_checks        INTEGER       NOT NULL DEFAULT 0,
    passed              INTEGER       NOT NULL DEFAULT 0,
    failed              INTEGER       NOT NULL DEFAULT 0,
    warnings            INTEGER       NOT NULL DEFAULT 0,
    errors              INTEGER       NOT NULL DEFAULT 0,
    pass_rate           DOUBLE PRECISION NOT NULL DEFAULT 0,
    run_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, domain, layer)     -- a retried run updates its report instead of duplicating it
);

CREATE INDEX IF NOT EXISTS idx_reports_domain ON quality.reports(domain, layer, run_at DESC);
CREATE INDEX IF NOT EXISTS idx_reports_run_at ON quality.reports(run_at DESC);

-- ─── Quality — Data Contract Violations ───────────────────────────────────────
-- Written by src/quality/contracts when a Silver run breaks its data contract.
CREATE TABLE IF NOT EXISTS quality.contract_violations (
    id                  BIGSERIAL     PRIMARY KEY,
    run_id              VARCHAR(256),
    domain              VARCHAR(128)  NOT NULL,
    contract_version    VARCHAR(32)   NOT NULL,
    rule                VARCHAR(128)  NOT NULL,
    column_name         VARCHAR(128),
    severity            VARCHAR(16)   NOT NULL,
    message             TEXT,
    detected_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_contract_domain ON quality.contract_violations(domain, detected_at DESC);

-- ─── Catalog — Table Registry ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS catalog.tables (
    table_id            UUID          NOT NULL DEFAULT uuid_generate_v4(),
    schema_name         VARCHAR(64)   NOT NULL,
    table_name          VARCHAR(256)  NOT NULL,
    layer               VARCHAR(16)   NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
    domain              VARCHAR(128)  NOT NULL,
    description         TEXT,
    owner_team          VARCHAR(128),
    s3_location         TEXT,
    format              VARCHAR(32)   DEFAULT 'delta',
    partition_columns   TEXT[],
    primary_key_columns TEXT[],
    row_count           BIGINT,
    size_bytes          BIGINT,
    schema_version      VARCHAR(32),
    schema_json         JSONB,
    tags                TEXT[],
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_id),
    UNIQUE (schema_name, table_name)
);

-- ─── Catalog — Column Registry ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS catalog.columns (
    id                  SERIAL        PRIMARY KEY,
    table_id            UUID          NOT NULL REFERENCES catalog.tables(table_id) ON DELETE CASCADE,
    column_name         VARCHAR(128)  NOT NULL,
    data_type           VARCHAR(64)   NOT NULL,
    ordinal_position    INTEGER,
    is_nullable         BOOLEAN       DEFAULT TRUE,
    is_pii              BOOLEAN       DEFAULT FALSE,
    is_pci              BOOLEAN       DEFAULT FALSE,
    description         TEXT,
    example_values      TEXT[],
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_columns_table_id ON catalog.columns(table_id);

-- ─── Lineage — Edges ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS catalog.lineage (
    id                  SERIAL        PRIMARY KEY,
    source_table        VARCHAR(256)  NOT NULL,
    target_table        VARCHAR(256)  NOT NULL,
    transformation_type VARCHAR(64),
    job_name            VARCHAR(256)  NOT NULL DEFAULT '',   -- NULLs would defeat the UNIQUE key below
    run_id              VARCHAR(256),
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (source_table, target_table, job_name)
);

-- ─── API — Key Store ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS api.api_keys (
    key_id              UUID          NOT NULL DEFAULT uuid_generate_v4(),
    key_hash            VARCHAR(256)  NOT NULL UNIQUE,
    key_prefix          VARCHAR(16)   NOT NULL,
    client_name         VARCHAR(128)  NOT NULL,
    permissions         JSONB         NOT NULL DEFAULT '[]'::jsonb,
    is_active           BOOLEAN       NOT NULL DEFAULT TRUE,
    expires_at          TIMESTAMPTZ,
    last_used_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (key_id)
);

-- ─── Seed: one watermark row per domain ───────────────────────────────────────
INSERT INTO pipeline.watermarks (domain)
VALUES
    ('customers'), ('orders'), ('order_lines'), ('products'), ('inventory'),
    ('payments'), ('shipments'), ('returns'), ('campaigns'), ('clickstream'),
    ('reviews'), ('iot_sensors'), ('suppliers'), ('stores'), ('employees'), ('finance')
ON CONFLICT (domain) DO NOTHING;

-- Gold builders and warehouse loaders keep one row per Gold table (keyed by table name) in the
-- same table; seed them so the API and dashboards list every table before its first run.
INSERT INTO pipeline.watermarks (domain)
VALUES
    ('dim_date'), ('dim_location'), ('dim_supplier'), ('dim_store'), ('dim_employee'),
    ('dim_product'), ('dim_customer'), ('dim_promotion'),
    ('fact_sales'), ('fact_inventory'), ('fact_marketing'), ('fact_returns'),
    ('fact_shipments'), ('fact_finance')
ON CONFLICT (domain) DO NOTHING;

-- ─── Update trigger for updated_at ────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pipeline.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_watermarks_updated_at ON pipeline.watermarks;
CREATE TRIGGER trg_watermarks_updated_at
    BEFORE UPDATE ON pipeline.watermarks
    FOR EACH ROW EXECUTE FUNCTION pipeline.set_updated_at();

DROP TRIGGER IF EXISTS trg_catalog_tables_updated_at ON catalog.tables;
CREATE TRIGGER trg_catalog_tables_updated_at
    BEFORE UPDATE ON catalog.tables
    FOR EACH ROW EXECUTE FUNCTION pipeline.set_updated_at();

-- Done
\echo 'RetailNexus PostgreSQL schema initialised successfully.'
