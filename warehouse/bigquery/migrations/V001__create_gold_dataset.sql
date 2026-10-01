-- Migration V001: Create RetailNexus Gold dataset in BigQuery
-- Run via: bq query --use_legacy_sql=false < V001__create_gold_dataset.sql

-- Create the gold dataset (region: US multi-region)
-- Note: bq mk must be run separately (DDL CREATE SCHEMA is standard SQL)
CREATE SCHEMA IF NOT EXISTS `retailnexus.gold`
OPTIONS (
    location = 'US',
    description = 'RetailNexus Gold star-schema — cleaned, dimensionally-modelled data'
);

-- Row-level access policy example (apply after tables are created)
-- CREATE ROW ACCESS POLICY pii_restriction
-- ON `retailnexus.gold.dim_customer`
-- GRANT TO ('group:bi-analysts@retailnexus.com')
-- FILTER USING (TRUE);  -- analysts see all rows but PII columns are already tokenised
