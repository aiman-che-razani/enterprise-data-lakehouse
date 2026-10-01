-- Migration V001: Create the Gold schema, groups and grants in Redshift.
-- Idempotent. Tables are created by V002__create_gold_tables.sql (generated from
-- ddl/gold_schema.sql by scripts/deployment/build_migrations.py).

CREATE SCHEMA IF NOT EXISTS gold;

-- Grants for BI / analyst roles
CREATE GROUP IF NOT EXISTS bi_analysts;
CREATE GROUP IF NOT EXISTS data_engineers;

GRANT USAGE ON SCHEMA gold TO GROUP bi_analysts;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO GROUP bi_analysts;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO GROUP bi_analysts;

GRANT ALL ON SCHEMA gold TO GROUP data_engineers;
GRANT ALL ON ALL TABLES IN SCHEMA gold TO GROUP data_engineers;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO GROUP data_engineers;
