# Data Governance & Observability

How the platform describes, guards and watches its data: **data contracts**, **catalog**, **lineage**,
**alerting**, and the **metadata database** they all share.

## Data contracts (Silver)

`src/quality/contracts/` publishes one contract per Silver domain (16). A contract lists the columns
downstream layers depend on — type family, nullability, allowed values, numeric range — plus
`forbidden_columns` (raw PII that must never reach Silver) and the standard Silver metadata columns.

Contracts are checked on **every Silver run**, after the domain transform and before the write
(`BaseSilverProcessor.process`, stage 7b). Schema checks are free (`df.dtypes`); data checks run as a
**single Spark aggregation** covering nulls, enums, ranges and primary-key uniqueness.

| `DATA_CONTRACT_MODE` | Behaviour |
|---|---|
| `off` | No validation (rejected in production) |
| `warn` *(default in dev)* | Log + persist violations to `quality.contract_violations`; never fails the run |
| `enforce` *(default in staging/prod)* | Same, and raises `ContractViolationError` on any **error**-severity violation |

The default comes from `Settings.contract_mode` (environment-aware); `DATA_CONTRACT_MODE` overrides it.

Severities: missing column, type mismatch, forbidden column, null in a non-nullable column, invalid enum
value, duplicate/`NULL` key, too few rows → **error**. Out-of-range numerics → **warning**.

Staging and production start in `enforce`; run dev in `warn` while a contract is being changed. Changing a contract is a versioned
change: bump `CONTRACT_VERSION` in `definitions.py` for any breaking edit so consumers can pin.

Violations are emitted as structured `contract_violation` log events (Kibana: *Data contract violations*).

## Privacy: PII and PCI columns

`SILVER_PII_HANDLING` (`src/processing/silver/domain_meta.py`) is the single list of what is hashed or dropped;
`SILVER_PII_DOMAINS` is derived from it and the data contract lists the same columns as `forbidden_columns`.

| Domain | Handling |
|---|---|
| customers | `email`, `first_name`, `last_name` → keyed hash, `phone` / `address_line1` dropped, `date_of_birth` → `age_bucket` |
| employees | `email`, `first_name`, `last_name` → keyed hash |
| suppliers | `contact_email` → keyed hash, `contact_phone` dropped |
| stores | `address` dropped |
| payments | `card_exp`, `card_network` dropped (only `card_last4` is retained) |
| clickstream | `ip_address` → keyed hash |

Hashes are `sha256("<PII_HASH_KEY>|<lowercased value>")`, computed with Spark's built-in `sha2`. The key
(`Settings.pii_hash_key`, required in production) is what stops a dictionary attack on known emails or IPv4
addresses. Names get the same treatment (`first_name_hash`, `last_name_hash`), so Gold `dim_employee` supports joins
and de-duplication but not display of a name (TDR-006). Bronze, the immutable landing zone, keeps the raw values;
restrict access to it accordingly.

## Catalog

`catalog.tables` / `catalog.columns` describe what is actually on storage. `scripts/maintenance/sync_catalog.py`
(`make sync-catalog`, and the weekly `catalog_sync` task in `pipeline_maintenance`) reads each Delta table's
schema and `DESCRIBE DETAIL`, upserts the table and replaces its columns, and flags PII/PCI columns by name.
Tables not yet created are reported as `missing`, not errors. Pass `--count-rows` for exact counts.

Served by the API: `GET /catalog/tables`, `/catalog/tables/{layer.table}`, `/catalog/columns/{layer.table}`.
Table names are `layer.name` (e.g. `silver.orders`, `gold.fact_sales`).

## Lineage

Table-level lineage lives in `catalog.lineage`, seeded from the pipeline metadata
(`src/lineage/static_graph.py`):

```
kafka.<topic> → bronze.<domain> → silver.<domain> → gold.<table> → redshift|snowflake|bigquery.<table>
```

Bronze→Silver is 1:1; Silver→Gold mirrors the `read_silver()` calls in the Gold builders (a unit test fails
if a builder's sources drift from the declared graph). Query it with
`GET /catalog/lineage/{table}?direction=upstream|downstream&max_depth=N`, e.g. what feeds
`gold.fact_sales`, or what is affected if `bronze.customers` is late.

## Quality gates

`gold_batch` starts with `silver_dq_gate` and `warehouse_load` with `gold_dq_gate`: a `run_dq --fail-fast` task
that fails on any CRITICAL check and blocks everything downstream (TDR-005). The `data_quality` DAG at 10:00 is
the reporting run. `DQPassRateSensor` (`airflow/plugins/sensors/`) can wait on a healthy report where a schedule
alone is not enough.

## Retention

| Layer | Time travel kept | Enforced by |
|---|---|---|
| Bronze | 7 years | `delta.deletedFileRetentionDuration` at ingest, `delta_maintenance.py` refuses a shorter VACUUM, S3 lifecycle (2555 days) |
| Silver / Gold | 72 hours | `delta_maintenance.py` default |

## API access

Keys are scoped per router (`read:pipeline`, `read:quality`, `read:catalog`, or `*`). A missing key is 401, a key
without the scope is 403. Dev uses the seeded key in `scripts/setup/seed_dev_data.sql`; staging/production keys are
created with `scripts/deployment/create_api_key.py` (random, hashed at rest, expiring). Requests are rate limited per key (or per IP when unauthenticated) with a token bucket; over the limit is a 429 with `Retry-After`.

## Alerting & dashboards

| Signal | Where |
|---|---|
| API gauges (`retailnexus_*`) | `api/main.py`, refreshed every 60 s from the metadata DB |
| Alert rules | `monitoring/prometheus/rules/` — pipeline freshness, DQ pass rate (layer and per-domain), API, infra |
| Routing | `monitoring/alerts/alertmanager.yml` (dev, no notifications) · `alertmanager.prod.yml` (Slack + PagerDuty, secrets from files) |
| Grafana | *RetailNexus Pipeline Health*, *RetailNexus Data Quality* (per-layer and per-domain scores) |
| Logs | Logstash → `lakehouse-*` indices; `make setup-elk` installs the template, ILM policy and the Kibana dashboard *Lakehouse – Data Quality & Errors* |

## Metadata database

`scripts/setup/init_postgres.sql` is the single definition of the operational schema. Each layer writes only
its own columns of `pipeline.watermarks` (one row per domain):

| Writer | Columns |
|---|---|
| Bronze ingest | `bronze_last_ingest`, `bronze_watermark`, `bronze_rows_written`, `bronze_last_status` |
| Silver | `silver_last_run`, `silver_bronze_version`, `silver_rows_written` |
| Gold | `gold_last_run`, `gold_rows_written` |
| Warehouse loaders | `redshift_last_load`, `snowflake_last_load`, `bigquery_last_load` |

`tests/unit/test_postgres_schema.py` pins every column the code relies on, and
`tests/integration/test_postgres_contract.py` runs the real statements against PostgreSQL
(`TEST_DATABASE_URL=… pytest tests/integration/test_postgres_contract.py -m integration`).
