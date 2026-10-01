# RetailNexus Data Lakehouse — Development Roadmap

> **Project:** Enterprise Data Lakehouse Platform  
> **Scope:** Full production-grade implementation  
> **Methodology:** 2-week sprints · 10 sprints · ~20 weeks  
> **Delivery:** Module-by-module, each fully tested and documented before moving to the next

---

## Milestones

| Milestone | Target Sprint | Description |
|---|---|---|
| **M1 — Foundation** | Sprint 1 end | Repo, CI/CD, Docker, all services start cleanly |
| **M2 — Data Ready** | Sprint 2 end | 1 M+ synthetic records generated and validated |
| **M3 — Bronze Live** | Sprint 3 end | All 14 ingestion patterns operational |
| **M4 — Silver Live** | Sprint 4 end | Full cleanse/validate/SCD2 pipeline running |
| **M5 — Gold Live** | Sprint 5 end | Dimensional model populated, queries verified |
| **M6 — Warehouses Live** | Sprint 6 end | All three warehouse targets loaded and queryable |
| **M7 — Quality Live** | Sprint 7 end | 80+ DQ checks running, reports published |
| **M8 — Orchestration Live** | Sprint 8 end | All Airflow DAGs scheduled, monitored, alerting |
| **M9 — Serving Live** | Sprint 9 end | API healthy, monitoring dashboards live |
| **M10 — Production Ready** | Sprint 10 end | Test coverage > 90%, docs complete, hardened |

---

## Sprint Plan

---

### Sprint 1 — Foundation & Infrastructure (Weeks 1–2)

**Goal:** Get the skeleton standing. Every developer can clone the repo, run `make up`, and have every service healthy within 5 minutes.

#### Epic 1.1 — Repository Bootstrap

| Task | File / Component | Priority |
|---|---|---|
| Initialize Git repository with `.gitignore` | `.gitignore` | P0 |
| Write `pyproject.toml` (Black, isort, ruff, mypy, pytest config) | `pyproject.toml` | P0 |
| Write `Makefile` with developer targets | `Makefile` | P0 |
| Write `.env.example` | `.env.example` | P0 |
| Write `LICENSE` | `LICENSE` | P1 |

#### Epic 1.2 — Docker Compose Stack

| Task | File / Component | Priority |
|---|---|---|
| Spark Master + Worker Dockerfile | `docker/spark/Dockerfile` | P0 |
| Airflow Dockerfile (custom image with deps) | `docker/airflow/Dockerfile` | P0 |
| FastAPI Dockerfile | `docker/api/Dockerfile` | P0 |
| Full `docker-compose.yml` (14 services) | `docker-compose.yml` | P0 |
| Compose health checks for all services | `docker-compose.yml` | P0 |
| MinIO bucket initialization script | `scripts/setup/init_minio.sh` | P0 |
| PostgreSQL initialization SQL | `scripts/setup/init_postgres.sql` | P0 |

#### Epic 1.3 — Python Package Structure

| Task | File / Component | Priority |
|---|---|---|
| `src/common/config/` — env-aware config loader | `src/common/config/settings.py` | P0 |
| `src/common/logging/` — structured JSON logger | `src/common/logging/logger.py` | P0 |
| `src/common/security/` — secrets client | `src/common/security/secrets.py` | P1 |
| All `__init__.py` files | Throughout `src/` | P0 |

#### Epic 1.4 — CI/CD Pipeline

| Task | File / Component | Priority |
|---|---|---|
| GitHub Actions CI workflow (lint + type + test) | `.github/workflows/ci.yml` | P0 |
| GitHub Actions CD-Staging workflow | `.github/workflows/cd-staging.yml` | P1 |
| GitHub Actions CD-Prod workflow | `.github/workflows/cd-prod.yml` | P1 |
| PR template | `.github/PULL_REQUEST_TEMPLATE.md` | P2 |

#### Sprint 1 Definition of Done

- [ ] `make up` starts all 14 services with no errors
- [ ] All service health checks pass
- [ ] `make lint` passes (zero ruff/mypy violations)
- [ ] `make test` runs (even if only smoke tests exist)
- [ ] GitHub Actions CI passes on first PR

---

### Sprint 2 — Synthetic Data Generation (Weeks 3–4)

**Goal:** Produce ≥ 1 million records across all 14 operational domains with realistic distributions, relationships, and temporal patterns.

#### Epic 2.1 — Data Generator Framework

| Task | File / Component | Priority |
|---|---|---|
| Base generator class with Faker integration | `datasets/generators/base_generator.py` | P0 |
| Shared entity registries (customer IDs, SKUs, etc.) | `datasets/generators/registries.py` | P0 |
| Reproducible seed management | `datasets/generators/seed_config.py` | P0 |
| CLI entrypoint (`make generate-data`) | `scripts/data-generation/generate_all.py` | P0 |

#### Epic 2.2 — Domain Data Generators

| Domain | Generator File | Record Count | Format |
|---|---|---|---|
| Customers | `generators/customers.py` | 500,000 | JSON |
| Orders | `generators/orders.py` | 1,000,000 | JSON |
| Order Lines | `generators/order_lines.py` | 2,500,000 | JSON |
| Products | `generators/products.py` | 50,000 | CSV |
| Inventory | `generators/inventory.py` | 500,000 | JSON |
| Payments | `generators/payments.py` | 1,000,000 | JSON |
| Shipments | `generators/shipments.py` | 800,000 | JSON |
| Returns | `generators/returns.py` | 150,000 | CSV |
| Marketing Campaigns | `generators/campaigns.py` | 10,000 | CSV |
| Clickstream | `generators/clickstream.py` | 5,000,000 | JSON (gzipped) |
| Customer Reviews | `generators/reviews.py` | 200,000 | JSON |
| IoT Sensors | `generators/iot_sensors.py` | 2,000,000 | JSON |
| Suppliers | `generators/suppliers.py` | 2,000 | CSV |
| Employees | `generators/employees.py` | 5,000 | CSV |
| Finance Transactions | `generators/finance.py` | 500,000 | Parquet |

#### Epic 2.3 — Schema Definitions

| Task | File / Component |
|---|---|
| JSON schemas for all 14 domains | `datasets/schemas/*.json` |
| Spark StructType definitions | `spark/schemas/*.py` |
| Pydantic models for API/ingestion | `src/common/schemas/*.py` |

#### Epic 2.4 — Kafka Producer Simulation

| Task | File / Component |
|---|---|
| Kafka producer for orders | `src/ingestion/streaming/producers/orders_producer.py` |
| Kafka producer for payments | `src/ingestion/streaming/producers/payments_producer.py` |
| Kafka producer for clickstream | `src/ingestion/streaming/producers/clickstream_producer.py` |
| Kafka producer for inventory | `src/ingestion/streaming/producers/inventory_producer.py` |
| Kafka producer for IoT sensors | `src/ingestion/streaming/producers/sensor_producer.py` |
| Kafka topic creation script | `scripts/setup/create_kafka_topics.sh` |

#### Sprint 2 Definition of Done

- [x] `make generate-data` produces ≥ 1 M records total in `datasets/raw/` (1,439,917 at scale 0.1; see docs/performance/PERFORMANCE.md)
- [ ] All datasets have declared JSON schemas
- [ ] Kafka producers emit events on all 5 topics
- [ ] Referential integrity holds across datasets (order → customer, line → product, etc.)
- [ ] Unit tests cover generator edge cases (nulls, boundary dates, special characters)

---

### Sprint 3 — Bronze Layer Ingestion (Weeks 5–6)

**Goal:** All 14 data sources land reliably in Bronze Delta tables with metadata enrichment, schema enforcement, and idempotent writes.

#### Epic 3.1 — Ingestion Framework

| Task | File / Component |
|---|---|
| `BaseIngestionJob` abstract class | `src/ingestion/batch/base_ingestion_job.py` |
| Watermark / state store client | `src/ingestion/batch/watermark_store.py` |
| Bronze Delta writer utility | `src/ingestion/batch/bronze_writer.py` |
| Prometheus metrics emitter | `src/common/utils/metrics_emitter.py` |

#### Epic 3.2 — Batch Ingestion Jobs

| Job | Source Format | File |
|---|---|---|
| Customers (CDC) | JSON | `src/ingestion/batch/customers_ingestion.py` |
| Orders (full) | JSON | `src/ingestion/batch/orders_ingestion.py` |
| Products | CSV | `src/ingestion/batch/products_ingestion.py` |
| Inventory (incremental) | JSON | `src/ingestion/batch/inventory_ingestion.py` |
| Payments | JSON | `src/ingestion/batch/payments_ingestion.py` |
| Shipments | JSON | `src/ingestion/batch/shipments_ingestion.py` |
| Returns | CSV | `src/ingestion/batch/returns_ingestion.py` |
| Campaigns | CSV | `src/ingestion/batch/campaigns_ingestion.py` |
| Reviews | JSON | `src/ingestion/batch/reviews_ingestion.py` |
| Suppliers | CSV | `src/ingestion/batch/suppliers_ingestion.py` |
| Employees | CSV | `src/ingestion/batch/employees_ingestion.py` |
| Finance | Parquet | `src/ingestion/batch/finance_ingestion.py` |

#### Epic 3.3 — Streaming Ingestion (Spark Structured Streaming)

| Job | Kafka Topic | File |
|---|---|---|
| Orders stream | `retailnexus.orders` | `spark/jobs/streaming/orders_stream.py` |
| Payments stream | `retailnexus.payments` | `spark/jobs/streaming/payments_stream.py` |
| Clickstream | `retailnexus.clickstream` | `spark/jobs/streaming/clickstream_stream.py` |
| Inventory stream | `retailnexus.inventory` | `spark/jobs/streaming/inventory_stream.py` |
| IoT sensors stream | `retailnexus.iot-sensors` | `spark/jobs/streaming/sensors_stream.py` |

#### Epic 3.4 — CDC Simulation

| Task | File |
|---|---|
| CDC generator (INSERT/UPDATE/DELETE envelopes) | `src/ingestion/cdc/cdc_generator.py` |
| CDC Bronze processor | `src/ingestion/cdc/cdc_processor.py` |

#### Epic 3.5 — File Watcher

| Task | File |
|---|---|
| S3 file watcher sensor | `src/ingestion/file_watcher/s3_watcher.py` |
| Airflow FileSensor wrapper | `airflow/plugins/sensors/s3_file_sensor.py` |

#### Sprint 3 Definition of Done

- [ ] All 14 batch sources write to Bronze Delta tables
- [ ] All 5 Kafka topics consumed into Bronze streaming tables
- [ ] Schema enforcement rejects malformed records (captured in dead-letter)
- [ ] Bronze metadata columns populated on every record
- [ ] Idempotent: re-running any ingestion job produces no duplicates
- [ ] Prometheus metrics emitted per job (records, bytes, duration, errors)
- [ ] Unit + integration tests for all ingestion jobs
- [ ] `make pipeline-ingest` runs end-to-end successfully

---

### Sprint 4 — Silver Layer Processing (Weeks 7–8)

**Goal:** All Bronze data transformed to Silver with type safety, deduplication, business rules, PII tokenization, and SCD Type 2 merges.

#### Epic 4.1 — Silver Processing Framework

| Task | File |
|---|---|
| `BaseSilverJob` abstract class | `src/processing/silver/base_silver_job.py` |
| Column normalizer | `src/processing/silver/normalizers.py` |
| Null handler (per-domain strategies) | `src/processing/silver/null_handler.py` |
| Type caster | `src/processing/silver/type_caster.py` |
| Deduplicator | `src/processing/silver/deduplicator.py` |
| PII tokenizer | `src/processing/silver/pii_tokenizer.py` |
| SCD2 merge handler | `src/processing/silver/scd2_merge.py` |
| Delta MERGE utility | `src/common/utils/delta_merge.py` |

#### Epic 4.2 — Domain Silver Jobs

One Silver job per domain, each applying the full cleanse + validate + SCD2 pipeline.

| Domain | Dedup Key | SCD2? | File |
|---|---|---|---|
| Customers | `customer_id` | Yes | `src/processing/silver/customers_silver.py` |
| Orders | `order_id` | No | `src/processing/silver/orders_silver.py` |
| Order Lines | `order_id + line_id` | No | `src/processing/silver/order_lines_silver.py` |
| Products | `sku` | Yes | `src/processing/silver/products_silver.py` |
| Inventory | `sku + location_id + snapshot_date` | No | `src/processing/silver/inventory_silver.py` |
| Payments | `payment_id` | No | `src/processing/silver/payments_silver.py` |
| Shipments | `shipment_id` | No | `src/processing/silver/shipments_silver.py` |
| Returns | `return_id` | No | `src/processing/silver/returns_silver.py` |
| Campaigns | `campaign_id` | Yes | `src/processing/silver/campaigns_silver.py` |
| Reviews | `review_id` | No | `src/processing/silver/reviews_silver.py` |
| Suppliers | `supplier_id` | Yes | `src/processing/silver/suppliers_silver.py` |
| Employees | `employee_id` | Yes | `src/processing/silver/employees_silver.py` |
| Finance | `transaction_id` | No | `src/processing/silver/finance_silver.py` |

#### Epic 4.3 — Schema Evolution Handling

| Task | File |
|---|---|
| Schema evolution detector | `src/processing/silver/schema_evolution.py` |
| Auto-merge for additive changes | Built into Delta writer |
| Breaking change alert + quarantine | `src/processing/silver/schema_quarantine.py` |

#### Sprint 4 Definition of Done

- [ ] All 13 Silver jobs pass end-to-end
- [ ] SCD2 history verified: changing a customer's segment creates new row, preserves old
- [ ] Deduplication: feeding the same Bronze data twice produces identical Silver output
- [ ] PII columns absent from Silver (email/address removed, tokens present)
- [ ] Business rule violations flagged with `_dq_flag` columns
- [ ] Test coverage > 90% for all Silver processing code

---

### Sprint 5 — Gold Layer Dimensional Models (Weeks 9–10)

**Goal:** Star schema Gold layer populated with 6 fact tables, 8 dimension tables, and pre-aggregated KPI summaries.

#### Epic 5.1 — Gold Processing Framework

| Task | File |
|---|---|
| `BaseGoldJob` abstract class | `src/processing/gold/base_gold_job.py` |
| Dimension loader (handle SCD2 surrogate keys) | `src/processing/gold/dimension_loader.py` |
| Fact builder (join dimensions, calc metrics) | `src/processing/gold/fact_builder.py` |

#### Epic 5.2 — Dimension Jobs

| Dimension | File |
|---|---|
| `dim_date` (static, pre-populated) | `spark/jobs/gold/dim_date.py` |
| `dim_customer` (from Silver customers SCD2) | `spark/jobs/gold/dim_customer.py` |
| `dim_product` (from Silver products SCD2) | `spark/jobs/gold/dim_product.py` |
| `dim_store` (from Silver stores SCD2) | `spark/jobs/gold/dim_store.py` |
| `dim_employee` (from Silver employees SCD2) | `spark/jobs/gold/dim_employee.py` |
| `dim_supplier` (from Silver suppliers SCD2) | `spark/jobs/gold/dim_supplier.py` |
| `dim_location` (Type 1) | `spark/jobs/gold/dim_location.py` |
| `dim_promotion` (from Silver campaigns) | `spark/jobs/gold/dim_promotion.py` |

#### Epic 5.3 — Fact Jobs

| Fact | File |
|---|---|
| `fact_sales` | `spark/jobs/gold/fact_sales.py` |
| `fact_inventory` | `spark/jobs/gold/fact_inventory.py` |
| `fact_marketing` | `spark/jobs/gold/fact_marketing.py` |
| `fact_returns` | `spark/jobs/gold/fact_returns.py` |
| `fact_shipments` | `spark/jobs/gold/fact_shipments.py` |
| `fact_finance` | `spark/jobs/gold/fact_finance.py` |

#### Epic 5.4 — Analytical Aggregates

| Aggregate | File |
|---|---|
| Daily sales summary | `spark/jobs/gold/agg_daily_sales.py` |
| Product ABC analysis | `spark/jobs/gold/agg_abc_inventory.py` |
| Customer RFM scores | `spark/jobs/gold/agg_rfm.py` |
| Marketing ROAS | `spark/jobs/gold/agg_marketing_roas.py` |

#### Sprint 5 Definition of Done

- [ ] All 8 dimensions populated with correct SCD2 history
- [ ] All 6 fact tables populated with correct dimensional surrogate keys
- [ ] `dim_date` covers 5 years (2022–2026)
- [ ] RFM query returns segmented customers with correct recency/frequency/monetary
- [ ] Gold Delta tables pass ACID transaction verification
- [ ] Time travel: previous day's Gold accessible via `VERSION AS OF`

---

### Sprint 6 — Data Warehouse Loads (Weeks 11–12)

**Goal:** Gold layer synchronized to Redshift, Snowflake, and BigQuery with full DDL, load scripts, and analytical SQL.

#### Epic 6.1 — Redshift

| Task | File |
|---|---|
| DDL for all fact + dimension tables | `warehouse/schemas/redshift/*.sql` |
| Distribution and sort key strategy | In DDL |
| COPY command scripts (from S3 Parquet) | `warehouse/schemas/redshift/load_scripts/*.sql` |
| WLM configuration | `warehouse/schemas/redshift/wlm_config.json` |

#### Epic 6.2 — Snowflake

| Task | File |
|---|---|
| DDL for all fact + dimension tables | `warehouse/schemas/snowflake/*.sql` |
| COPY INTO scripts (from S3) | `warehouse/schemas/snowflake/load_scripts/*.sql` |
| Row-level security policies | `warehouse/schemas/snowflake/rls_policies.sql` |

#### Epic 6.3 — BigQuery

| Task | File |
|---|---|
| Schema JSON definitions | `warehouse/schemas/bigquery/*.json` |
| Partitioning + clustering config | In schema JSON |
| Load scripts | `warehouse/schemas/bigquery/load_scripts/*.sh` |

#### Epic 6.4 — Analytical SQL Library

| Script | File |
|---|---|
| Daily / weekly / monthly sales analysis | `warehouse/models/analytics/sales_analysis.sql` |
| Customer segmentation (RFM) | `warehouse/models/analytics/rfm_segmentation.sql` |
| ABC inventory analysis | `warehouse/models/analytics/abc_inventory.sql` |
| Revenue & profitability | `warehouse/models/analytics/revenue_profitability.sql` |
| Top customers by CLV | `warehouse/models/analytics/clv_ranking.sql` |
| Inventory turnover | `warehouse/models/analytics/inventory_turnover.sql` |
| Marketing effectiveness | `warehouse/models/analytics/marketing_effectiveness.sql` |
| Repeat purchase rate | `warehouse/models/analytics/repeat_purchase.sql` |
| Daily KPI dashboard | `warehouse/models/analytics/daily_kpis.sql` |

#### Sprint 6 Definition of Done

- [ ] All tables exist in Redshift with correct DDL
- [ ] `COPY` loads from S3 complete in < 30 min for full dataset
- [ ] All analytical SQL scripts execute in < 60 seconds
- [ ] Snowflake and BigQuery DDL validated (schema parity with Redshift)
- [ ] Unit tests for all load scripts (row count assertions)

---

### Sprint 7 — Data Quality Framework (Weeks 13–14)

**Goal:** 80+ automated data quality checks running against every Silver and Gold table, with structured reports and Grafana dashboards.

#### Epic 7.1 — Quality Engine

| Task | File |
|---|---|
| `DataQualityCheck` abstract base | `src/quality/checks/base_check.py` |
| `CompletenessCheck` | `src/quality/checks/completeness.py` |
| `UniquenessCheck` | `src/quality/checks/uniqueness.py` |
| `ValidityCheck` (range, regex, enum) | `src/quality/checks/validity.py` |
| `ConsistencyCheck` (referential integrity) | `src/quality/checks/consistency.py` |
| `TimelinessCheck` (freshness) | `src/quality/checks/timeliness.py` |
| `QualityRunner` (orchestrate checks per table) | `src/quality/quality_runner.py` |
| Report writer (JSON → S3 + Prometheus) | `src/quality/reports/report_writer.py` |

#### Epic 7.2 — Domain Quality Configs

YAML configuration for each table's checks, thresholds, and severities.

| Domain | Config File |
|---|---|
| All Silver tables (13) | `configs/quality/silver/*.yml` |
| All Gold tables (14) | `configs/quality/gold/*.yml` |

#### Epic 7.3 — Data Contracts

Formal contracts between producers and consumers:

| Task | File |
|---|---|
| Contract schema definition | `src/quality/contracts/contract_schema.py` |
| Orders contract | `src/quality/contracts/orders_contract.yml` |
| Customers contract | `src/quality/contracts/customers_contract.yml` |
| Products contract | `src/quality/contracts/products_contract.yml` |
| Contract validation job | `src/quality/contracts/contract_validator.py` |

#### Sprint 7 Definition of Done

- [x] ≥ 80 quality checks defined and passing against generated data (528 defined; Silver and Gold pass on generated data)
- [ ] CRITICAL check failure halts pipeline (tested with injected bad data)
- [ ] Quality reports written to S3 and readable in Kibana
- [ ] Grafana quality dashboard shows scores per domain
- [ ] Data contracts validated on every Silver run

---

### Sprint 8 — Airflow Orchestration (Weeks 15–16)

**Goal:** All pipelines scheduled, monitored, and alerting through Airflow with proper dependency management, retry logic, and backfill support.

#### Epic 8.1 — Custom Operators & Hooks

| Task | File |
|---|---|
| `SparkSubmitDeltaOperator` | `airflow/plugins/operators/spark_delta_operator.py` |
| `DeltaMergeOperator` | `airflow/plugins/operators/delta_merge_operator.py` |
| `KafkaLagSensor` | `airflow/plugins/sensors/kafka_lag_sensor.py` |
| `S3DeltaTableSensor` | `airflow/plugins/sensors/delta_table_sensor.py` |
| `SlackAlertHook` | `airflow/plugins/hooks/slack_alert_hook.py` |

#### Epic 8.2 — DAG Implementations

| DAG | File |
|---|---|
| `bronze_batch_ingestion` | `airflow/dags/ingestion/bronze_batch_ingestion.py` |
| `silver_processing` | `airflow/dags/processing/silver_processing.py` |
| `gold_processing` | `airflow/dags/processing/gold_processing.py` |
| `warehouse_load` | `airflow/dags/warehouse/warehouse_load.py` |
| `quality_checks` | `airflow/dags/quality/quality_checks.py` |
| `quality_report` | `airflow/dags/quality/quality_report.py` |
| `stream_monitor` | `airflow/dags/ingestion/stream_monitor.py` |
| `backfill_coordinator` | `airflow/dags/ingestion/backfill_coordinator.py` |
| `compaction` | `airflow/dags/processing/compaction.py` |

#### Epic 8.3 — Airflow Configuration

| Task | File |
|---|---|
| Airflow connections (S3, Kafka, Postgres, Slack) | `configs/airflow/connections.yml` |
| Airflow variables (environment settings) | `configs/airflow/variables.yml` |
| Airflow RBAC roles | `configs/airflow/rbac.yml` |

#### Sprint 8 Definition of Done

- [ ] All 9 DAGs visible in Airflow UI, no import errors
- [ ] Full daily pipeline runs end-to-end without manual intervention
- [ ] SLA miss triggers Slack notification (tested by injecting delay)
- [ ] Failed task retries 3 times with exponential backoff
- [ ] Backfill DAG successfully re-processes 7 days of historical data
- [ ] Cross-DAG dependencies enforced via `ExternalTaskSensor`

---

### Sprint 9 — API, Monitoring & Logging (Weeks 17–18)

**Goal:** FastAPI service live, Prometheus + Grafana dashboards populated, ELK stack centralizing all logs.

#### Epic 9.1 — FastAPI Service

| Task | File |
|---|---|
| App factory + middleware | `api/main.py` |
| Database connection pool | `api/dependencies/db.py` |
| Auth middleware (API key + JWT) | `api/middleware/auth.py` |
| Rate limiting middleware | `api/middleware/rate_limit.py` |
| Sales router | `api/routers/sales.py` |
| Customers router | `api/routers/customers.py` |
| Inventory router | `api/routers/inventory.py` |
| Products router | `api/routers/products.py` |
| Revenue router | `api/routers/revenue.py` |
| Health router | `api/routers/health.py` |
| Service layer (business logic) | `api/services/*.py` |
| Repository layer (DB queries) | `api/repositories/*.py` |
| Pydantic response models | `api/models/*.py` |

#### Epic 9.2 — Prometheus Metrics

| Task | File |
|---|---|
| Prometheus config (scrape targets) | `configs/prometheus/prometheus.yml` |
| Alert rules | `monitoring/prometheus/rules/alert_rules.yml` |
| Alertmanager config | `monitoring/prometheus/alertmanager.yml` |
| PySpark metrics emitter | `src/common/utils/metrics_emitter.py` |
| Kafka JMX exporter config | `configs/kafka/jmx_exporter.yml` |

#### Epic 9.3 — Grafana Dashboards

| Dashboard | File |
|---|---|
| Pipeline Overview | `configs/grafana/dashboards/pipeline_overview.json` |
| Data Quality | `configs/grafana/dashboards/data_quality.json` |
| Kafka Health | `configs/grafana/dashboards/kafka_health.json` |
| Spark Jobs | `configs/grafana/dashboards/spark_jobs.json` |
| API Health | `configs/grafana/dashboards/api_health.json` |
| Infrastructure | `configs/grafana/dashboards/infrastructure.json` |

#### Epic 9.4 — ELK Stack

| Task | File |
|---|---|
| Logstash pipeline config | `monitoring/elk/logstash/pipeline.conf` |
| Kibana index patterns | `monitoring/elk/kibana/index_patterns.ndjson` |
| Kibana dashboards | `monitoring/elk/kibana/dashboards/*.ndjson` |
| Structured logger (JSON, all services) | `src/common/logging/logger.py` |

#### Sprint 9 Definition of Done

- [ ] All 8 API endpoints return 200 with correct schemas
- [ ] API Swagger UI accessible at `/docs` with full documentation
- [x] API p99 latency < 200 ms under 100 RPS load test (p99 26 ms, single worker, no headroom; docs/performance/PERFORMANCE.md)
- [ ] All 6 Grafana dashboards populated with live data
- [ ] PagerDuty/Slack alerts fire correctly for simulated failures
- [ ] Kibana shows structured logs from all services

---

### Sprint 10 — Testing, Hardening & Documentation (Weeks 19–20)

**Goal:** Test coverage > 90%, all documentation complete, production hardening applied, Terraform IaC written.

#### Epic 10.1 — Test Coverage Push

| Task | File |
|---|---|
| Unit tests: all ingestion jobs | `tests/unit/ingestion/` |
| Unit tests: all Silver processors | `tests/unit/processing/` |
| Unit tests: data quality checks | `tests/unit/quality/` |
| Unit tests: API routers + services | `tests/unit/api/` |
| Integration tests: Spark + Delta | `tests/integration/spark/` |
| Integration tests: Kafka consumers | `tests/integration/kafka/` |
| Pipeline tests: Bronze → Silver → Gold | `tests/pipeline/` |
| Coverage gating in CI (> 90%) | `.github/workflows/ci.yml` |

#### Epic 10.2 — Terraform IaC

| Task | File |
|---|---|
| S3 buckets module | `terraform/modules/s3/` |
| EMR cluster module | `terraform/modules/emr/` |
| Redshift cluster module | `terraform/modules/redshift/` |
| MSK (Kafka) module | `terraform/modules/kafka/` |
| VPC + security groups module | `terraform/modules/networking/` |
| IAM roles module | `terraform/modules/iam/` |
| Dev environment | `terraform/environments/dev/main.tf` |
| Prod environment | `terraform/environments/prod/main.tf` |

#### Epic 10.3 — Documentation Completion

| Document | File |
|---|---|
| Setup Guide | `docs/setup/SETUP_GUIDE.md` |
| Deployment Guide | `docs/deployment/DEPLOYMENT_GUIDE.md` |
| Troubleshooting Guide | `docs/troubleshooting/TROUBLESHOOTING.md` |
| Developer Guide | `docs/developer/DEVELOPER_GUIDE.md` |
| Data Dictionary (all 14 domains) | `docs/data-dictionary/DATA_DICTIONARY.md` |
| ER Diagram | `docs/diagrams/ER_DIAGRAM.md` |

#### Epic 10.4 — Bonus Features

| Feature | File | Sprint |
|---|---|---|
| Data Lineage tracking (OpenLineage) | `src/lineage/` | 10 |
| Metadata Catalog client | `src/catalog/` | 10 |
| Time Travel demo notebook | `notebooks/exploration/time_travel.ipynb` | 10 |
| Small file compaction job | `spark/jobs/maintenance/compaction.py` | 8 |
| Partition optimization report | `src/quality/reports/partition_report.py` | 10 |
| Cost estimation dashboard | `configs/grafana/dashboards/cost_estimation.json` | 10 |

#### Sprint 10 Definition of Done

- [ ] `make test-coverage` reports > 90% overall
- [ ] Zero open CRITICAL or HIGH bugs
- [ ] `terraform plan` for dev environment succeeds with no errors
- [ ] All documentation files written and cross-linked
- [ ] Data dictionary covers all columns in all Silver and Gold tables
- [ ] `make up && make generate-data && make pipeline-run` works end-to-end for a new developer
- [ ] GitHub Actions CI passes on clean checkout

---

## Timeline

```
Week  1- 2: Sprint 1  — Foundation
Week  3- 4: Sprint 2  — Synthetic Data
Week  5- 6: Sprint 3  — Bronze Ingestion       ← M3
Week  7- 8: Sprint 4  — Silver Processing      ← M4
Week  9-10: Sprint 5  — Gold Dimensional Model ← M5
Week 11-12: Sprint 6  — Warehouse Loads        ← M6
Week 13-14: Sprint 7  — Data Quality           ← M7
Week 15-16: Sprint 8  — Airflow Orchestration  ← M8
Week 17-18: Sprint 9  — API + Monitoring       ← M9
Week 19-20: Sprint 10 — Hardening + Docs       ← M10
```

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Docker resource constraints on dev machine | HIGH | MEDIUM | Tune compose memory limits; provide single-service start options |
| Spark job performance on generated data | MEDIUM | HIGH | Profile early; tune AQE and partition counts |
| Schema drift between generators and ingestion | MEDIUM | HIGH | Schema tests in CI; JSON schema validation before ingestion |
| Warehouse target unavailable (Redshift/Snowflake) | LOW | MEDIUM | All Gold development uses Delta/MinIO; warehouse load is last step |
| Delta Lake version incompatibility with Spark | LOW | HIGH | Pin exact versions in `pyproject.toml`; test matrix in CI |

---

*This roadmap is a living document. Update it at the start of each sprint to reflect any scope changes.*
