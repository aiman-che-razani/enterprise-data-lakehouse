# RetailNexus Data Lakehouse — Architecture Document

**Version:** 1.0.0  
**Authors:** Platform Engineering Team  
**Status:** Approved  
**Last Updated:** 2026-06

---

## Table of Contents

1. [Business Context](#1-business-context)
2. [Architecture Goals](#2-architecture-goals)
3. [Architecture Principles](#3-architecture-principles)
4. [System Overview](#4-system-overview)
5. [Data Flow Architecture](#5-data-flow-architecture)
6. [Medallion Layer Specification](#6-medallion-layer-specification)
7. [Ingestion Architecture](#7-ingestion-architecture)
8. [Processing Architecture](#8-processing-architecture)
9. [Storage Architecture](#9-storage-architecture)
10. [Orchestration Architecture](#10-orchestration-architecture)
11. [Streaming Architecture](#11-streaming-architecture)
12. [Warehouse Architecture](#12-warehouse-architecture)
13. [API Architecture](#13-api-architecture)
14. [Data Quality Architecture](#14-data-quality-architecture)
15. [Monitoring Architecture](#15-monitoring-architecture)
16. [Security Architecture](#16-security-architecture)
17. [Infrastructure Architecture](#17-infrastructure-architecture)
18. [Scalability and Performance](#18-scalability-and-performance)
19. [Disaster Recovery](#19-disaster-recovery)
20. [Technology Decision Records](#20-technology-decision-records)

---

## 1. Business Context

RetailNexus operates as a modern omnichannel retailer with:

- **Ecommerce platform** — online storefront, website clickstream, abandoned cart, recommendations
- **Physical stores** — 200+ locations, POS transactions, in-store analytics
- **Warehouse operations** — inventory management, IoT sensor telemetry, shipment tracking
- **Finance** — accounts payable/receivable, general ledger, FX exposure
- **CRM** — customer 360, loyalty programme, support tickets
- **Marketing** — campaign management, attribution, A/B testing
- **Supply chain** — supplier management, purchase orders, lead times

The business operates across three time zones and processes approximately **5 million events per day** across all channels.

### Business Drivers

| Driver | Requirement |
|---|---|
| Unified customer 360 | Single view of customer across all channels |
| Real-time inventory | Sub-minute inventory visibility across all DCs |
| Marketing attribution | Cross-channel attribution with < 1-hour freshness |
| Financial close | Automated daily P&L with 100% data completeness |
| ML / AI readiness | Feature store-ready, governed, versioned datasets |
| Regulatory compliance | GDPR, PCI-DSS, SOX data lineage and audit trail |

---

## 2. Architecture Goals

### Functional Goals

- Ingest data from all 14 operational domains reliably and at scale
- Support both batch (hourly/daily) and streaming (sub-second to sub-minute) workloads
- Enforce data quality automatically with clear failure escalation
- Serve curated data to BI tools, data science, ML pipelines, and APIs
- Maintain full data lineage from source to consumption

### Non-Functional Goals

| Goal | Target |
|---|---|
| Pipeline SLA | 99.5% on-time delivery |
| Data freshness (streaming) | < 5 minutes end-to-end |
| Data freshness (batch) | < 2 hours after source extraction |
| API latency (p50) | < 50 ms |
| API latency (p99) | < 200 ms |
| Storage efficiency | < $0.023 / GB / month (S3 Standard) |
| Recovery time objective | < 4 hours |
| Recovery point objective | < 1 hour |

---

## 3. Architecture Principles

1. **Immutability at Bronze** — raw data is never modified; all transformations produce new datasets
2. **Schema-first** — every dataset has a declared schema before data flows through it
3. **Quality gates** — data cannot advance to the next layer unless it passes defined quality checks
4. **Idempotency** — every pipeline job can be re-run safely without producing duplicate data
5. **Observability by default** — every component emits metrics, structured logs, and traces
6. **Fail fast, alert loud** — failures surface immediately; SLA breaches page on-call
7. **Least privilege** — every component has only the permissions it needs, nothing more
8. **Configuration over code** — environment-specific behaviour is driven by config, not code changes
9. **Separation of concerns** — ingestion, processing, quality, and serving are independently deployable
10. **Cost awareness** — storage tiering, partition pruning, and job sizing are first-class concerns

---

## 4. System Overview

The platform is divided into six horizontal concerns and three vertical data layers:

### Horizontal Concerns

```
┌─────────────────────────────────────────────────────────────────┐
│                    Orchestration (Airflow)                       │
├────────────┬────────────┬────────────┬────────────┬────────────┤
│ Ingestion  │ Processing │  Quality   │  Serving   │ Monitoring │
├────────────┴────────────┴────────────┴────────────┴────────────┤
│                      Storage (Delta Lake / S3)                  │
└─────────────────────────────────────────────────────────────────┘
```

### Vertical Data Layers

```
Source Systems  →  Bronze  →  Silver  →  Gold  →  Consumers
```

### Component Inventory

| Component | Technology | Purpose |
|---|---|---|
| Batch ingestion engine | PySpark | CSV/JSON/Parquet source → Bronze |
| Streaming ingestion | Kafka + Spark Streaming | Real-time source → Bronze |
| CDC engine | Debezium simulation + PySpark | DB change capture → Bronze |
| Bronze processor | PySpark | Schema enforcement, metadata tagging |
| Silver processor | PySpark | Cleanse, validate, deduplicate, SCD2 |
| Gold processor | PySpark | Aggregate, model, denormalize |
| Quality engine | Great Expectations + custom | 80+ automated checks |
| Orchestrator | Apache Airflow 2.9 | DAG scheduling and dependency mgmt |
| Object store | AWS S3 / MinIO (local) | Immutable data storage |
| Table format | Delta Lake | ACID transactions on object store |
| Metadata catalog | Custom + AWS Glue | Schema registry, data discovery |
| Lineage tracker | OpenLineage protocol | Source-to-consumption lineage |
| Warehouse connector | PySpark + JDBC | Gold → Redshift/Snowflake/BigQuery |
| API server | FastAPI | REST API over Gold layer |
| Metrics | Prometheus + Grafana | Pipeline and infrastructure monitoring |
| Logging | ELK Stack | Centralized structured log aggregation |
| Secrets | AWS Secrets Manager | Credential management |
| IaC | Terraform | AWS infrastructure provisioning |
| CI/CD | GitHub Actions | Automated test and deployment |

---

## 5. Data Flow Architecture

### Batch Data Flow

```
Source System
     │
     │  (1) Extract — Airflow schedules extraction at configured cadence
     ▼
Staging Area (S3 raw prefix or local FS)
     │
     │  (2) Ingest — PySpark reads source file, adds metadata columns
     ▼
Bronze Layer (Delta Lake)
  ├── _metadata.ingestion_timestamp
  ├── _metadata.source_system
  ├── _metadata.source_file_path
  ├── _metadata.pipeline_run_id
  └── [original columns untouched]
     │
     │  (3) Validate Bronze — schema check, not-null on key fields
     ▼
Silver Layer (Delta Lake)
  ├── Columns renamed to snake_case standard
  ├── Types cast to canonical types
  ├── Nulls handled (imputed / dropped / flagged)
  ├── Duplicates removed (deterministic key)
  ├── Business rules applied
  ├── PII tokenized
  └── SCD2 merge for slowly changing dimensions
     │
     │  (4) Quality gate — 80+ checks; halt pipeline on CRITICAL failure
     ▼
Gold Layer (Delta Lake)
  ├── Star schema fact and dimension tables
  ├── Pre-aggregated summary tables
  └── Feature store exports
     │
     │  (5) Load — COPY / INSERT-SELECT to warehouse targets
     ▼
Redshift · Snowflake · BigQuery
     │
     ▼
BI Tools · FastAPI · ML Pipelines
```

### Streaming Data Flow

```
Event Producer
     │  Kafka topic (orders, payments, clickstream, inventory, sensors)
     ▼
Kafka Broker Cluster
     │
     │  Spark Structured Streaming (micro-batch 30s)
     ▼
Stream Processor
  ├── Schema validation (Avro / JSON schema registry)
  ├── Watermarking for late data (up to 10 min)
  ├── Stateful aggregations (session windows)
  └── Exactly-once guarantees (Delta Lake ACID)
     │
     ▼
Bronze Streaming Table (append-only Delta)
     │
     │  Triggered Silver job (every 5 min)
     ▼
Silver Streaming Table
     │
     │  Near-real-time Gold aggregation
     ▼
Gold Streaming Summary
     │
     ▼
API · Dashboards
```

---

## 6. Medallion Layer Specification

### Bronze Layer

| Property | Value |
|---|---|
| S3 prefix | `s3://retailnexus-lakehouse/bronze/` |
| Table format | Delta Lake |
| Write mode | Append-only |
| Schema enforcement | Strict (reject records that don't match declared schema) |
| Partitioning | `_ingestion_year` / `_ingestion_month` / `_ingestion_day` |
| Compaction | Weekly OPTIMIZE + VACUUM |
| Retention | 7 years (regulatory minimum) |
| Access | Platform engineers, data scientists (read-only) |

**System columns appended at Bronze:**

| Column | Type | Description |
|---|---|---|
| `_ingested_at` | TIMESTAMP | UTC timestamp when record landed in Bronze |
| `_source_system` | STRING | Source system identifier |
| `_source_file` | STRING | Source file path or Kafka topic/partition/offset |
| `_pipeline_run_id` | STRING | Airflow DAG run ID |
| `_ingestion_year` | INT | Partition year |
| `_ingestion_month` | INT | Partition month |
| `_ingestion_day` | INT | Partition day |
| `_schema_version` | STRING | Schema version at time of ingestion |

### Silver Layer

| Property | Value |
|---|---|
| S3 prefix | `s3://retailnexus-lakehouse/silver/` |
| Table format | Delta Lake |
| Write mode | MERGE (upsert) for dimensions, APPEND for facts |
| Schema enforcement | Strict |
| Partitioning | Domain-specific (typically by date or category) |
| Compaction | Daily OPTIMIZE |
| Retention | 3 years online, archive to Glacier after |
| Access | Analysts, data scientists, downstream teams (read-only) |

**Transformations applied at Silver:**

1. Column name normalization (snake_case, max 64 chars)
2. Type casting (string dates → TIMESTAMP, numeric strings → DOUBLE)
3. Null handling strategy (domain-specific: impute / drop / flag)
4. Deduplication (deterministic composite key, latest-wins within partition)
5. Business rule validation (reject/flag violating records)
6. PII tokenization (customer_id → UUID token, email → SHA-256 hash)
7. SCD Type 2 merge for dimension tables
8. Referential integrity checks (FK lookup against Silver reference tables)

### Gold Layer

| Property | Value |
|---|---|
| S3 prefix | `s3://retailnexus-lakehouse/gold/` |
| Table format | Delta Lake |
| Write mode | OVERWRITE (daily snapshots) or MERGE (incremental) |
| Schema enforcement | Strict |
| Partitioning | By business date |
| Compaction | On write |
| Retention | 5 years online |
| Access | BI tools, API, ML pipelines (read-only) |

---

## 7. Ingestion Architecture

### Batch Ingestion

The batch ingestion framework is built on PySpark and follows a common abstract base with domain-specific implementations:

```
BatchIngestionBase
├── CsvIngestionJob        ── configs/ingestion/csv_sources.yml
├── JsonIngestionJob       ── configs/ingestion/json_sources.yml
├── ParquetIngestionJob    ── configs/ingestion/parquet_sources.yml
├── RestApiIngestionJob    ── configs/ingestion/api_sources.yml
└── CdcIngestionJob        ── configs/ingestion/cdc_sources.yml
```

Each job:
1. Reads source configuration (schema, watermark column, partition key)
2. Extracts data since last watermark (incremental) or full table
3. Appends Bronze metadata columns
4. Writes to Bronze Delta table with schema merge enabled
5. Updates watermark in the pipeline state store (PostgreSQL)
6. Emits Prometheus metrics (records ingested, bytes, duration, errors)

### Streaming Ingestion

Kafka topics flow through Spark Structured Streaming:

```
Kafka Topic → SparkStreamingReader → Bronze Delta Table (streaming)
```

Configuration per topic:

| Topic | Consumer Group | Starting Offset | Checkpoint | Trigger |
|---|---|---|---|---|
| `retailnexus.orders` | `lakehouse-orders` | latest | `s3://checkpoint/orders` | 30s |
| `retailnexus.payments` | `lakehouse-payments` | latest | `s3://checkpoint/payments` | 30s |
| `retailnexus.clickstream` | `lakehouse-clickstream` | latest | `s3://checkpoint/clickstream` | 10s |
| `retailnexus.inventory` | `lakehouse-inventory` | latest | `s3://checkpoint/inventory` | 30s |
| `retailnexus.iot-sensors` | `lakehouse-sensors` | latest | `s3://checkpoint/sensors` | 10s |

### CDC Simulation

Simulates Debezium-style CDC output:

- Source: PostgreSQL operational databases
- Change types: INSERT, UPDATE, DELETE
- Output format: JSON envelope `{op, before, after, ts_ms, source}`
- Processing: Silver MERGE handles upserts and soft deletes

### File Watcher

An Airflow sensor polls an S3 prefix for new file arrivals:
- Configured per source with glob patterns
- Triggers the corresponding ingestion DAG on arrival
- Supports late file handling (configurable wait window)

### Schema Evolution

Delta Lake `mergeSchema = true` handles additive schema changes automatically.  
Breaking changes (column removal, type narrowing) are detected at ingestion time and routed to a schema evolution review queue.

---

## 8. Processing Architecture

### Spark Cluster Configuration

| Environment | Cluster Type | Driver | Workers | Notes |
|---|---|---|---|---|
| Local | Docker Spark standalone | 2 CPU / 4 GB | 1 × 2 CPU / 4 GB | Development only |
| Dev | EMR on-demand | r6g.xlarge | 2 × r6g.xlarge | Auto-terminate |
| Staging | EMR managed scaling | r6g.2xlarge | 2–8 × r6g.2xlarge | Scale on backlog |
| Prod | EMR managed scaling | r6g.4xlarge | 4–20 × r6g.4xlarge | Spot + On-demand |

### PySpark Job Design

All jobs inherit from a common `BaseSparkJob`:

```python
class BaseSparkJob:
    def __init__(self, config: JobConfig, spark: SparkSession): ...
    def run(self) -> JobResult: ...          # Template method
    def read(self) -> DataFrame: ...         # Abstract
    def transform(self, df: DataFrame) -> DataFrame: ...  # Abstract
    def write(self, df: DataFrame) -> None: ...           # Abstract
    def emit_metrics(self, result: JobResult) -> None: ...
```

### Optimization Techniques Applied

| Technique | When Used |
|---|---|
| Broadcast joins | Dimension tables < 256 MB |
| Partition pruning | All reads use partition filter pushdown |
| Predicate pushdown | Delta Lake stats used for file skipping |
| Caching | Reused DataFrames within a job |
| Adaptive Query Execution | Enabled globally (AQE) |
| Dynamic partition overwrite | Incremental Gold writes |
| Checkpointing | Streaming jobs every 5 minutes |
| Small file compaction | Weekly OPTIMIZE on all Delta tables |
| Z-ordering | High-cardinality filter columns |

---

## 9. Storage Architecture

### S3 Bucket Layout

```
s3://retailnexus-lakehouse/
├── bronze/
│   ├── customers/
│   ├── orders/
│   ├── products/
│   └── ...
├── silver/
│   ├── customers/
│   ├── orders/
│   └── ...
├── gold/
│   ├── fact_sales/
│   ├── fact_inventory/
│   ├── dim_customer/
│   └── ...
├── quality-reports/
│   ├── {date}/
│   └── ...
├── checkpoints/
│   ├── orders_stream/
│   └── ...
└── pipeline-state/
    └── watermarks.db
```

### Delta Lake Features Used

| Feature | Usage |
|---|---|
| ACID transactions | All writes — prevents partial writes |
| Time travel | Audit, rollback, reproducibility |
| Schema enforcement | Reject records that don't match declared schema |
| Schema evolution | Additive changes auto-merged |
| OPTIMIZE / Z-ORDER | Weekly compaction for query performance |
| VACUUM | 7-day file retention after VACUUM |
| Change Data Feed | Streaming Silver → Gold incremental processing |
| Generated columns | Partition columns derived from event timestamp |

---

## 10. Orchestration Architecture

### Airflow Configuration

- **Deployment:** Docker Compose (local) / AWS MWAA (prod)
- **Executor:** LocalExecutor (dev) / CeleryExecutor (prod)
- **Metadata DB:** PostgreSQL
- **RBAC:** Enabled with role-based UI access

### DAG Inventory

Times are UTC. The daily chain is time-scheduled with a **quality gate in front of each downstream layer**
(see TDR-005), so a late or failing upstream run blocks the next layer instead of feeding it bad data.

| DAG ID | Schedule | Description |
|---|---|---|
| `bronze_batch_ingestion` | `0 2 * * *` | Ingest all 16 domains from raw files into Bronze Delta |
| `bronze_streaming_health` | `*/5 * * * *` | Health + freshness monitor for the 5 Kafka → Bronze streaming consumers |
| `silver_batch` | `30 3 * * *` | Bronze → Silver: dedup, cleanse, validate, PII tokenisation, SCD2, data-contract check |
| `gold_batch` | `0 5 * * *` | Silver → Gold star schema. Starts with `silver_dq_gate` (`run_dq --layer silver --fail-fast`) |
| `warehouse_load` | `30 7 * * *` | Gold → Redshift / Snowflake / BigQuery. Starts with `gold_dq_gate` (`run_dq --layer gold --fail-fast`) |
| `data_quality` | `0 10 * * *` | Full three-layer DQ run and report (the *reporting* DAG; the gates above are the *blocking* ones) |
| `sla_monitor` | `*/30 * * * *` | Freshness, DQ pass-rate and SLA-miss checks |
| `pipeline_e2e` | manual | Runs Bronze → Silver → Gold → warehouses → DQ in one DAG run |
| `pipeline_backfill` | manual | Historical backfill for a layer / domain / date range |
| `pipeline_maintenance` | `0 1 * * 0` | Weekly OPTIMIZE + VACUUM (Bronze keeps its 7-year window), catalog sync, Postgres VACUUM, watermark audit |

### DAG Design Patterns

- **Quality gates:** `gold_batch` and `warehouse_load` begin with a blocking DQ task using `--fail-fast`;
  a CRITICAL failure (exit code 3) fails the gate and every downstream task. `AlertReporter` only raises when
  `fail_on_critical` is set, which is why the gates pass it explicitly.
- **Failure handling:** `retries` of 1–2 with a fixed delay on the batch DAGs (0 on manual backfills, which
  must not silently re-run). SLA misses are recorded in `pipeline.sla_misses` and alerted by Prometheus.
- **Dependencies:** time-based scheduling plus the quality gates. `WatermarkSensor` and `DQPassRateSensor`
  (`airflow/plugins/sensors/`) are available for cross-DAG waits where a fixed schedule is not enough.
- **Backfill:** `catchup=False` everywhere; historical loads go through `pipeline_backfill`, not scheduler catch-up.
- **Idempotency:** Delta MERGE (facts, Silver) and overwrite (dimensions) make every task safe to retry;
  Silver and Gold therefore *fail* on a checkpoint-write error rather than continuing with a stale one.

---

## 11. Streaming Architecture

### Kafka Cluster

| Property | Value |
|---|---|
| Broker count | 3 (prod), 1 (local) |
| Replication factor | 3 (prod), 1 (local) |
| Retention | 7 days |
| Compression | LZ4 |
| Schema registry | Confluent Schema Registry |

### Spark Structured Streaming

```
Kafka Source → Watermark → Transform → Delta Sink
                 │
                 └── Late data tolerance: 10 minutes
                     Output mode: append (facts) / update (aggregates)
                     Checkpointing: S3-backed, every 5 minutes
                     Fault tolerance: exactly-once via Delta idempotency
```

### Topic Schemas

| Topic | Key | Value Schema |
|---|---|---|
| `retailnexus.orders` | `order_id` | Avro — OrderEvent |
| `retailnexus.payments` | `payment_id` | Avro — PaymentEvent |
| `retailnexus.clickstream` | `session_id` | JSON — ClickEvent |
| `retailnexus.inventory` | `sku_id` | Avro — InventoryEvent |
| `retailnexus.iot-sensors` | `sensor_id` | JSON — SensorReading |

---

## 12. Warehouse Architecture

### Dimensional Model

Star schema with conformed dimensions shared across all fact tables.

**Fact Tables:**

| Table | Grain | Partitioned By | Approx. Size |
|---|---|---|---|
| `fact_sales` | 1 row per order line | `order_date` | 200 GB/year |
| `fact_inventory` | 1 row per SKU per location per day | `snapshot_date` | 50 GB/year |
| `fact_marketing` | 1 row per campaign touch | `event_date` | 30 GB/year |
| `fact_returns` | 1 row per return line | `return_date` | 20 GB/year |
| `fact_shipments` | 1 row per shipment event | `event_date` | 40 GB/year |
| `fact_finance` | 1 row per GL line | `posting_date` | 100 GB/year |

**Dimension Tables (SCD Type 2 where noted):**

| Table | Type | Description |
|---|---|---|
| `dim_customer` | SCD2 | Customer demographics, segment, loyalty tier |
| `dim_date` | Static | Full calendar with fiscal calendar overlay |
| `dim_store` | SCD2 | Store attributes, region, format |
| `dim_employee` | SCD2 | Employee hierarchy, department |
| `dim_supplier` | SCD2 | Supplier attributes, tier |
| `dim_location` | Type 1 | Geographic hierarchy |
| `dim_product` | SCD2 | Product hierarchy, pricing, attributes |
| `dim_promotion` | Type 1 | Promotion catalogue |

### Load Strategy

Gold → Warehouse using `COPY` (Redshift), `COPY INTO` (Snowflake), `LOAD DATA` (BigQuery).  
Daily full refresh for dimensions (with SCD2 history preserved in Delta).  
Incremental append for facts using high-watermark on `created_at`.

---

## 13. API Architecture

### FastAPI Service

```
┌────────────────────────────────────────────────┐
│                  FastAPI App                    │
├────────────────────────────────────────────────┤
│  Middleware: Auth · Rate Limit · Logging · CORS │
├──────────┬───────────┬────────────┬────────────┤
│  /sales  │ /customers│ /inventory │  /products  │
├──────────┴───────────┴────────────┴────────────┤
│               Service Layer                     │
├────────────────────────────────────────────────┤
│          Repository Layer (async SQLAlchemy)    │
├────────────────────────────────────────────────┤
│  Redshift / PostgreSQL (Gold layer replica)     │
└────────────────────────────────────────────────┘
```

### Authentication

- API key authentication for external consumers
- JWT for internal service-to-service
- Scopes: `read:sales`, `read:customers`, `read:inventory`, `admin`

---

## 14. Data Quality Architecture

### Quality Check Taxonomy

```
DataQualityCheck (abstract)
├── CompletenessCheck
│   ├── NullRateCheck          -- Fail if null rate > threshold
│   └── RequiredFieldCheck     -- Fail if required column has any null
├── UniquenessCheck
│   └── DuplicateKeyCheck      -- Fail if composite key has duplicates
├── ValidityCheck
│   ├── RangeCheck             -- Numeric/date within expected range
│   ├── RegexCheck             -- String matches pattern
│   └── CategoryCheck          -- Value in allowed set
├── ConsistencyCheck
│   ├── ReferentialIntegrityCheck  -- FK exists in reference table
│   └── CrossTableCheck            -- Derived column matches computation
└── TimelinessCheck
    └── FreshnessCheck         -- Table updated within SLA window
```

### Quality Report Structure

```json
{
  "run_id": "dq_20260628_orders",
  "table": "silver.orders",
  "timestamp": "2026-06-28T06:00:00Z",
  "summary": {
    "total_checks": 24,
    "passed": 22,
    "failed": 1,
    "warnings": 1,
    "overall_score": 91.7
  },
  "checks": [
    {
      "name": "null_rate.order_id",
      "status": "PASSED",
      "metric": 0.0,
      "threshold": 0.0,
      "severity": "CRITICAL"
    }
  ]
}
```

### Severity Levels

| Severity | Action |
|---|---|
| CRITICAL | Halt pipeline, page on-call, block downstream |
| HIGH | Alert, continue pipeline, flag records in Silver |
| MEDIUM | Log warning, continue pipeline |
| INFO | Log only |

---

## 15. Monitoring Architecture

### Metrics Collection

All components expose Prometheus metrics:

- **PySpark jobs:** Custom `PrometheusMetricsEmitter` class writes to Pushgateway
- **FastAPI:** `prometheus-fastapi-instrumentator` auto-instruments endpoints
- **Airflow:** StatsD → Prometheus via statsd_exporter
- **Kafka:** Kafka JMX → Prometheus via jmx_exporter

### Grafana Dashboards

| Dashboard | Description |
|---|---|
| Pipeline Overview | Job success rate, duration, record counts, SLA status |
| Data Quality | Quality scores per domain, trend, failure history |
| Kafka Health | Consumer lag, throughput, partition distribution |
| Spark Jobs | Stage duration, shuffle, GC pressure, task failures |
| API Health | RPS, latency p50/p95/p99, error rate, status codes |
| Infrastructure | CPU, memory, disk, network per host |
| Cost | S3 storage costs, EMR cluster hours, API calls |

### Alert Rules

| Alert | Condition | Severity | Routing |
|---|---|---|---|
| `PipelineSLAMiss` | DAG > SLA | CRITICAL | PagerDuty |
| `KafkaConsumerLag` | Lag > 100K | HIGH | Slack |
| `DataQualityCritical` | CRITICAL check failed | CRITICAL | PagerDuty |
| `SparkJobFailed` | Exit code != 0 | HIGH | Slack |
| `APIHighErrorRate` | Error rate > 1% | HIGH | Slack |
| `StorageUsageHigh` | > 80% quota | MEDIUM | Slack |

---

## 16. Security Architecture

### Data Classification

| Tier | Examples | Controls |
|---|---|---|
| PII | Customer name, email, address | Tokenized in Silver, encrypted at rest |
| PCI | Card BIN, payment method type | Masked, scope-restricted access |
| Confidential | Revenue, margins, employee data | Role-based access, audit logging |
| Internal | Product catalogue, store list | Standard access |
| Public | Marketing content | No restrictions |

### Encryption

- **At rest:** S3 SSE-S3 (AES-256) for all buckets; Delta Lake encrypted at storage layer
- **In transit:** TLS 1.3 for all API calls, Kafka inter-broker and client, Airflow web
- **Secrets:** AWS Secrets Manager for all credentials; never in code or environment variables directly

### IAM Design

| Role | Permissions |
|---|---|
| `lakehouse-ingestion-role` | S3 write (bronze/*), Secrets read, Glue table create |
| `lakehouse-processing-role` | S3 read/write (silver/*, gold/*), Glue table update |
| `lakehouse-api-role` | Redshift read, S3 read (gold/*) |
| `lakehouse-monitoring-role` | CloudWatch read, S3 read (quality-reports/*) |

---

## 17. Infrastructure Architecture

### AWS Services Used

| Service | Purpose |
|---|---|
| S3 | Primary lakehouse storage |
| EMR | Spark cluster for batch jobs |
| MSK | Managed Kafka |
| Redshift | Primary analytics warehouse |
| RDS PostgreSQL | Airflow metadata, pipeline state store |
| Secrets Manager | Credential management |
| CloudWatch | Infrastructure monitoring |
| Glue | Data catalog (schema registry) |
| IAM | Identity and access management |
| VPC | Network isolation |
| CloudTrail | Audit logging |

### Local Development Stack (Docker Compose)

| Service | Port | Notes |
|---|---|---|
| Spark Master | 4040, 7077 | Single-node standalone |
| Spark Worker | — | 1 worker, 2 CPU / 4 GB |
| Kafka | 9092 | Single broker |
| Zookeeper | 2181 | Required by Kafka |
| Airflow Webserver | 8080 | |
| Airflow Scheduler | — | Shares Postgres with webserver |
| PostgreSQL | 5432 | Airflow metadata + app DB |
| MinIO | 9000, 9001 | S3-compatible object store |
| FastAPI | 8000 | |
| Prometheus | 9090 | |
| Grafana | 3000 | |
| Elasticsearch | 9200 | |
| Logstash | 5044 | |
| Kibana | 5601 | |

---

## 18. Scalability and Performance

### Horizontal Scaling

- **Spark:** EMR managed scaling adds/removes workers based on YARN pending containers
- **Kafka:** Partition rebalancing when adding brokers; producers auto-detect
- **Airflow:** CeleryExecutor scales workers independently of scheduler
- **API:** Horizontal pod autoscaling behind an ALB (ECS Fargate in prod)

### Vertical Partitioning

- Bronze: `year/month/day` (time-based, aligned with Airflow backfill windows)
- Silver: domain-specific (e.g., `orders` partitioned by `order_date`)
- Gold facts: `business_date`
- Gold dimensions: not partitioned (small tables, full read typical)

### Performance Targets

| Workload | Volume | Target Duration |
|---|---|---|
| Daily Bronze ingest (all 14 sources) | ~5 M records | < 30 min |
| Daily Silver transform | ~5 M records | < 45 min |
| Daily Gold build | ~5 M records | < 30 min |
| Warehouse load (Redshift) | ~100 GB | < 30 min |
| Streaming latency | Per event | < 5 min |
| API response | Per request | < 200 ms p99 |

---

## 19. Disaster Recovery

### Recovery Scenarios

| Scenario | Detection | Recovery Procedure | RTO | RPO |
|---|---|---|---|---|
| Corrupt Silver table | Quality check fails | Time travel to last-known-good snapshot | 30 min | 1 hour |
| Failed Airflow scheduler | Alert | Restart ECS task; DAGs resume from last checkpoint | 5 min | 0 |
| Kafka broker failure | Broker health alert | MSK auto-recovers with replication factor 3 | 5 min | 0 |
| S3 data deletion | Audit log alert | Restore from S3 Versioning | 1 hour | 0 |
| Redshift cluster failure | CloudWatch | Restore from automated snapshot | 2 hours | 1 hour |
| Full region failure | Monitoring | Cross-region S3 replication → secondary EMR | 4 hours | 1 hour |

### Backup Strategy

- **S3:** Versioning enabled on all buckets; cross-region replication to `us-west-2`
- **Delta Lake:** Time travel retains 30 days of history (VACUUM threshold)
- **PostgreSQL:** Automated RDS snapshots every 1 hour, retained 35 days
- **Redshift:** Automated snapshots every 8 hours, retained 35 days

---

## 20. Technology Decision Records

### TDR-001: Delta Lake over Apache Iceberg

**Decision:** Use Delta Lake as the table format.  
**Rationale:** Databricks-native, mature Python/PySpark API, superior MERGE performance at the volumes we expect, extensive production track record at comparable scale. Iceberg is a strong alternative but the team's expertise and tooling already favour Delta.  
**Trade-offs:** Vendor proximity to Databricks; Iceberg has better multi-engine support. Revisit in 18 months.

### TDR-002: Airflow over Prefect / Dagster

**Decision:** Use Apache Airflow for orchestration.  
**Rationale:** Largest ecosystem, managed offering (MWAA) on AWS, team familiarity, industry standard for this stack profile.  
**Trade-offs:** Airflow dynamic DAGs are more verbose than Prefect/Dagster. The operational overhead is justified by MWAA managed service.

### TDR-003: Multiple Warehouse Targets

**Decision:** Load Gold into Redshift, Snowflake, and BigQuery.  
**Rationale:** Different consumer groups have existing tooling on each warehouse. Redshift for BI, Snowflake for self-service, BigQuery for data science / ML (native integration with Vertex AI).  
**Trade-offs:** 3× load jobs and 3× schema maintenance. Mitigated by shared Gold Delta tables as the source of truth.

### TDR-004: PySpark over dbt for Transformations

**Decision:** Use PySpark for Bronze→Silver→Gold transformations.  
**Rationale:** Volume (5 M events/day) and complexity (streaming, CDC, ML feature engineering) exceed what SQL-centric dbt handles cleanly. PySpark gives full programmatic control.  
**Trade-offs:** dbt would be faster to write and easier to audit for SQL-fluent analysts. Consider dbt for Gold→Warehouse SQL transformations in a future iteration.


### TDR-005: Blocking quality gates, enforced data contracts, and retention as code

**Decision:**
1. The daily chain keeps its fixed schedule (Bronze 02:00 → Silver 03:30 → Gold 05:00 → warehouses 07:30) but each
   downstream DAG opens with a blocking quality gate: `silver_dq_gate` before Gold, `gold_dq_gate` before the
   warehouse loads. The `data_quality` DAG (10:00) remains the reporting run.
2. Silver publishes a versioned data contract per domain (`src/quality/contracts`), checked on every run.
   `DATA_CONTRACT_MODE` defaults to `enforce` in staging/prod and `warn` in dev (`Settings.contract_mode`);
   `off` is rejected in production.
3. Retention is enforced in code, not by convention: Bronze `VACUUM` is refused below 7 years
   (`spark/jobs/maintenance/delta_maintenance.py`), matching `delta.deletedFileRetentionDuration` set at ingest and
   the S3 lifecycle rule; Silver/Gold keep 72 hours.

**Rationale:** The original design said data must not advance unless it passes checks, but the only DQ run was
scheduled after Gold and the warehouse loads and could not halt them. Gates make the principle true; contracts move
schema/PII guarantees from documentation into a check that fails the run.
**Trade-offs:** Two extra Spark submissions per day and a failed gate delays the whole chain until someone acts.
Mitigated by the per-domain alerts and runbooks. Time-based scheduling still lets a *late* upstream run be read as
"no new data" — sensors are available if that becomes a problem. The gate is a *critical-failure* gate:
`run_dq --fail-fast` exits 3 on a critical check (row count, uniqueness) and 0 otherwise, so a sparse-but-expected
column cannot hold Gold back; those are alerted on and stored in `quality.reports`.


### TDR-006: Business-month partitioning for Gold facts, file-level idempotent Bronze ingest, hashed names

Found by running the whole chain on real infrastructure rather than by review.

**Decision:**
1. **Gold facts partition by business month**, not by build day. A Delta-only column `_date_month` (yyyymm, derived
   from `sk_date_id`) replaces `_gold_date` as the partition column; `_gold_date` stays as an ordinary column meaning
   "the day this row first landed" and is never rewritten by a MERGE. Facts whose business date cannot change
   (sales, inventory, marketing, returns, finance) also match the MERGE on the partition, so it touches only the
   months present in the batch; `fact_shipments` does not, because a shipment's date moves from "created" to
   "shipped". A table still partitioned by `_gold_date` is rewritten once, automatically, on its next build.
   The warehouse loaders drop `_date_month`.
2. **Bronze ingest is idempotent per source file.** Every run reads the whole raw folder, so without this each daily
   run appended all of history again (a second day doubled every Bronze table). Rows from files already present in
   Bronze's `_source_file` are skipped; raw files are treated as immutable and new data arrives as a new file (the
   Auto Loader contract). A re-delivered file under the same name is deliberately not re-ingested.
3. **Customer and employee names are keyed hashes from Silver onward** (`first_name_hash`, `last_name_hash`, same
   mechanism as email). Bronze keeps the raw values (it is the immutable landing zone); Gold `dim_employee` carries
   the hashes, so the dimension supports joins and de-duplication but not display of names.
4. **DQ configs state reality.** `DomainCheckConfig.max_null_rates` records per-column expected sparsity (an
   undelivered shipment has no transit days; web orders have no store), freshness measures when the pipeline last
   wrote a layer (`_ingested_at` / `_silver_processed_at` / `_gold_processed_at`) rather than an event date, and a
   test fails if a config names a column that does not exist (such checks used to be skipped silently).

**Rationale:** Date-range queries could not prune on the processing-day partition and every MERGE scanned the whole
table; the daily re-ingest was an unbounded cost; and a permanently red quality dashboard teaches people to ignore it.
**Trade-offs:** Monthly partitions are small at dev volumes (about 30 small files per fact table at 1 % scale; OPTIMIZE
compacts within a partition). Name hashing removes a display capability that HR / support views would need and would
have to be served from a governed source instead. File-level idempotency cannot pick up an in-place edit of an
existing raw file.
