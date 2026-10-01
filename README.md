# Enterprise Data Lakehouse Platform

![CI](https://github.com/your-org/enterprise-data-lakehouse/actions/workflows/ci.yml/badge.svg)
![CD Staging](https://github.com/your-org/enterprise-data-lakehouse/actions/workflows/cd-staging.yml/badge.svg)
![Coverage](https://img.shields.io/badge/coverage-≥90%25-brightgreen)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Spark](https://img.shields.io/badge/spark-3.5.3-orange)
![Delta Lake](https://img.shields.io/badge/delta--lake-3.2.0-blue)
![License](https://img.shields.io/badge/license-MIT-green)

> Production-grade, medallion-architecture data lakehouse for a multi-channel retail enterprise.  
> Ingests 1 M+ records across 14 operational domains, processes them through Bronze → Silver → Gold,  
> and serves curated data to analytics, ML, and downstream applications in near-real time.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Quick Start](#quick-start)
- [Data Domains](#data-domains)
- [Medallion Layers](#medallion-layers)
- [Ingestion Patterns](#ingestion-patterns)
- [Data Quality](#data-quality)
- [Analytics & Warehouse](#analytics--warehouse)
- [API](#api)
- [Monitoring & Observability](#monitoring--observability)
- [CI/CD](#cicd)
- [Security](#security)
- [Development Guide](#development-guide)
- [Roadmap](#roadmap)
- [Contributing](#contributing)

---

## Overview

RetailNexus Data Lakehouse is an enterprise-grade data platform built on the **Medallion Architecture** (Bronze → Silver → Gold).  
It consolidates data from every corner of a modern omnichannel retailer — ecommerce, physical stores, warehouses, finance, CRM, marketing, inventory, and customer support — into a single governed lakehouse that serves as the single source of truth for all analytics, reporting, and ML workloads.

| Metric | Value |
|---|---|
| Daily ingestion volume | ~5 M events / day |
| Batch datasets | 14 operational domains |
| Streaming topics | 5 Kafka topics |
| Data quality checks | 80+ automated checks |
| Warehouse targets | Redshift · Snowflake · BigQuery |
| API latency (p99) | < 200 ms |
| Pipeline SLA | 99.5% on-time delivery |

---

## Architecture

See [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) for the full design document.  
See [docs/diagrams/DIAGRAMS.md](docs/diagrams/DIAGRAMS.md) for all Mermaid diagrams.

### High-Level Flow

```
Operational Systems
        │
        ▼
┌───────────────────────────────────────────────────────┐
│                  Ingestion Layer                       │
│  Batch (CSV/JSON/Parquet)  │  Kafka Streaming  │ CDC  │
└─────────────┬──────────────┴────────┬──────────┴──────┘
              │                       │
              ▼                       ▼
        ┌─────────┐           ┌──────────────┐
        │  Bronze │◄──────────│ Spark Stream │
        │  (Raw)  │           └──────────────┘
        └────┬────┘
             │  Spark Batch Jobs
             ▼
        ┌─────────┐
        │  Silver │  Cleaned · Validated · Deduplicated
        └────┬────┘
             │  Spark Aggregate Jobs
             ▼
        ┌─────────┐
        │   Gold  │  Dimensional Models · Business KPIs
        └────┬────┘
             │
    ┌────────┼─────────┐
    ▼        ▼         ▼
Redshift  Snowflake  BigQuery
    │
    ▼
FastAPI · Grafana · ML Pipelines
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Languages | Python 3.12 · SQL |
| Compute | Apache Spark 3.5 · PySpark |
| Storage | Delta Lake · AWS S3 · MinIO |
| Streaming | Apache Kafka 3.7 · Spark Structured Streaming |
| Orchestration | Apache Airflow 2.9 |
| Warehouses | Amazon Redshift · Snowflake · Google BigQuery |
| Operational DB | PostgreSQL 16 |
| API | FastAPI 0.111 · Pydantic v2 |
| Containers | Docker · Docker Compose |
| IaC | Terraform 1.8 |
| CI/CD | GitHub Actions |
| Monitoring | Prometheus · Grafana |
| Logging | ELK Stack (Elasticsearch · Logstash · Kibana) |
| Cloud | AWS (S3, EMR, Glue, Redshift) · Databricks |
| Code Quality | Black · isort · ruff · mypy |

---

## Repository Structure

```
enterprise-data-lakehouse/
│
├── .github/                    # GitHub Actions CI/CD workflows
│   └── workflows/
│       ├── ci.yml              # Lint · type-check · test
│       ├── cd-staging.yml      # Deploy to staging on merge
│       └── cd-prod.yml         # Deploy to prod on release tag
│
├── configs/                    # Environment-specific configuration
│   ├── airflow/                # Airflow connections & variables
│   ├── kafka/                  # Kafka topic & consumer configs
│   ├── spark/                  # Spark defaults per environment
│   ├── grafana/                # Dashboard JSON & provisioning
│   ├── prometheus/             # Scrape configs & alert rules
│   └── environments/
│       ├── dev/
│       ├── staging/
│       └── prod/
│
├── src/                        # Core application source
│   ├── ingestion/
│   │   └── streaming/          # Kafka producers (synthetic event generators)
│   ├── processing/
│   │   ├── bronze/             # Raw landing processors
│   │   ├── silver/             # Cleanse & validate
│   │   └── gold/               # Aggregate & model
│   ├── quality/
│   │   ├── checks/             # 80+ data quality rule implementations
│   │   ├── reports/            # Quality report generators
│   │   └── contracts/          # Data contract definitions
│   ├── catalog/                # Metadata catalog client
│   ├── lineage/                # Data lineage tracker
│   └── common/
│       ├── utils/
│       ├── schemas/            # Pydantic & Spark schema definitions
│       ├── models/
│       ├── config/             # Config loader (env-aware)
│       ├── logging/            # Structured logging setup
│       └── security/           # Secrets management
│
├── spark/                      # PySpark jobs (submitted to EMR / Databricks)
│   ├── jobs/
│   │   ├── bronze/             # Raw ingestion jobs
│   │   ├── silver/             # Transform & validate jobs
│   │   ├── gold/               # Dimensional model jobs
│   │   ├── warehouse/          # Gold -> Redshift / Snowflake / BigQuery
│   │   ├── quality/            # Data quality runner
│   │   └── maintenance/        # OPTIMIZE + VACUUM for every layer
│   ├── streaming/              # Structured Streaming consumers (Kafka -> Bronze)
│   └── schemas/                # StructType definitions
│
├── airflow/                    # Orchestration
│   ├── dags/
│   │   ├── bronze_*_dag.py     # Source → Bronze (batch + streaming health)
│   │   ├── silver_/gold_batch_dag.py  # Bronze → Silver → Gold
│   │   ├── dq_dag.py           # Data quality checks & reports
│   │   ├── warehouse_load_dag.py      # Gold → Warehouse loads
│   │   └── pipeline_*_dag.py   # E2E, backfill, maintenance, SLA monitoring
│   ├── plugins/
│   │   ├── operators/          # Custom Airflow operators
│   │   ├── sensors/            # File & API sensors
│   │   └── hooks/              # Custom hooks (Delta, Kafka)
│   └── include/sql/            # Templated SQL for Airflow tasks
│
├── warehouse/                  # Dimensional model SQL
│   ├── schemas/
│   │   ├── redshift/
│   │   ├── snowflake/
│   │   └── bigquery/
│   ├── migrations/             # Flyway-style versioned migrations
│   ├── models/
│   │   ├── facts/              # Fact table DDL
│   │   └── dimensions/         # Dimension table DDL (incl. SCD2)
│   ├── procedures/             # Stored procedures
│   └── seeds/                  # Reference data seeds
│
├── api/                        # FastAPI service
│   ├── routers/                # Route handlers per domain
│   ├── models/                 # Pydantic response models
│   ├── services/               # Business logic layer
│   ├── middleware/             # Auth · logging · rate limiting
│   └── dependencies/           # DI providers (DB, cache)
│
├── tests/                      # Test suite (target: >90% coverage)
│   ├── unit/
│   ├── integration/
│   ├── pipeline/
│   └── fixtures/
│
├── docker/                     # Dockerfiles per service
├── monitoring/                 # Prometheus rules, Grafana dashboards, ELK configs
├── terraform/                  # AWS infrastructure as code
├── docs/                       # All project documentation
├── scripts/                    # Setup, deployment, maintenance scripts
├── datasets/                   # Synthetic data generators and samples
├── notebooks/                  # Jupyter notebooks for exploration & analytics
│
├── docker-compose.yml          # Full local stack
├── pyproject.toml              # Python project metadata & tool config
├── Makefile                    # Developer convenience targets
├── .env.example                # Environment variable template
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker Desktop ≥ 25
- Docker Compose ≥ 2.24
- Python 3.12
- Make

### 1. Clone & Configure

```bash
git clone https://github.com/<your-org>/enterprise-data-lakehouse.git
cd enterprise-data-lakehouse
cp .env.example .env
# Edit .env with your credentials
```

### 2. Start the Full Local Stack

```bash
make up
```

This starts: Spark, Kafka, Zookeeper, Airflow (webserver + scheduler + worker), PostgreSQL, MinIO, Prometheus, Grafana, FastAPI, Elasticsearch, Logstash, Kibana.

### 3. Generate Synthetic Data

```bash
make generate-data
```

Produces 1 M+ records across all 14 operational domains into `datasets/raw/`.

### 4. Run the Pipeline

```bash
make pipeline-run
```

Triggers the full Bronze → Silver → Gold pipeline via Airflow.

### 5. Access Services

| Service | URL | Default Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark UI | http://localhost:4040 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Kibana | http://localhost:5601 | — |
| FastAPI Docs | http://localhost:8000/docs | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Prometheus | http://localhost:9090 | — |

### 6. Run Tests

```bash
make test           # Full suite
make test-unit      # Unit only (fast)
make test-coverage  # Coverage report
```

---

## Data Domains

| Domain | Source System | Format | Volume / Day | Load Pattern |
|---|---|---|---|---|
| Customers | CRM (PostgreSQL) | CDC / JSON | 50 K | Incremental |
| Orders | Ecommerce API | JSON | 200 K | Streaming + Batch |
| Products | PIM system | CSV / Parquet | 100 K | Full daily |
| Inventory | WMS | JSON | 500 K | Streaming |
| Payments | Payment gateway | JSON | 200 K | Streaming |
| Shipments | 3PL API | JSON | 150 K | Incremental |
| Returns | RMS | CSV | 30 K | Incremental |
| Marketing Campaigns | Marketing Cloud | CSV | 10 K | Full daily |
| Website Clickstream | Kafka / Kinesis | JSON | 2 M | Streaming |
| Customer Reviews | Review platform | JSON | 20 K | Incremental |
| IoT Warehouse Sensors | MQTT → Kafka | JSON | 1 M | Streaming |
| Suppliers | ERP | CSV | 5 K | Full daily |
| Employees | HRIS | CSV | 5 K | Full daily |
| Finance Transactions | ERP | Parquet | 100 K | Incremental |

---

## Medallion Layers

### Bronze — Raw Immutable Landing Zone

- **Storage:** `s3://retailnexus-lakehouse/bronze/{domain}/{year}/{month}/{day}/`
- **Format:** Delta Lake (preserving source format + metadata)
- **Guarantee:** No transformations — exactly as received
- **Retention:** 7 years (regulatory)
- **Partitioning:** `year / month / day / hour` (streaming), `year / month / day` (batch)

### Silver — Cleansed & Validated

- **Storage:** `s3://retailnexus-lakehouse/silver/{domain}/`
- **Format:** Delta Lake (Parquet + transaction log)
- **Transformations:**
  - Type casting & standardization
  - Null handling (drop / impute / flag)
  - Duplicate removal (deterministic deduplication)
  - Business rule validation
  - PII masking / tokenization
  - Schema enforcement
- **SCD Type 2** on slowly changing dimensions
- **Data quality gate:** pipeline halts if critical checks fail

### Gold — Business-Ready Dimensional Models

- **Storage:** `s3://retailnexus-lakehouse/gold/{model}/`
- **Format:** Delta Lake
- **Models:** Star schema — 6 fact tables · 8 dimension tables
- **Materialization:** Scheduled hourly aggregations + on-demand compute
- **Targets:** Redshift (BI), Snowflake (self-service), BigQuery (ML / data science)

---

## Ingestion Patterns

| Pattern | Implementation | Trigger |
|---|---|---|
| Batch CSV / JSON / Parquet | `src/processing/bronze/batch_ingestor.py` via `spark/jobs/bronze/ingest_bronze.py` | Airflow (`bronze_batch_ingestion`) |
| Kafka Streaming | `spark/streaming/*_stream.py` (5 topics) | Continuous |
| Incremental Load | Watermark-based (`pipeline.watermarks`) | Airflow schedule |
| Schema Evolution | Auto-merge via Delta Lake `mergeSchema` | Automatic |

> REST-API connectors, CDC simulation and an S3 file-watcher are on the [ROADMAP](ROADMAP.md) but not implemented.

---

## Data Quality

80+ automated quality checks organized in five categories:

| Category | Checks |
|---|---|
| Completeness | Null rates, required field presence |
| Uniqueness | Duplicate detection per primary key |
| Validity | Range checks, regex pattern matching, enum membership |
| Consistency | Referential integrity, cross-domain joins |
| Timeliness | Freshness assertions, pipeline SLA monitoring |

Quality results are written to `s3://retailnexus-lakehouse/quality-reports/` and surfaced in Grafana.

---

## Analytics & Warehouse

### Fact Tables

`fact_sales` · `fact_inventory` · `fact_marketing` · `fact_returns` · `fact_shipments` · `fact_finance`

### Dimension Tables

`dim_customer` · `dim_date` · `dim_store` · `dim_employee` · `dim_supplier` · `dim_location` · `dim_product` · `dim_promotion`

### Analytical SQL Scripts

Located in `warehouse/models/` — covers:

- Daily / weekly / monthly sales analysis
- Customer segmentation (RFM)
- ABC inventory analysis
- Revenue & profitability
- Top customers by CLV
- Inventory turnover
- Marketing effectiveness & ROAS
- Repeat purchase rate
- Daily KPI dashboard

---

## API

FastAPI service exposing curated Gold layer data:

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/v1/sales/latest` | Latest sales metrics |
| GET | `/v1/customers/top` | Top customers by revenue |
| GET | `/v1/revenue` | Revenue summary with filters |
| GET | `/v1/inventory` | Inventory levels by SKU |
| GET | `/v1/products/{sku}` | Product lookup |
| GET | `/v1/customers/{id}` | Customer lookup |
| GET | `/docs` | Swagger UI (auto-generated) |
| GET | `/redoc` | ReDoc UI |

---

## Monitoring & Observability

### Metrics (Prometheus + Grafana)

- Pipeline latency per DAG and task
- Kafka consumer lag per topic
- Spark job duration and shuffle metrics
- Data quality scores per domain
- API request rate, latency, error rate
- Delta table file counts and compaction status

### Logging (ELK Stack)

Structured JSON logs from all components flow into Elasticsearch via Logstash.  
Kibana dashboards cover: pipeline errors, Spark driver logs, API access logs, Airflow task logs.

---

## CI/CD

Three GitHub Actions workflows:

| Workflow | Trigger | Steps |
|---|---|---|
| `ci.yml` | Every PR | ruff lint → mypy → pytest → Docker build |
| `cd-staging.yml` | Merge to `main` | Build → push image → deploy staging → smoke test |
| `cd-prod.yml` | Release tag `v*` | Build → push image → deploy prod → notify |

---

## Security

| Concern | Implementation |
|---|---|
| Secrets | AWS Secrets Manager / `.env` (local) |
| Encryption at rest | S3 SSE-S3, Delta Lake encryption |
| Encryption in transit | TLS 1.3 on all inter-service communication |
| IAM | Least-privilege IAM roles per component |
| RBAC | Airflow RBAC + Redshift/Snowflake row-level security |
| PII | Tokenization in Silver layer before write |
| Audit | All data access logged to Elasticsearch |

---

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/architecture/ARCHITECTURE.md) | Full system design, data flows, TDRs |
| [Governance & Observability](docs/architecture/GOVERNANCE.md) | Data contracts, catalog, lineage, alerting, metadata DB |
| [Diagrams](docs/diagrams/DIAGRAMS.md) | Mermaid architecture and flow diagrams |
| [Setup Guide](docs/setup/SETUP.md) | Local development environment setup |
| [Deployment Guide](docs/deployment/DEPLOYMENT.md) | AWS production deployment with Terraform |
| [Data Dictionary](docs/data-dictionary/DATA_DICTIONARY.md) | All 14 domains + Gold star schema field reference |
| [Developer Guide](docs/developer/DEVELOPER.md) | Coding standards, patterns, PR process |
| [Troubleshooting Runbook](docs/troubleshooting/TROUBLESHOOTING.md) | Operational runbook for common failures |

---

## Development Guide

See [docs/developer/DEVELOPER.md](docs/developer/DEVELOPER.md) for the full guide.

```bash
# Install dev dependencies
make install-dev

# Run linters
make lint

# Run type checker
make typecheck

# Run all tests
make test

# Build Docker images
make build

# Tear down local stack
make down
```

---

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the full 10-sprint delivery plan.

| Sprint | Focus |
|---|---|
| 1 | Foundation — repo, configs, Docker, CI/CD skeleton |
| 2 | Synthetic data generation (1 M+ records) |
| 3 | Bronze layer — all 14 ingestion patterns |
| 4 | Silver layer — cleanse, validate, SCD2 |
| 5 | Gold layer — dimensional models |
| 6 | Data warehouse loads (Redshift · Snowflake · BigQuery) |
| 7 | Data quality framework (80+ checks) |
| 8 | Airflow DAGs for all pipelines |
| 9 | FastAPI service + monitoring |
| 10 | Testing, hardening, documentation |

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org/)
4. Push and open a Pull Request
5. CI must pass before review

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

*Built as a senior-level portfolio demonstration of enterprise data engineering patterns.*  
*All data is synthetic and does not represent real individuals or organizations.*
