# Local Development Setup Guide

This guide walks you from zero to a fully running RetailNexus Data Lakehouse on your local machine.

---

## Prerequisites

| Tool | Minimum Version | Notes |
|---|---|---|
| Docker Desktop | 25.0 | Enable at least 8 CPU / 16 GB RAM in Docker settings |
| Docker Compose | 2.24 | Bundled with Docker Desktop |
| Python | 3.12 | Use `pyenv` or the official installer |
| Make | 4.x | Ships with macOS Xcode Tools; `choco install make` on Windows |
| Git | 2.40 | Any recent version works |

> **Windows users:** Use WSL 2 (Ubuntu 22.04 LTS recommended). All `make` targets run under Bash.

---

## 1. Clone the Repository

```bash
git clone https://github.com/<your-org>/enterprise-data-lakehouse.git
cd enterprise-data-lakehouse
```

---

## 2. Configure Environment Variables

```bash
cp .env.example .env
```

Open `.env` and review the defaults. For local development, the defaults work as-is.
For cloud targets, fill in:

```dotenv
# S3 / MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
AWS_ACCESS_KEY_ID=        # only needed when targeting real S3
AWS_SECRET_ACCESS_KEY=    # only needed when targeting real S3

# PostgreSQL (local — do not change)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=lakehouse
POSTGRES_PASSWORD=lakehouse
POSTGRES_DB=lakehouse

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Airflow
AIRFLOW__CORE__FERNET_KEY=        # generate: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__WEBSERVER__SECRET_KEY=   # any random string

# API
API_SECRET_KEY=   # min 32 chars, never default in prod

# Secrets backend (env = local dev, aws_secrets_manager = prod)
SECRETS_BACKEND=env
```

> **Security:** `.env` is in `.gitignore`. Never commit real credentials.

---

## 3. Install Python Dependencies (for local dev tooling)

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev,api,spark,kafka,datagen]"
pre-commit install
```

---

## 4. Start the Full Local Stack

```bash
make up
```

This starts **16 services** in Docker Compose:

| Service | Port | Purpose |
|---|---|---|
| `spark-master` | 8081 | Spark standalone master UI |
| `spark-worker` | — | Spark executor |
| `zookeeper` | 2181 | Kafka coordination |
| `kafka` | 9092 | Message broker |
| `kafka-ui` | 8090 | Kafka topic browser |
| `postgres` | 5432 | Pipeline metadata DB |
| `minio` | 9000 / 9001 | S3-compatible object store |
| `airflow-webserver` | 8080 | Airflow UI (admin/admin) |
| `airflow-scheduler` | — | DAG scheduler |
| `airflow-worker` | — | Celery worker |
| `api` | 8000 | FastAPI service |
| `prometheus` | 9090 | Metrics collection |
| `grafana` | 3000 | Dashboards (admin/admin) |
| `elasticsearch` | 9200 | Log storage |
| `logstash` | 5044 | Log ingestion |
| `kibana` | 5601 | Log exploration |

Startup takes ~2 minutes on the first run (image pulls). On subsequent runs, ~30 seconds.

### Verify all services are healthy

```bash
make health
# or:
docker compose ps
```

All services should show `healthy` or `running`.

---

## 5. Bootstrap Airflow Connections and Variables

```bash
make bootstrap-airflow
```

This seeds Airflow with:
- Spark connection (`spark_default` → `spark://spark-master:7077`)
- PostgreSQL connection (`postgres_pipeline`)
- MinIO/S3 connection (`aws_default`)
- All pipeline variables (S3 paths, domain lists, SLA thresholds)

---

## 6. Generate Synthetic Data

```bash
make generate-data
```

Produces **1 M+ records** across all 14 domains into `datasets/raw/`:

| Domain | Records | Format |
|---|---|---|
| Customers | 500,000 | JSON |
| Orders | 1,000,000 | JSON |
| Order Lines | 2,500,000 | JSON |
| Payments | 1,000,000 | JSON |
| Products | 50,000 | CSV |
| Inventory | 500,000 | JSON |
| Shipments | 800,000 | JSON |
| Returns | 150,000 | CSV |
| Clickstream | 5,000,000 | JSON.gz |
| IoT Sensors | 2,000,000 | JSON |
| Reviews | 200,000 | JSON |
| Campaigns | 10,000 | CSV |
| Suppliers | 2,000 | CSV |
| Employees | 5,000 | CSV |
| Finance | 500,000 | Parquet |
| Stores | 250 | CSV |

To generate a smaller dataset for faster iteration:

```bash
python -m datasets.generators --scale-factor 0.1 --domains orders,customers
```

---

## 7. Upload Data to MinIO

```bash
make upload-data
```

Uploads all generated files to `s3://retailnexus-lakehouse/raw/` (served by local MinIO).

---

## 8. Run the Pipeline

### Option A — Airflow (recommended)

```bash
# Trigger the master pipeline DAG for today
make pipeline-run
# or navigate to http://localhost:8080 and trigger pipeline_e2e_dag manually
```

### Option B — Direct spark-submit

```bash
# Bronze
make bronze-ingest

# Silver (runs after Bronze completes)
make silver-process

# Gold
make gold-build

# Warehouse loads
make warehouse-load WAREHOUSE=redshift   # or snowflake, bigquery
```

---

## 9. Access Services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8081 | — |
| Kafka UI | http://localhost:8090 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| Kibana | http://localhost:5601 | — |
| Prometheus | http://localhost:9090 | — |
| FastAPI Docs | http://localhost:8000/docs | X-API-Key: dev-api-key-retailnexus-2026 |
| FastAPI Metrics | http://localhost:8000/metrics | — |

---

## 10. Run Tests

```bash
# Unit tests only (no external services, ~30s)
make test-unit

# Pipeline tests (requires local Spark — no docker-compose needed)
make test-pipeline

# Integration tests (requires full docker-compose stack)
TEST_DATABASE_URL=postgresql://lakehouse:lakehouse@localhost:5432/lakehouse \
TEST_API_URL=http://localhost:8000 \
TEST_API_KEY=dev-api-key-retailnexus-2026 \
make test-integration

# Full suite with coverage
make test-coverage
```

Coverage must stay ≥ 90% (enforced by `[tool.coverage.report].fail_under` in `pyproject.toml`).

---

## 11. Lint and Type-Check

```bash
make lint       # ruff + isort check
make format     # black + isort fix-in-place
make typecheck  # mypy
make pre-commit # run all pre-commit hooks against staged files
```

---

## 12. Tear Down

```bash
make down         # stop containers, preserve volumes
make down-clean   # stop containers AND delete all volumes (full reset)
```

---

## Common Local Dev Workflows

### Iterate on a single domain

```bash
# Regenerate only orders data
python -m datasets.generators --domains orders

# Re-run Bronze for orders
make bronze-ingest DOMAIN=orders

# Re-run Silver for orders
make silver-process DOMAIN=orders
```

### Inspect Delta tables

```bash
# Open a PySpark shell connected to local Spark
make spark-shell

# Then in the shell:
# spark.read.format("delta").load("s3a://retailnexus-lakehouse/silver/orders/").show()
```

### Trigger a DQ run manually

```bash
make dq-run LAYER=bronze
make dq-run LAYER=silver DOMAINS=orders,payments
```

### Replay a historical batch (backfill)

Trigger the `pipeline_backfill_dag` via the Airflow UI with:
- `layer`: bronze
- `start_date`: 2025-01-01
- `end_date`: 2025-01-31
- `domains`: orders,payments

---

## Environment Variable Reference

See [`.env.example`](../../.env.example) for the full list with descriptions.

| Variable | Default | Required in Prod |
|---|---|---|
| `ENVIRONMENT` | `dev` | Yes |
| `POSTGRES_HOST` | `postgres` | Yes |
| `MINIO_ENDPOINT` | `http://minio:9000` | No (use S3) |
| `S3_BUCKET` | `retailnexus-lakehouse` | Yes |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Yes |
| `SECRETS_BACKEND` | `env` | Yes (`aws_secrets_manager`) |
| `API_SECRET_KEY` | — | Yes (min 32 chars) |
| `AIRFLOW__CORE__FERNET_KEY` | — | Yes |
