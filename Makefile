# ═══════════════════════════════════════════════════════════════════════════════
# RetailNexus Data Lakehouse — Developer Makefile
# ═══════════════════════════════════════════════════════════════════════════════

.DEFAULT_GOAL := help
.PHONY: help health pre-commit up down restart build pull logs ps shell-spark shell-airflow \
        shell-api shell-postgres install install-dev lint format typecheck \
        test test-unit test-integration test-pipeline test-coverage \
        generate-data kafka-topics pipeline-run pipeline-ingest \
        pipeline-bronze pipeline-silver pipeline-gold warehouse-load \
        quality-check sync-catalog setup-elk smoke-test init-repo clean nuke \n        bronze-ingest bronze-optimize silver-process gold-build dq-run bootstrap-airflow upload-data down-clean spark-shell

# ─── Variables ────────────────────────────────────────────────────────────────
COMPOSE        := docker compose
PYTHON         := python
PIP            := pip
PROJECT_NAME   := retailnexus-lakehouse
DOCKER_REGISTRY := ghcr.io/retailnexus

# Colour helpers
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[32m
CYAN  := \033[36m
YELLOW := \033[33m
RED   := \033[31m

# ─── Help ─────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "$(BOLD)RetailNexus Data Lakehouse — Available Targets$(RESET)"
	@echo "──────────────────────────────────────────────────────────────────────"
	@echo ""
	@echo "$(CYAN)Docker Stack$(RESET)"
	@echo "  make up               Start all services (detached)"
	@echo "  make down             Stop and remove containers"
	@echo "  make restart          Restart all services"
	@echo "  make build            Build all custom Docker images"
	@echo "  make pull             Pull latest base images"
	@echo "  make logs             Tail logs from all services"
	@echo "  make ps               Show running services"
	@echo ""
	@echo "$(CYAN)Dev Shells$(RESET)"
	@echo "  make shell-spark      Open PySpark shell"
	@echo "  make shell-airflow    Open Airflow CLI shell"
	@echo "  make shell-api        Open FastAPI container shell"
	@echo "  make shell-postgres   Open psql shell"
	@echo ""
	@echo "$(CYAN)Code Quality$(RESET)"
	@echo "  make lint             Run ruff + isort + black checks"
	@echo "  make format           Auto-format with black + isort"
	@echo "  make typecheck        Run mypy type checker"
	@echo ""
	@echo "$(CYAN)Testing$(RESET)"
	@echo "  make test             Run full test suite"
	@echo "  make test-unit        Unit tests only (fast, no services needed)"
	@echo "  make test-integration Integration tests (requires live services)"
	@echo "  make test-pipeline    End-to-end pipeline tests"
	@echo "  make test-coverage    Run tests and open coverage report"
	@echo ""
	@echo "$(CYAN)Data Pipeline$(RESET)"
	@echo "  make generate-data    Generate 1M+ synthetic records in datasets/raw/"
	@echo "  make kafka-topics     Create all Kafka topics"
	@echo "  make pipeline-run     Trigger full Bronze→Silver→Gold pipeline"
	@echo "  make pipeline-bronze  Run Bronze ingestion only"
	@echo "  make pipeline-silver  Run Silver processing only"
	@echo "  make pipeline-gold    Run Gold model build only"
	@echo "  make warehouse-load   Load Gold → Redshift/Snowflake/BigQuery"
	@echo "  make quality-check    Run all 80+ data quality checks"
	@echo "  make sync-catalog     Sync Delta tables + lineage into the catalog"
	@echo "  make setup-elk        Install ES index template/ILM + Kibana dashboards"
	@echo "  make smoke-test       Smoke-test the running API"
	@echo ""
	@echo "$(CYAN)Setup$(RESET)"
	@echo "  make install          Install runtime dependencies"
	@echo "  make install-dev      Install dev + runtime dependencies"
	@echo "  make init-repo        First-time repo setup (copy .env, create buckets)"
	@echo ""
	@echo "$(CYAN)Maintenance$(RESET)"
	@echo "  make clean            Remove temp files and __pycache__"
	@echo "  make nuke             ⚠ Destroy all containers, volumes, and data"
	@echo ""

# ─── Docker Stack ─────────────────────────────────────────────────────────────
up:
	@echo "$(GREEN)Starting RetailNexus Lakehouse stack...$(RESET)"
	$(COMPOSE) up -d
	@echo ""
	@echo "$(GREEN)Services started. Access points:$(RESET)"
	@echo "  Airflow   → http://localhost:8080  (admin/admin)"
	@echo "  Spark UI  → http://localhost:18080"
	@echo "  Grafana   → http://localhost:3000  (admin/admin)"
	@echo "  Kibana    → http://localhost:5601"
	@echo "  FastAPI   → http://localhost:8000/docs"
	@echo "  MinIO     → http://localhost:9001  (minioadmin/minioadmin)"
	@echo "  Prometheus→ http://localhost:9090"

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart

build:
	@echo "$(GREEN)Building Docker images...$(RESET)"
	$(COMPOSE) build --parallel

pull:
	$(COMPOSE) pull

health:
	$(COMPOSE) ps
	@curl -sf http://localhost:8000/health/ready && echo "" || (echo "API not ready" && exit 1)

logs:
	$(COMPOSE) logs -f --tail=100

logs-%:
	$(COMPOSE) logs -f --tail=200 $*

ps:
	$(COMPOSE) ps

pre-commit:
	pre-commit run --all-files

# ─── Dev Shells ───────────────────────────────────────────────────────────────
shell-spark:
	$(COMPOSE) exec spark-master /opt/spark/bin/pyspark \
		--conf spark.jars.packages=io.delta:delta-spark_2.12:3.2.0 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

shell-airflow:
	$(COMPOSE) exec airflow-scheduler bash

shell-api:
	$(COMPOSE) exec fastapi bash

shell-postgres:
	$(COMPOSE) exec postgres psql -U lakehouse -d lakehouse

shell-kafka:
	$(COMPOSE) exec kafka bash

# ─── Code Quality ─────────────────────────────────────────────────────────────
lint:
	@echo "$(GREEN)Running linters...$(RESET)"
	ruff check src/ tests/ api/ spark/ datasets/generators/ airflow/
	isort --check-only src/ tests/ api/ spark/ datasets/generators/ airflow/
	black --check src/ tests/ api/ spark/ datasets/generators/ airflow/
	@echo "$(GREEN)All lint checks passed.$(RESET)"

format:
	@echo "$(GREEN)Formatting code...$(RESET)"
	black src/ tests/ api/ spark/ datasets/generators/ airflow/
	isort src/ tests/ api/ spark/ datasets/generators/ airflow/
	ruff check --fix src/ tests/ api/ spark/ datasets/generators/ airflow/

typecheck:
	@echo "$(GREEN)Running mypy type checker...$(RESET)"
	mypy src/ api/ --ignore-missing-imports

# ─── Testing ─────────────────────────────────────────────────────────────────
test:
	@echo "$(GREEN)Running full test suite...$(RESET)"
	pytest tests/ -v --tb=short

test-unit:
	@echo "$(GREEN)Running unit tests...$(RESET)"
	pytest tests/unit/ -v -m unit --tb=short

test-integration:
	@echo "$(GREEN)Running integration tests (requires live services)...$(RESET)"
	pytest tests/integration/ -v -m integration --tb=short

test-pipeline:
	@echo "$(GREEN)Running pipeline tests...$(RESET)"
	pytest tests/pipeline/ -v -m pipeline --tb=short

test-coverage:
	@echo "$(GREEN)Running tests with coverage...$(RESET)"
	pytest tests/ \
		--cov=src \
		--cov=api \
		--cov=spark \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		--cov-fail-under=90
	@echo "$(GREEN)Coverage report written to htmlcov/index.html$(RESET)"

# ─── Installation ─────────────────────────────────────────────────────────────
install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev,datagen,api]"
	pre-commit install

# ─── First-Time Setup ─────────────────────────────────────────────────────────
init-repo:
	@echo "$(GREEN)Initialising repository...$(RESET)"
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "$(YELLOW)Created .env from .env.example — update secrets before running.$(RESET)"; \
	else \
		echo ".env already exists, skipping."; \
	fi
	@echo "$(GREEN)Generating Airflow Fernet key...$(RESET)"
	@$(PYTHON) -c "from cryptography.fernet import Fernet; k=Fernet.generate_key().decode(); print(f'AIRFLOW_FERNET_KEY={k}')"
	@echo "$(YELLOW)Copy the AIRFLOW_FERNET_KEY above into your .env file.$(RESET)"
	@mkdir -p datasets/raw/csv datasets/raw/json datasets/raw/parquet datasets/samples
	@touch datasets/raw/.gitkeep datasets/samples/.gitkeep
	@echo "$(GREEN)Setup complete. Run 'make up' to start the stack.$(RESET)"

# ─── Data Pipeline ────────────────────────────────────────────────────────────
generate-data:
	@echo "$(GREEN)Generating synthetic datasets (1M+ records)...$(RESET)"
	$(PYTHON) scripts/data-generation/generate_all.py

kafka-topics:
	@echo "$(GREEN)Creating Kafka topics...$(RESET)"
	bash scripts/setup/create_kafka_topics.sh

pipeline-run:
	@echo "$(GREEN)Triggering full pipeline via Airflow CLI...$(RESET)"
	$(COMPOSE) exec airflow-scheduler airflow dags trigger bronze_batch_ingestion
	$(COMPOSE) exec airflow-scheduler airflow dags trigger silver_batch
	$(COMPOSE) exec airflow-scheduler airflow dags trigger gold_batch

pipeline-bronze:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger bronze_batch_ingestion

pipeline-silver:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger silver_batch

pipeline-gold:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger gold_batch

warehouse-load:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger warehouse_load

quality-check:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger data_quality

# Direct spark-submit into the Spark master container (Option B in docs/setup/SETUP.md).
# DOMAIN=<name> limits a run to one domain/table; LAYER / DOMAINS drive dq-run.
SPARK_SUBMIT := $(COMPOSE) exec spark-master spark-submit --master spark://spark-master:7077 	--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4
JOB := /opt/spark/spark/jobs

bronze-ingest:
	$(SPARK_SUBMIT) $(JOB)/bronze/ingest_bronze.py $(if $(DOMAIN),--domain $(DOMAIN),--all)

bronze-optimize:
	$(SPARK_SUBMIT) $(JOB)/bronze/optimize_bronze.py $(if $(DOMAIN),--domain $(DOMAIN),--all) --skip-vacuum

silver-process:
	$(SPARK_SUBMIT) $(JOB)/silver/process_silver.py $(if $(DOMAIN),--domain $(DOMAIN),--all)

gold-build:
	$(SPARK_SUBMIT) $(JOB)/gold/build_gold.py $(if $(TABLE),--table $(TABLE),--all)

dq-run:
	$(SPARK_SUBMIT) $(JOB)/quality/run_dq.py --layer $(or $(LAYER),all) $(if $(DOMAINS),--domains $(subst $(comma), ,$(DOMAINS)))

comma := ,

bootstrap-airflow:
	cat scripts/airflow/bootstrap_airflow.py | $(COMPOSE) exec -T airflow-scheduler python -

upload-data:
	$(COMPOSE) run --rm --no-deps -v "$(CURDIR)/datasets/raw:/data:ro" --entrypoint bash minio-init 		-c 'mc alias set lh http://minio:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" && mc mirror --overwrite /data "lh/$$LAKEHOUSE_BUCKET/raw"'

down-clean:
	$(COMPOSE) down -v --remove-orphans

spark-shell: shell-spark

sync-catalog:
	$(PYTHON) scripts/maintenance/sync_catalog.py

setup-elk:
	bash scripts/setup/setup_elk.sh

quality-catalog:
	$(PYTHON) -m src.quality.catalog

# Run from inside the compose network: a host-side run measures Docker Desktop's port proxy, not the API.
# The target needs API_RATE_LIMIT_PER_MINUTE=0 (a single key at 100 RPS is otherwise throttled).
api-load-test:
	$(COMPOSE) cp scripts/performance/api_load_test.py airflow-scheduler:/tmp/api_load_test.py
	$(COMPOSE) exec -T airflow-scheduler bash -c "python -m pip install -q httpx && python /tmp/api_load_test.py --base-url http://fastapi:8000 --api-key $(or $(API_KEY),dev-api-key-retailnexus-2026) --rps $(or $(RPS),100) --duration $(or $(DURATION),30)"

smoke-test:
	bash scripts/deployment/smoke_test.sh $${API_URL:-http://localhost:8000} $${API_KEY:-dev-api-key-retailnexus-2026}

# ─── Maintenance ─────────────────────────────────────────────────────────────
clean:
	@echo "$(GREEN)Cleaning temporary files...$(RESET)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -name ".coverage*" -delete 2>/dev/null || true
	@echo "$(GREEN)Clean complete.$(RESET)"

nuke:
	@echo "$(RED)$(BOLD)WARNING: This will destroy all containers and data volumes.$(RESET)"
	@read -p "Are you sure? (yes/no): " confirm && [ "$$confirm" = "yes" ]
	$(COMPOSE) down -v --remove-orphans
	docker system prune -f
	@echo "$(RED)Stack destroyed.$(RESET)"

# ─── CI Helpers (used by GitHub Actions) ─────────────────────────────────────
ci-lint: lint typecheck
ci-test: test-unit
ci-build: build
