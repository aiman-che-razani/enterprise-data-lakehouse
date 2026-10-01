# Developer Guide

This guide is for engineers contributing to the RetailNexus Data Lakehouse platform.

---

## Table of Contents

1. [Getting Started](#1-getting-started)
2. [Repository Layout](#2-repository-layout)
3. [Coding Standards](#3-coding-standards)
4. [Testing Strategy](#4-testing-strategy)
5. [Architecture Patterns](#5-architecture-patterns)
6. [Adding a New Data Domain](#6-adding-a-new-data-domain)
7. [Adding a New Data Quality Check](#7-adding-a-new-data-quality-check)
8. [Adding a New API Endpoint](#8-adding-a-new-api-endpoint)
9. [Git Workflow](#9-git-workflow)
10. [Pull Request Process](#10-pull-request-process)
11. [Environment Configuration](#11-environment-configuration)

---

## 1. Getting Started

Follow [docs/setup/SETUP.md](../setup/SETUP.md) to bring up the local stack.
Come back here once `make test-unit` passes.

```bash
# Verify your environment
make health           # all Docker services running
make test-unit        # ~30s, no external deps
make lint             # ruff + isort
make typecheck        # mypy strict
```

All four commands must pass before opening a PR.

---

## 2. Repository Layout

```
src/
  common/
    config/       Settings (pydantic-settings v2) — single source of truth for all env vars
    logging/      structlog JSON logger — use get_logger(__name__) everywhere
    security/     SecretsClient — fetches from env (dev) or AWS Secrets Manager (prod)
    utils/        retry.py (tenacity presets), type helpers

  processing/
    bronze/       BronzeBatchIngestor, quality gates, domain metadata
    silver/       BaseSilverProcessor, ScdType2Handler, SilverDeduplicator, PiiTokenizer
    gold/         BaseGoldBuilder, surrogate_keys, domain builders

  quality/
    checks/       BaseQualityCheck subclasses (8 check types)
    engines/      BronzeDQEngine, SilverDQEngine, GoldDQEngine
    reporters/    PostgresReporter, AlertReporter
    dq_runner.py  Orchestrates engines → reporters → alerts

api/
  dependencies.py   get_db(), verify_api_key(), ThreadedConnectionPool
  routers/          one file per domain (health, pipeline, quality, catalog)
  services/         business logic (PipelineService, QualityService)
  repositories/     raw SQL (PipelineRepository, QualityRepository)
  models/           Pydantic response models

spark/
  jobs/
    bronze/       ingest_bronze.py, optimize_bronze.py
    silver/       process_silver.py
    gold/         build_gold.py
    warehouse/    load_warehouse.py
    quality/      run_dq.py
  streaming/      5 Kafka → Bronze streaming jobs

airflow/
  dags/           pipeline_e2e_dag, bronze/silver/gold_batch_dag, dq_dag, maintenance
  plugins/
    operators/    DQSparkOperator, DeltaIngestOperator
    sensors/      WatermarkSensor, DQPassRateSensor
    hooks/        PipelinePostgresHook
```

---

## 3. Coding Standards

### Python style

- **Formatter:** Black (line-length=100). Never override manually.
- **Imports:** isort with `profile=black`. Third-party, then first-party, then local.
- **Linter:** ruff with the rules in `pyproject.toml`. Fix all warnings.
- **Type checker:** mypy strict. All functions must be fully annotated.
- **Python version:** 3.12 target. Use `from __future__ import annotations` in every file.

### Comments and docstrings

- No comments explaining **what** the code does — names do that.
- Comment the **why**: a hidden constraint, a subtle invariant, a workaround.
- One-line docstrings only on public classes. No multi-paragraph docstrings.
- No `# TODO` or `# FIXME` in committed code — open a GitHub issue instead.

### Error handling

- Validate only at system boundaries (API request parameters, external API responses).
- Never catch `Exception` broadly in library code — let it propagate.
- Use `tenacity` retry decorators from `src.common.utils.retry` for I/O operations.

### Naming

| Context | Convention | Example |
|---|---|---|
| Functions / methods | `snake_case` | `get_pipeline_status()` |
| Classes | `PascalCase` | `BronzeBatchIngestor` |
| Constants | `UPPER_SNAKE` | `DOMAIN_PKS` |
| Pydantic models | `PascalCase` | `WatermarkResponse` |
| DB columns | `snake_case` | `bronze_last_ingest` |
| Delta paths | lowercase + underscores | `s3://…/silver/order_lines/` |

### Secrets

- Never hardcode credentials. Use `settings.postgres_dsn` or `SecretsClient.get_secret()`.
- Never log credentials, even at DEBUG level.
- In tests, mock secrets via `monkeypatch.setenv` or the `mock_secrets_env` fixture.

---

## 4. Testing Strategy

### Three test layers

| Layer | Location | Speed | Deps | Marker |
|---|---|---|---|---|
| Unit | `tests/unit/` | ~30s | None (mock everything) | `@pytest.mark.unit` |
| Pipeline | `tests/pipeline/` | ~2–5 min | Local PySpark (`local[*]`) | `@pytest.mark.pipeline` |
| Integration | `tests/integration/` | ~5–10 min | Full docker-compose stack | `@pytest.mark.integration` |

### Running tests

```bash
make test-unit         # CI baseline — must always pass
make test-pipeline     # Spark session, no network
make test-integration  # Requires: docker-compose up + env vars set
make test              # All layers
make test-coverage     # All layers + coverage report (must be ≥ 90%)
```

### Writing unit tests

- Mock all I/O: psycopg2 connections, boto3, Spark reads.
- Use `unittest.mock.patch` at the module level, not the class level.
- One `assert` per logical concern (multiple asserts in a single test case are fine when testing a single behaviour with multiple aspects).
- Do not write tests for private methods. Test behaviour, not implementation.

```python
# Good — tests observable behaviour
def test_verify_api_key_returns_context_on_valid_key():
    conn = _make_mock_conn({"key_id": 1, "client_name": "airflow", "permissions": ["read"]})
    ctx = verify_api_key(x_api_key="valid", conn=conn)
    assert isinstance(ctx, ApiKeyContext)
    assert ctx.permissions == ["read"]

# Bad — tests implementation detail
def test__hash_key_calls_hashlib():
    with patch("hashlib.sha256") as mock_sha:
        _hash_key("x")
    mock_sha.assert_called_once()
```

### PySpark tests

- Use `conftest.py:spark_session` fixture (scope=session) — starting Spark is expensive.
- Create DataFrames with `spark.createDataFrame(rows, schema)` — no file I/O.
- Use `tmp_path` (pytest built-in) for Delta Lake writes; it's cleaned up automatically.

### Coverage

- Coverage is enforced at ≥ 90% by `pyproject.toml`.
- Exclude unreachable branches with `# pragma: no cover` sparingly.
- New code without tests will block CI.

---

## 5. Architecture Patterns

### Settings

All configuration comes from `src.common.config.settings.Settings` (pydantic-settings v2).
Never read `os.environ` directly in application code. Always use the `settings` singleton.

```python
from src.common.config.settings import get_settings
settings = get_settings()    # lru_cache — safe to call frequently
dsn = settings.postgres_dsn
```

### Logging

Always use `structlog`. Never use `print()` or `logging.getLogger()` directly.

```python
from src.common.logging.logger import get_logger
logger = get_logger(__name__)

logger.info("bronze_ingested", domain="orders", rows=100_000, duration_ms=4321)
# Output: {"event":"bronze_ingested","domain":"orders","rows":100000,"duration_ms":4321,...}
```

Bind context at the start of a long-running operation:

```python
log = logger.bind(domain=domain, batch_id=batch_id)
log.info("processing_started")
# ... work ...
log.info("processing_complete", rows=n)
```

### Surrogate keys

Use `xxhash64` for all Gold surrogate keys. Never use sequences or UUIDs.

```python
from src.processing.gold.surrogate_keys import add_sk, date_to_sk
df = add_sk(df, "sk_order_id", "order_id")
df = df.withColumn("sk_date_id", date_to_sk(F.col("order_date")))
```

### Repository pattern (API)

All raw SQL lives in `api/repositories/`. Services call repositories, routers call services.
No SQL in routers or services.

```
router.py → service.py → repository.py → psycopg2
```

### Dependency injection (FastAPI)

Use `Depends()` for all cross-cutting concerns. Never import `get_db` directly in route handlers.

```python
@router.get("/status")
def get_status(
    auth: AuthDep,   # verify_api_key
    conn: DbDep,     # get_db
) -> PipelineStatusResponse:
    ...
```

---

## 6. Adding a New Data Domain

### Step 1 — Generator

Add `datasets/generators/domains/<domain>.py` implementing `BaseGenerator`.
Register in `datasets/generators/registry.py`.

### Step 2 — Schema

Add `datasets/schemas/<domain>.json` (JSON Schema draft-07).
Add the Spark `StructType` to `spark/schemas/bronze_schemas.py`.

### Step 3 — Bronze metadata

Add the domain to `src/processing/bronze/domain_meta.py`:
- `DOMAIN_PKS` — primary key column
- `DOMAIN_FORMATS` — csv / json / parquet
- `DOMAIN_ZORDER` — columns to Z-ORDER on
- `BATCH_INGESTION_ORDER` — dependency group (0–3)

### Step 4 — Silver processor

Create `src/processing/silver/domains/<domain>.py` extending `BaseSilverProcessor`.
Implement `domain_transform(df)` for business logic enrichment.
Register in `silver/process_silver.py:_PROCESSOR_REGISTRY`.

### Step 5 — Gold builder (if applicable)

For a new dim, add to `src/processing/gold/dimensions/`.
For a new fact, add to `src/processing/gold/facts/`.
Register in `spark/jobs/gold/build_gold.py`.

### Step 6 — DQ config

Add `DomainCheckConfig` entries to:
- `src/quality/domain_config.py:BRONZE_CONFIGS`
- `src/quality/domain_config.py:SILVER_CONFIGS`
- `src/quality/domain_config.py:GOLD_CONFIGS` (if Gold table)

### Step 7 — Tests

Add to:
- `tests/unit/test_silver_domain_meta.py` — metadata consistency
- `tests/pipeline/test_bronze_pipeline.py` — Bronze write/read
- `tests/unit/test_dq_checks.py` — config invariants

---

## 7. Adding a New Data Quality Check

1. Create `src/quality/checks/<check_name>.py` extending `BaseQualityCheck`.
2. Implement `run(self, df, domain, layer) -> CheckResult`.
3. Return a `CheckResult` with `CheckStatus.PASSED / FAILED / WARNING / ERROR`.
4. Add to the relevant engine(s) in `src/quality/engines/`.
5. Add unit tests to `tests/unit/test_dq_checks.py`.
6. Add a PySpark integration test to `tests/pipeline/test_dq_pipeline.py`.

Example skeleton:

```python
from src.quality.checks.base_check import BaseQualityCheck, CheckResult, CheckStatus
from pyspark.sql import DataFrame

class MyNewCheck(BaseQualityCheck):
    def __init__(self, threshold: float) -> None:
        self._threshold = threshold

    def run(self, df: DataFrame, domain: str, layer: str) -> CheckResult:
        # … compute metric …
        metric = 0.0
        status = CheckStatus.PASSED if metric >= self._threshold else CheckStatus.FAILED
        return CheckResult(
            domain=domain, layer=layer,
            check_name="my_new_check",
            status=status,
            rows_checked=df.count(),
            message=f"metric={metric:.4f}, threshold={self._threshold}",
            metric_value=metric,
            threshold=self._threshold,
        )
```

---

## 8. Adding a New API Endpoint

1. Add the Pydantic response model to `api/models/`.
2. Add the SQL query to `api/repositories/`.
3. Add the business logic method to `api/services/`.
4. Add the route to the appropriate `api/routers/<router>.py`.
5. Add unit tests to `tests/unit/test_api_endpoints.py` using `app.dependency_overrides`.
6. Add integration tests to `tests/integration/test_api_integration.py`.

Always use `AuthDep` and `DbDep` as parameters. Never accept raw credentials or raw connections from the client.

---

## 9. Git Workflow

This project follows **trunk-based development** with short-lived feature branches.

```
main ──────────────────────────────── (protected, always deployable)
  │
  ├── feature/add-returns-domain  ── merge in < 3 days
  ├── fix/gold-surrogate-key-null
  └── chore/bump-delta-lake-322
```

### Branch naming

| Type | Pattern | Example |
|---|---|---|
| Feature | `feature/<slug>` | `feature/add-reviews-gold-builder` |
| Bug fix | `fix/<slug>` | `fix/silver-scd2-null-hash` |
| Chore | `chore/<slug>` | `chore/bump-pyspark-355` |
| Hotfix | `hotfix/<slug>` | `hotfix/prod-api-key-expiry` |

### Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

[body — if non-obvious why, not what]

[footer — e.g. Closes #123]
```

Types: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `chore`, `ci`

Examples:

```
feat(silver): add age_bucket derivation for customers domain
fix(dq): prevent RowCountCheck Z-score with < 7 history points
chore: bump delta-spark to 3.2.1
docs: add data dictionary for finance domain
```

---

## 10. Pull Request Process

1. **Self-review** your diff before requesting review.
2. **CI must pass** — lint, typecheck, unit tests, Docker build.
3. **Coverage must not decrease** — new code requires tests.
4. **One reviewer** minimum for application code; two for security-sensitive changes (auth, secrets).
5. **Squash merge** for small fixes; merge commit for features.
6. **Delete the branch** after merge.

### PR description template

```markdown
## What
One sentence summary.

## Why
Business or technical motivation.

## How
Key implementation decisions. What did you consider and reject?

## Test plan
- [ ] Unit tests added / updated
- [ ] Pipeline tests pass locally
- [ ] Manual smoke test (describe steps)
- [ ] Integration tests pass against docker-compose stack

## Checklist
- [ ] No `.env` or credentials committed
- [ ] `make lint && make typecheck` pass
- [ ] Coverage ≥ 90%
- [ ] ARCHITECTURE.md updated if design changed
- [ ] DATA_DICTIONARY.md updated if schema changed
```

---

## 11. Environment Configuration

All configuration is handled by `Settings` in `src/common/config/settings.py`.
The class reads from environment variables (or `.env` file in dev).

### Adding a new config value

```python
# In src/common/config/settings.py
class Settings(BaseSettings):
    my_new_setting: str = Field(default="default-value", alias="MY_NEW_SETTING")
```

```dotenv
# In .env.example
MY_NEW_SETTING=default-value   # Description of what this does
```

### Per-environment overrides

| Environment | How |
|---|---|
| Local dev | `.env` file |
| CI | GitHub Actions secrets → `env:` block |
| Staging / Prod | AWS Secrets Manager + ECS task environment |

The `ENVIRONMENT` variable (`dev` / `staging` / `prod`) is available in `settings.environment`
and can be used for environment-specific branching, but prefer separate config values over
`if settings.environment == "prod"` checks in business logic.
