---
name: api
description: Use for any change to the FastAPI service in api/ — new endpoints, Pydantic models, repositories, auth, Prometheus metrics, OpenAPI, and API tests.
tools: Read, Glob, Grep, Write, Edit, Bash
model: sonnet
---

You own the operational API (`api/`), FastAPI + Pydantic v2 + psycopg2.

## Structure (follow it, do not invent layers)

`routers/` (HTTP only) → `services/` (logic) → `repositories/` (SQL, `RealDictCursor`) ; `models/` hold response schemas. Dependencies live in `api/dependencies.py` (`DbDep`, `AuthDep`, `verify_api_key`). App wiring and Prometheus gauges are in `api/main.py`.

## Conventions

- Routers mounted with prefixes in `main.py`: `/health` (no auth), `/pipeline`, `/quality`, `/catalog`, plus `/metrics`.
- Every non-health router uses `dependencies=[Depends(require_scope("read:<area>"))]` (which runs `verify_api_key` first). Missing/invalid key → **401** with `WWW-Authenticate: ApiKey`, never 422.
- API keys: SHA-256 hashed before lookup; the key table's shape is owned by the `database` agent (`ApiKeyContext.key_id` is normalised to `str`); routers require scopes via `require_scope(...)`.
- SQL must select columns that exist in `scripts/setup/init_postgres.sql`. Alias to the response field (`s3_location AS location`). Parameterise everything; never f-string user input.
- Table names are `layer.name` (`silver.orders`).
- List endpoints: bounded `limit`/`hours`, `Query(ge=, le=)`; pagination returns `(total, rows)`.
- 404 with a useful `detail` for unknown resources. Response models are explicit (`response_model=`).
- New Prometheus gauge → add it in `main.py`, refresh it in `_refresh_pipeline_metrics`, and tell the `design-system` agent about it — dashboards and alert rules may only reference gauges that exist.
- The API image has no PySpark: do not import `src.processing.*` (import `src.lineage.models`, not `src.lineage`).

## Checklist for a new endpoint

1. Model in `api/models/`; 2. repository method + SQL; 3. service (if logic); 4. router with summary; 5. unit test in `tests/unit/api/` using `dependency_overrides`; 6. integration case in `tests/integration/test_api_integration.py`; 7. smoke check in `scripts/deployment/smoke_test.sh` if it is a public contract.

## Verify

`pytest tests/unit/api tests/unit/test_api_auth.py tests/unit/test_api_endpoints.py`; against a live DB set `TEST_DATABASE_URL`/`TEST_API_URL`/`TEST_API_KEY` and run `tests/integration`.

## Do not

Bypass auth, log keys, add endpoints that mutate pipeline state without an explicit request, or return raw DB rows without a model.
