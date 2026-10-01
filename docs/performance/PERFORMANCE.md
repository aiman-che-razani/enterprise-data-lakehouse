# Performance baseline

Measured on a developer machine (Windows 11, Docker Desktop, 16 logical CPUs / 16 GB), **not** on production
hardware. These are baselines and regression guards, not capacity guarantees. Date of measurement: 2026-10-01.

## What was measured, and how

| Criterion (ROADMAP) | Result | Status |
|---|---|---|
| `generate-data` produces ≥ 1 M records | **1,439,917 records** at `--scale-factor 0.1`, 423 MB on disk, 6.4 min | met |
| ≥ 80 quality checks defined and passing against generated data | **528 checks** defined (`python -m src.quality.catalog`); Silver 190 / Gold 151 / Bronze 187 | met (see the DQ note below) |
| API p99 < 200 ms under 100 RPS | **p99 26 ms, p50 7 ms, 0 errors** at 100 RPS for 30 s (single container, single worker) | met, with no headroom |
| `COPY` from S3 < 30 min for the full dataset | needs a Redshift / Snowflake / BigQuery account | **not measured** |
| Analytical SQL scripts < 60 s | needs a loaded warehouse | **not measured** |
| Pipeline wall-clock (ARCHITECTURE: ~9 h at full scale) | see the stage table; full scale (≈ 14 M records) not run | **not measured at full scale** |

## Pipeline stages at 1.44 M records (scale 0.1)

Spark standalone, one worker with 8 cores / 12 GB (`SPARK_WORKER_CORES=8`, `SPARK_WORKER_MEMORY=12g`), executors
sized 4 GB / 2 cores (`SPARK_EXECUTOR_MEMORY`, `SPARK_EXECUTOR_CORES`), Airflow `LocalExecutor`, MinIO as object store.
Each task is a separate `spark-submit`, so every task pays a JVM + application start (roughly 20–40 s here); that
fixed cost dominates the small domains.

| Stage (DAG) | Tasks | Wall-clock |
|---|---|---|
| Bronze ingest + OPTIMIZE (`bronze_batch_ingestion`) | 35 | 32.5 min |
| Silver (`silver_batch`) | 19 | 16.6 min |
| Gold (`gold_batch`, includes the Silver DQ gate) | 18 | 14.6 min |
| Data quality, all layers (`data_quality`) | 6 | Silver 1.7 min, Gold 3.0 min, Bronze 4.2 min |

Row counts reconcile across layers (e.g. `fact_sales` 256,254 = `silver.order_lines` 256,254 = generator output).
Warehouse loads were not run (no cloud credentials).

Extrapolation to full scale is **not** reliable: the large domains (clickstream 500 k, order lines 256 k at this
scale) scale with data, the small ones do not, and the single worker is the bottleneck. Treat the full-scale
estimate in ARCHITECTURE.md as unverified until it is run on a real cluster.

## API load test

`scripts/performance/api_load_test.py` is an open-loop generator (fixed offered rate, so queueing shows up as
latency instead of silently lowering the load). It discovers the parameterless GET endpoints from `/openapi.json`
(17 endpoints, round-robin) and exits non-zero if p99 or the error budget is missed.

```bash
# run the target with API_RATE_LIMIT_PER_MINUTE=0, or one key at 100 RPS is throttled with 429s
python scripts/performance/api_load_test.py --base-url http://fastapi:8000 \
    --api-key dev-api-key-retailnexus-2026 --rps 100 --duration 30
```

| Offered load (in the Docker network) | Outcome |
|---|---|
| 5 RPS | p50 9.8 ms, p99 18.5 ms |
| 100 RPS | p50 7.2 ms, p95 13.3 ms, p99 26.0 ms, no errors |
| 200 RPS | saturated: p50 12.7 s, 135 × 503 (connection pool exhausted) |
| 400 RPS | collapse |

Read this as: one API worker comfortably serves 100 RPS and saturates between 100 and 200 RPS, and **past
saturation it queues instead of shedding load**. Scale out (more tasks / `--workers`) rather than up, and put a
load balancer or WAF rate rule in front (see `api/ratelimit.py`: the in-process limiter is per task).

**Measure from inside the network.** The same test from a Windows host through Docker Desktop's port proxy showed
p50 22 s at "100 RPS" because the proxy, not the API, was the ceiling. A host-side number is not an API number.

## Bugs this measurement found

- The order generator rebuilt a list of all customers *for every order* (O(orders × customers)): scale 0.1 needed
  more than 7 minutes for half the orders, and full scale (1 M orders × 500 k customers) was infeasible. Orders at
  scale 0.1 now take 37 s.
- Bronze re-ingested the whole raw folder on every run, doubling every table each day (see TDR-006).

## DQ note

Silver and Gold quality runs end green on the generated data (Silver 173 / 173, Gold 139 / 139 checks at the time
of the run). Bronze reports two **legitimate** failures in this dev stack: the clickstream and IoT freshness SLA
is 2 h and nothing is streaming. Those should fail here; they are the checks working.
