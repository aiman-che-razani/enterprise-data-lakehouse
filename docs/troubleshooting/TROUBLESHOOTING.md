# Troubleshooting Runbook

Operational runbook for the RetailNexus Data Lakehouse platform.  
Organised by symptom. If an issue is not covered here, check the [Grafana dashboard](http://localhost:3000/d/retailnexus-pipeline-health) and [Kibana logs](http://localhost:5601) first.

---

## Table of Contents

1. [Local Stack Won't Start](#1-local-stack-wont-start)
2. [Airflow DAG Failures](#2-airflow-dag-failures)
3. [Spark Job Failures](#3-spark-job-failures)
4. [Data Quality Failures](#4-data-quality-failures)
5. [API Issues](#5-api-issues)
6. [Delta Lake Issues](#6-delta-lake-issues)
7. [Kafka / Streaming Issues](#7-kafka--streaming-issues)
8. [PostgreSQL Issues](#8-postgresql-issues)
9. [Warehouse Load Failures](#9-warehouse-load-failures)
10. [Monitoring Alerts](#10-monitoring-alerts)
11. [CI/CD Failures](#11-cicd-failures)

---

## 1. Local Stack Won't Start

### Symptom: `make up` hangs or exits with errors

**Check Docker resources first:**
```bash
docker stats --no-stream
# If memory > 14 GB or CPU > 90%, increase Docker Desktop limits to 8 CPU / 16 GB
```

**Port conflicts:**
```bash
# Check which process holds a port
netstat -ano | findstr :5432   # Windows
lsof -i :5432                  # macOS/Linux
```

Common port conflicts:

| Port | Service | Fix |
|---|---|---|
| 5432 | PostgreSQL | Stop local postgres: `sudo service postgresql stop` |
| 9092 | Kafka | Stop local Kafka |
| 8080 | Airflow | Stop any local Airflow or web server on 8080 |
| 3000 | Grafana | Stop any local service on 3000 |

**Rebuild images after code changes:**
```bash
docker compose build --no-cache api
docker compose up -d api
```

**Full reset (loses all data):**
```bash
make down-clean
make up
```

---

### Symptom: Airflow init fails with `permission denied for schema public`, or a DAG fails with `column ... does not exist` in `pipeline.*`

A reused `postgres_data` volume predates the current `scripts/setup/init_postgres.sql`. Postgres 15+ no longer lets a
role create objects in `public` unless it owns the database, and `CREATE TABLE IF NOT EXISTS` never upgrades an
existing table, so an old volume keeps its old schema for ever.

```bash
# the airflow database must be owned by the airflow role
docker compose exec postgres psql -U lakehouse -c 'ALTER DATABASE airflow OWNER TO airflow;'
# metadata schemas: recreate them from the current script (dev data only: watermarks, runs, DQ reports)
docker compose exec postgres psql -U lakehouse -d lakehouse -c 'DROP SCHEMA pipeline, quality, catalog, api CASCADE'
docker compose exec -T postgres psql -U lakehouse -d lakehouse < scripts/setup/init_postgres.sql
docker compose exec -T postgres psql -U lakehouse -d lakehouse < scripts/setup/seed_dev_data.sql
```

On a port clash with a locally installed Postgres, set `POSTGRES_HOST_PORT=15432` in `.env`.

### Symptom: MinIO init fails — bucket not created

```bash
# Check MinIO logs
docker compose logs minio-init

# Re-run MinIO init manually
docker compose run --rm minio-init
```

---

### Symptom: Airflow webserver shows "No module named X"

The Airflow image may be stale. Rebuild:
```bash
docker compose build --no-cache airflow-webserver
docker compose restart airflow-webserver airflow-scheduler
```

---

## 2. Airflow DAG Failures

### Symptom: DAG Import Error in Airflow UI

```bash
# Check DAG syntax locally
python airflow/dags/<dag_file>.py

# Check Airflow scheduler logs
docker compose logs airflow-scheduler | grep ERROR
```

Common causes:
- Missing import (module not in `requirements.txt` / Docker image)
- Invalid Airflow connection (check `Admin → Connections`)
- Environment variable not set in Airflow container

**Fix missing connection:**
```bash
# Re-run bootstrap
python scripts/airflow/bootstrap_airflow.py --connections-only
```

---

### Symptom: Spark job sits at "Initial job has not accepted any resources"

The job asks for executors the cluster cannot place. The dev worker has 2 cores and 4 GB (`SPARK_WORKER_CORES`,
`SPARK_WORKER_MEMORY`); DAG executor sizing comes from `SPARK_EXECUTOR_MEMORY` (default 4g) and
`SPARK_EXECUTOR_CORES` (default 2) in the Airflow containers. Raise the worker and the executor together, never the
executor alone. Check `http://localhost:18080` for the application and the worker's free cores / memory.

### Symptom: `io problem while parsing ivy file` / `unresolved dependency: io.delta`

Parallel `spark-submit --packages` runs raced on a shared Ivy cache. The Airflow image pre-fetches the Delta and
S3A packages at build time (`docker/airflow/Dockerfile`), so after `docker compose build airflow-init` this cannot
happen; if you change the package list in the DAGs, change it there too.

### Symptom: SparkSubmitOperator times out

Default timeout is 4 hours. For large backfills:

1. Navigate to Airflow UI → Admin → Variables
2. Update `spark_job_timeout` to `28800` (8 hours)
3. Or pass `execution_timeout=timedelta(hours=8)` in the DAG directly

Check Spark job progress in the Spark UI (http://localhost:8081).

---

### Symptom: WatermarkSensor never returns True

```bash
# Check what watermark is in the DB
psql "$DATABASE_URL" -c "SELECT * FROM pipeline.watermarks WHERE domain = 'orders';"
```

If the watermark is missing, the upstream ingest never completed. Check:
```bash
# Bronze ingest logs for the domain
docker compose logs spark-worker | grep "orders" | tail -50
```

If the Bronze job succeeded but no watermark was written, check for a `psycopg2` connection error in the Bronze ingestor logs.

---

### Symptom: SLA miss email/alert but pipeline looks healthy

The SLA monitoring DAG checks watermarks every 30 minutes. An SLA miss is fired when:
- `NOW() - bronze_last_ingest > sla_hours` for that domain

If the data is fresh but the alert fired, the Bronze ingest timestamp may not have been updated:
```bash
psql "$DATABASE_URL" -c \
  "SELECT domain, bronze_last_ingest, NOW() - bronze_last_ingest AS age
   FROM pipeline.watermarks
   ORDER BY age DESC NULLS FIRST;"
```

---

## 3. Spark Job Failures

### Symptom: `java.lang.OutOfMemoryError: Java heap space`

```bash
# Check worker memory in spark-defaults.conf
grep "executor.memory" configs/spark/spark-defaults.conf
```

Quick fixes (in `spark-defaults.conf` or as spark-submit args):
```
spark.executor.memory=4g          # increase if jobs are large
spark.driver.memory=2g
spark.sql.shuffle.partitions=200  # reduce if data is small
```

For the local Docker stack, don't exceed 60% of total available RAM for Spark.

---

### Symptom: `AnalysisException: Path does not exist` on Delta read

The Bronze table for this domain has not been written yet. Run ingestion first:
```bash
make bronze-ingest DOMAIN=orders
```

Or check that MinIO has the data:
```bash
docker compose exec minio mc ls retailnexus/lakehouse/bronze/orders/
```

---

### Symptom: `DeltaAnalysisException: Concurrent update` / writer conflict

Delta Lake ACID prevents concurrent writes to the same table. If two jobs attempted to write simultaneously:

1. Identify the conflicting jobs in the Spark UI
2. Kill the stale job
3. Retry the failed job — Delta's optimistic concurrency control will resolve it

To prevent this: DAGs use `max_active_runs=1` for all pipeline DAGs. Check that this is not overridden.

---

### Symptom: Spark job exits with code 137

OOM kill. The executor was killed by the OS.

```bash
# Check Docker stats during the run
docker stats spark-worker

# Reduce parallelism in spark-defaults.conf or use --conf on the job
spark.executor.cores=2   # fewer cores per executor → more executors with less memory
```

---

### Symptom: S3A `Connection refused` to MinIO

```bash
# Verify MinIO is running
docker compose ps minio

# Check S3A endpoint configuration
grep "endpoint" configs/spark/spark-defaults.conf
# Should be: spark.hadoop.fs.s3a.endpoint=http://minio:9000
```

If running Spark outside Docker, use `http://localhost:9000` instead of `http://minio:9000`.

---

## 4. Data Quality Failures

### Symptom: DQ run exits with code 1 (failures) or 3 (critical + fail-fast)

```bash
# Check the latest DQ results
psql "$DATABASE_URL" -c \
  "SELECT domain, layer, check_name, status, message, run_at
   FROM quality.check_results
   WHERE status IN ('failed','error')
   ORDER BY run_at DESC
   LIMIT 20;"
```

### Symptom: `table.load` ERROR on a domain

The Delta table could not be read. Check:
1. Does the table exist in MinIO?
2. Does the Silver/Bronze job for this domain succeed?
3. Is there a schema mismatch (use `DESCRIBE DETAIL`)?

```bash
# Delta table health check via Spark shell
spark.read.format("delta").load("s3a://retailnexus-lakehouse/silver/orders/") \
  .count()
```

### Symptom: `freshness` FAILED — stale data alert

```bash
# Check when the last Bronze ingest completed
psql "$DATABASE_URL" -c \
  "SELECT domain, bronze_last_ingest,
          EXTRACT(EPOCH FROM (NOW() - bronze_last_ingest))/3600 AS age_hours
   FROM pipeline.watermarks
   WHERE domain = 'orders';"
```

If the Bronze job ran but the timestamp wasn't updated, check the ingestor's `update_watermark()` call. A DB connection failure during the watermark update would cause this.

### Symptom: `uniqueness` FAILED — unexpected duplicates

```python
# Investigate via PySpark
df = spark.read.format("delta").load("s3a://retailnexus-lakehouse/bronze/orders/")
from pyspark.sql import functions as F
dupes = df.groupBy("order_id").count().filter(F.col("count") > 1)
dupes.show(20)
```

Duplicates in Bronze are normal if the same source file was re-ingested. Check `_source_file` to confirm:
```python
df.filter(F.col("order_id") == "ORD-DUPLICATE").select("_source_file", "_batch_id").show()
```

---

## 5. API Issues

### Symptom: `401 Unauthorized` from the API

```bash
# Confirm the API key is active in the DB
psql "$DATABASE_URL" -c \
  "SELECT client_name, is_active, expires_at
   FROM api.api_keys
   WHERE key_hash = encode(sha256('<your-plaintext-key>'::bytea), 'hex');"
```

If the key is expired, regenerate:
```bash
# Generate a new key and insert
python -c "
import secrets, hashlib
key = secrets.token_hex(32)
h   = hashlib.sha256(key.encode()).hexdigest()
print(f'Key: {key}')
print(f'Hash: {h}')
print(f'SQL: INSERT INTO api.api_keys (key_hash, client_name, permissions) VALUES (\'{h}\', \'my-client\', \'[\"read\"]\');')
"
```

### Symptom: API returns `503 Service Unavailable` on `/health/ready`

The readiness check (`SELECT 1` against PostgreSQL) is failing. Check:
```bash
docker compose ps postgres
docker compose logs postgres | tail -30
```

If PostgreSQL is healthy but the API can't connect, check the `DATABASE_URL` environment variable in the API container:
```bash
docker compose exec api env | grep POSTGRES
```

### Symptom: API latency p99 > 2 seconds

Check the Grafana dashboard for:
1. Slow SQL queries (add `EXPLAIN ANALYZE` to the suspect query)
2. Connection pool exhaustion (increase `maxconn` in `api/dependencies.py`)
3. Postgres lock contention (`SELECT * FROM pg_locks;`)

---

## 6. Delta Lake Issues

### Symptom: Delta table is corrupted / `_delta_log` missing entries

Do not delete `_delta_log/`. Use Delta's repair command:

```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3a://retailnexus-lakehouse/bronze/orders/")
dt.generate("symlink_format_manifest")  # optional, for external readers
```

If the log is genuinely corrupted (e.g. partial S3 write):
```python
# Roll back to the last healthy version
dt.restoreToVersion(42)  # check dt.history() for the last good version
```

### Symptom: Delta table has too many small files (slow reads)

```bash
# Run OPTIMIZE manually
make bronze-optimize DOMAIN=orders

# Or via spark-submit
spark-submit spark/jobs/bronze/optimize_bronze.py --domain orders
```

The maintenance DAG (`pipeline_maintenance_dag`) runs OPTIMIZE + VACUUM every Sunday at 01:00 UTC. If small files accumulate between runs, reduce the OPTIMIZE interval in `pipeline_maintenance_dag.py`.

### Symptom: VACUUM removes files that active queries need

Delta Lake protects files referenced by active transactions via the `deletedFileRetentionDuration` setting (default: 7 days). If a query running for > 7 days touches files that VACUUM deleted, it will fail.

Mitigation: increase the retention duration in `spark-defaults.conf`:
```
spark.databricks.delta.retentionDurationCheck.enabled=false
delta.deletedFileRetentionDuration=interval 30 days
```

---

## 7. Kafka / Streaming Issues

### Symptom: Spark Structured Streaming job won't start — `Unable to connect to broker`

```bash
# Check Kafka health
docker compose ps kafka
docker compose logs kafka | tail -30

# Verify topic exists
docker compose exec kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

If the topic doesn't exist, re-run:
```bash
bash scripts/setup/create_kafka_topics.sh
```

### Symptom: Kafka consumer lag is growing (> 100K messages)

Check the Grafana dashboard for the `KafkaConsumerLag` metric.

```bash
# Check lag per topic
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group spark-clickstream-consumer
```

Causes:
1. Spark streaming job is stopped — restart it via Airflow (`bronze_streaming_dag`)
2. Spark job is running but slow — check Spark UI for backpressure
3. Kafka topic has too many partitions per executor — increase `maxOffsetsPerTrigger`

### Symptom: `StreamingQueryException: Checkpoint corrupted`

The checkpoint directory at `s3://retailnexus-lakehouse/checkpoints/<topic>/` is incomplete.

Safe recovery:
1. Stop the streaming job
2. Delete the checkpoint directory for that stream
3. Restart — the stream will replay from the last committed Kafka offset

**Note:** Deleting the checkpoint means any data processed after the last Delta commit but before the checkpoint write may be reprocessed (at-least-once window). Delta Lake ACID idempotent MERGE prevents duplicates in the final table.

---

## 8. PostgreSQL Issues

### Symptom: `too many connections` error

The default `max_connections=100` may be insufficient under load.

```bash
# Check current connection count
psql "$DATABASE_URL" -c \
  "SELECT count(*) FROM pg_stat_activity;"

# See connections by application
psql "$DATABASE_URL" -c \
  "SELECT application_name, count(*) FROM pg_stat_activity GROUP BY 1 ORDER BY 2 DESC;"
```

Solutions:
1. Add a PgBouncer connection pooler between the application and PostgreSQL
2. Reduce `ThreadedConnectionPool` maxconn in `api/dependencies.py`
3. Reduce Airflow database connection concurrency via `AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE`

### Symptom: Slow watermark queries in SLA monitoring DAG

Add an index if missing:
```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_watermarks_domain
  ON pipeline.watermarks (domain);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_check_results_run_at
  ON quality.check_results (run_at DESC, domain, layer);
```

The weekly maintenance DAG runs `VACUUM ANALYZE` on all metadata tables automatically.

---

## 9. Warehouse Load Failures

### Symptom: Redshift COPY fails — `Load into table <X> failed`

```bash
# Check Redshift STL_LOAD_ERRORS
SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 10;
```

Common causes:
- Data type mismatch (e.g. a string that's too long for `VARCHAR(255)`)
- Null in a NOT NULL column
- Invalid UTF-8 characters

Fix: inspect the source Gold Delta table for the offending rows, fix upstream Silver transformation, rebuild Gold, retry load.

### Symptom: Snowflake COPY fails — `Number of columns in file does not match`

The Gold Delta schema changed but the Snowflake DDL was not updated. Apply a migration:
```sql
-- warehouse/snowflake/migrations/V002__add_new_column.sql
ALTER TABLE gold.fact_sales ADD COLUMN new_col NUMBER;
```

Then re-run Flyway and the warehouse load.

### Symptom: BigQuery load fails — `Cannot convert X to INT64`

BigQuery does not auto-cast `LongType` to `INT64` for non-standard Spark connectors. Check:
```python
# bigquery_loader.py ensures all SK cols are LongType before write
df = df.withColumn("sk_customer_id", F.col("sk_customer_id").cast("long"))
```

If a new SK column was added without a `.cast("long")`, add it to `BigQueryLoader._cast_sk_cols()`.

---

## 10. Monitoring Alerts

Every alert in `monitoring/prometheus/rules/` links here through its `runbook` annotation. Each entry
below has an explicit anchor (`#alert-<lowercase alert name>`). Severity `critical` pages on-call;
`warning` goes to Slack.

<a id="alert-bronzefreshnessbreachcritical"></a>
### Alert: `BronzeFreshnessBreachCritical` (clickstream or iot_sensors)

SLA = 2 hours. This is a P1 — page on-call.

1. Check if the streaming Spark job is running: Airflow → `bronze_streaming_health`
2. Check Kafka consumer lag for the topic
3. Check Spark job logs for errors
4. If Spark job died, restart it from the Airflow UI

**Escalation:** If the streaming job cannot be restarted within 30 minutes, trigger a batch backfill:
```bash
# Airflow trigger via CLI
airflow dags trigger pipeline_backfill \
  --conf '{"layer":"bronze","domains":"clickstream","start_date":"2025-06-01","end_date":"2025-06-01"}'
```

<a id="alert-bronzefreshnessbreachwarning"></a>
### Alert: `BronzeFreshnessBreachWarning` (daily domains, SLA 26h)

1. Open the last `bronze_batch_ingestion` run; find the failed or missing domain task
2. Read the Spark task log; the usual causes are a missing source file, a schema-enforcement reject
   burst (check the Bronze DLQ table) or S3 connectivity
3. Re-run the failed task from the Airflow UI once fixed

<a id="alert-bronzefreshnessbreachanalytical"></a>
### Alert: `BronzeFreshnessBreachAnalytical` (employees, returns, reviews, finance, campaigns; SLA 48h)

Same steps as the warning above; these domains have a looser SLA, so this is rarely urgent.
Check whether the source system delivered a file at all before debugging Spark.

<a id="alert-dqpassratecritical"></a>
### Alert: `DQPassRateCritical` (layer pass rate < 75%)

1. Query `quality.check_results` for FAILED checks (see Section 4)
2. Identify the root cause (upstream schema change, data corruption, referential integrity break)
3. If all failures are in one domain: pause that domain's pipeline, investigate, fix at source
4. If failures span multiple domains: escalate — likely a shared infrastructure issue

Per-domain versions (`DomainDQPassRateCritical`, `DomainDQPassRateLow`) name the layer and domain;
start from that pair on the *RetailNexus Data Quality* Grafana dashboard.

<a id="alert-dqpassratewarning"></a>
### Alert: `DQPassRateWarning` (layer pass rate < 90%)

Open the *Data Quality* dashboard, find the domains under 90% in that layer, and read their failing
checks (`GET /quality/checks?layer=<layer>&status=failed`). Fix before it drops below 75%.

<a id="alert-domaindqpassratelow"></a>
### Alert: `DomainDQPassRateLow` (domain pass rate < 90%)

Same as `DQPassRateWarning`, scoped to the `layer/domain` in the alert.

<a id="alert-domaindqpassratecritical"></a>
### Alert: `DomainDQPassRateCritical` (domain pass rate < 75%)

Same as `DQPassRateCritical`, scoped to the `layer/domain`. Downstream layers may be reading bad
data: check whether the Silver/Gold quality gates (`silver_dq_gate`, `gold_dq_gate`) blocked the run.

<a id="alert-dqmetricsstale"></a>
### Alert: `DQMetricsStale` (no DQ reports for 26h)

The daily `data_quality` DAG, or the API's metrics refresh, has stopped. Check the DAG run, then
`SELECT max(run_at) FROM quality.reports;` and the API log for `pipeline_metrics_refresh_failed`.

<a id="alert-slamisseselevated"></a>
### Alert: `SLAMissesElevated` (more than 5 in 24h)

`GET /pipeline/sla-misses?hours=24` lists them with domain, layer and the measured value. Look for one
domain repeating (a stuck source) versus many domains (cluster or scheduler trouble).

<a id="alert-slamissescritical"></a>
### Alert: `SLAMissesCritical` (more than 20 in 24h)

Treat as a platform incident: check Airflow scheduler health, Spark master/workers, Postgres, and
object storage before individual domains.

<a id="alert-apihigherrorrate"></a>
### Alert: `APIHighErrorRate` (5xx above 5%)

1. `GET /health/ready` — a 503 means the database is unreachable (see Section 5)
2. Search the API log for `unhandled_exception` and `db_pool_unavailable`
3. If it started with a deploy, roll back the ECS service (Section 11)

<a id="alert-apihighlatency"></a>
### Alert: `APIHighLatency` (p99 above 2s)

See Section 5 ("API latency p99 > 2 seconds"): slow queries in `pg_stat_activity`, connection-pool
exhaustion (`maxconn=20`), unbounded list requests.

<a id="alert-apidown"></a>
### Alert: `APIDown`

1. Check ECS task health: `aws ecs describe-services --cluster retailnexus-prod --services retailnexus-api`
2. Check CloudWatch logs for the crashed task
3. If OOM: increase Fargate task memory in Terraform and redeploy
4. If DB connection failure: check RDS Multi-AZ failover status

<a id="alert-sparkmasterdown"></a>
### Alert: `SparkMasterDown`

For local Docker: `docker compose restart spark-master spark-worker`
For EMR Serverless in prod: check the EMR console; serverless applications auto-recover.

<a id="alert-sparkworkerlost"></a>
### Alert: `SparkWorkerLost`

Local: `docker compose ps spark-worker`; restart it and confirm it registers with `spark://spark-master:7077`.
Prod (EMR Serverless): check the application's worker capacity and the job's maximum-capacity limit.

<a id="alert-kafkaconsumerlag"></a>
### Alert: `KafkaConsumerLag`

See Section 7 ("Kafka consumer lag is growing"). Confirm the Structured Streaming query is active and
not blocked on a corrupted checkpoint.

<a id="alert-kafkabrokerdown"></a>
### Alert: `KafkaBrokerDown`

`docker compose ps kafka zookeeper`; check `docker compose logs kafka`. Zookeeper must be healthy first. Once the
broker is back, confirm `kafka_brokers` returns to 1 and the consumer lag alert clears.

<a id="alert-miniounhealthy"></a>
### Alert: `MinIOUnhealthy`

`docker compose logs minio` for drive or permission errors (the Bitnami image runs as uid 1001 and needs a volume it
can write). Delta reads and writes fail while this is firing, so pause the pipeline DAGs until it clears.

<a id="alert-airflowschedulersilent"></a>
### Alert: `AirflowSchedulerSilent`

Confirm `airflow-scheduler` is running (`docker compose ps`), then that `statsd-exporter` is up and Prometheus's
`airflow` target is healthy. If the scheduler is fine but the metric is absent, check `AIRFLOW__METRICS__STATSD_*`.

<a id="alert-miniolowspace"></a>
### Alert: `MinIOLowSpace`

Run the Delta maintenance DAG (`pipeline_maintenance`: OPTIMIZE + VACUUM). Bronze is retained for
7 years by design, so do not shorten its VACUUM window to reclaim space — extend the volume instead.

---

## 11. CI/CD Failures

### Symptom: `ruff` or `mypy` fail in CI but pass locally

Your local tool versions may differ from CI. Pin versions match `pyproject.toml`:
```bash
pip install "ruff==0.4.7" "mypy==1.10.0" --upgrade
```

Or use the pre-commit hooks which use the same pinned versions:
```bash
pre-commit run --all-files
```

### Symptom: Docker build fails in CI — `npm ERR!` or `layer not found`

Usually a transient registry issue. Re-run the CI workflow from GitHub Actions UI.
If persistent, check ECR authentication:
```bash
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS \
    --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com
```

### Symptom: `pytest` times out in CI

Integration or pipeline tests should not run in CI without a docker-compose stack.
Verify the CI workflow only runs unit tests:
```yaml
# In ci.yml
- run: pytest tests/unit/ -m unit --tb=short -q
```

If pipeline tests were accidentally included, add the `-m unit` marker filter.

### Symptom: Deployment fails — `Service did not stabilize`

ECS deployment timed out waiting for tasks to reach RUNNING state. Check:
```bash
aws ecs describe-services \
  --cluster retailnexus-prod \
  --services retailnexus-api \
  --query 'services[0].events[:5]'
```

Usual cause: the new task version fails its health check (`GET /health/ready → 503`).
Check the DB connection — the most common reason for a failed readiness check.
Roll back immediately using the procedure in [DEPLOYMENT.md](../deployment/DEPLOYMENT.md#12-rollback-procedure).
