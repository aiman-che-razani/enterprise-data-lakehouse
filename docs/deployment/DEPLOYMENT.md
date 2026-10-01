# Production Deployment Guide

This document covers deploying the RetailNexus Data Lakehouse to AWS production.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Infrastructure Provisioning (Terraform)](#3-infrastructure-provisioning-terraform)
4. [Secrets Configuration](#4-secrets-configuration)
5. [Container Image Build & Push](#5-container-image-build--push)
6. [Database Initialisation](#6-database-initialisation)
7. [Airflow MWAA Bootstrap](#7-airflow-mwaa-bootstrap)
8. [API Service Deployment (ECS Fargate)](#8-api-service-deployment-ecs-fargate)
9. [Monitoring Setup](#9-monitoring-setup)
10. [CI/CD Pipeline](#10-cicd-pipeline)
11. [Blue-Green Deployment](#11-blue-green-deployment)
12. [Rollback Procedure](#12-rollback-procedure)
13. [Post-Deployment Verification](#13-post-deployment-verification)

---

## 1. Architecture Overview

```
                                  ┌─────────────────────────────────────────────┐
                                  │                   AWS VPC                   │
                                  │                                             │
  GitHub Actions ──── ECR ───────►│  ECS Fargate (FastAPI)                     │
                                  │        │                                    │
                                  │        ▼                                    │
  MWAA (Airflow) ────────────────►│  RDS PostgreSQL 16 (Multi-AZ)             │
        │                         │                                             │
        ├── EMR Serverless ──────►│  S3 (Delta Lake)                          │
        │   (Spark Jobs)          │    bronze/ silver/ gold/                   │
        │                         │    checkpoints/ quality-reports/           │
        └── MSK (Kafka) ─────────►│                                            │
                                  │  Redshift Serverless (BI analytics)        │
                                  │  Secrets Manager (all credentials)         │
                                  │  CloudWatch + Managed Grafana              │
                                  └─────────────────────────────────────────────┘
```

**Key AWS services:**

| Component | AWS Service | Sizing |
|---|---|---|
| Spark jobs | EMR Serverless | 4–32 vCPU, auto-scale |
| Airflow | MWAA | `mw1.medium` (2 worker) |
| API | ECS Fargate | 1 vCPU / 2 GB, 2 tasks |
| Metadata DB | RDS PostgreSQL 16 | `db.t3.medium`, Multi-AZ |
| Message bus | MSK | `kafka.t3.small`, 3 brokers |
| Delta store | S3 | Standard-IA lifecycle after 90 days |
| Warehouse | Redshift Serverless | 128 RPU base |
| Secrets | Secrets Manager | — |
| Monitoring | CloudWatch + AMG | — |

---

## 2. Prerequisites

### Tools

```bash
terraform --version   # >= 1.8
aws --version         # >= 2.15
docker --version      # >= 25
gh --version          # GitHub CLI (for secrets)
```

### AWS Permissions

The deployment IAM role requires:

```json
{
  "Statement": [
    { "Effect": "Allow", "Action": ["ec2:*", "ecs:*", "iam:PassRole",
        "ecr:*", "rds:*", "mwaa:*", "emr-serverless:*",
        "kafka:*", "s3:*", "secretsmanager:*",
        "cloudwatch:*", "logs:*", "redshift-serverless:*"],
      "Resource": "*" }
  ]
}
```

> Use a dedicated `cicd-deployer` IAM role with the above; never use root credentials.

### GitHub Secrets (set via `gh secret set`)

```bash
# AWS credentials for GitHub Actions
gh secret set AWS_ACCESS_KEY_ID       --body "<access-key>"
gh secret set AWS_SECRET_ACCESS_KEY   --body "<secret-key>"
gh secret set AWS_DEFAULT_REGION      --body "us-east-1"
gh secret set ECR_REGISTRY            --body "<account-id>.dkr.ecr.us-east-1.amazonaws.com"

# Slack notifications
gh secret set SLACK_WEBHOOK_URL       --body "<webhook-url>"
```

---

## 3. Infrastructure Provisioning (Terraform)

### First-time setup

```bash
cd terraform/environments/prod

# Initialise the backend (S3 + DynamoDB state locking)
terraform init \
  -backend-config="bucket=retailnexus-terraform-state" \
  -backend-config="key=prod/lakehouse.tfstate" \
  -backend-config="region=us-east-1" \
  -backend-config="dynamodb_table=retailnexus-terraform-locks"

# Review the plan
terraform plan -var-file="prod.tfvars"

# Apply (creates VPC, RDS, MSK, EMR, MWAA, ECS cluster, ECR repos)
terraform apply -var-file="prod.tfvars"
```

### Key `prod.tfvars` values

```hcl
environment          = "prod"
aws_region           = "us-east-1"
vpc_cidr             = "10.0.0.0/16"
availability_zones   = ["us-east-1a", "us-east-1b", "us-east-1c"]
rds_instance_class   = "db.t3.medium"
rds_multi_az         = true
mwaa_environment_class = "mw1.medium"
mwaa_max_workers     = 10
emr_release_label    = "emr-7.0.0"
kafka_broker_count   = 3
kafka_instance_type  = "kafka.t3.small"
ecr_repo_names       = ["retailnexus-api", "retailnexus-spark"]
s3_lakehouse_bucket  = "retailnexus-lakehouse-prod"
```

### Incremental updates

```bash
terraform plan -var-file="prod.tfvars" -out=plan.tfplan
terraform apply plan.tfplan
```

---

## 4. Secrets Configuration

All credentials are stored in AWS Secrets Manager. **Never store credentials in code, environment variables, or CI/CD logs.**

### Create secrets (one-time)

```bash
# PostgreSQL credentials
aws secretsmanager create-secret \
  --name "retailnexus/prod/postgres" \
  --secret-string '{"host":"<rds-endpoint>","port":5432,"user":"lakehouse","password":"<strong-password>","dbname":"lakehouse"}'

# Kafka credentials
aws secretsmanager create-secret \
  --name "retailnexus/prod/kafka" \
  --secret-string '{"bootstrap_servers":"<msk-endpoint>:9096","sasl_username":"lakehouse","sasl_password":"<strong-password>"}'

# API secret key
aws secretsmanager create-secret \
  --name "retailnexus/prod/api_secret_key" \
  --secret-string '{"key":"<random-64-char-string>"}'

# Snowflake credentials
aws secretsmanager create-secret \
  --name "retailnexus/prod/snowflake" \
  --secret-string '{"account":"<account>","user":"<user>","password":"<password>","warehouse":"RETAILNEXUS_WH","database":"RETAILNEXUS","role":"RETAILNEXUS_ENGINEER"}'
```

### Application configuration

Set the following environment variable on all services:

```
SECRETS_BACKEND=aws_secrets_manager
```

Settings **refuse to start in production** with any development default. Provide real values for:

| Setting | Notes |
|---|---|
| `API_SECRET_KEY` | Rejected if it is the development default |
| `POSTGRES_PASSWORD` | Rejected if it is the development default |
| `PII_HASH_KEY` | Secret mixed into email / IP hashes. Rotating it changes every hash, so treat it as a re-tokenisation |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Unset on ECS / EMR — the task role supplies credentials. MinIO dev keys are rejected |
| `S3_ENDPOINT_URL` | Must be unset in production (an endpoint containing `minio` is rejected) |
| `DATA_CONTRACT_MODE` | Defaults to `enforce` in staging/prod; `off` is rejected in production |
| `API_RATE_LIMIT_PER_MINUTE` | Per-API-key (or per-IP) sustained rate, default 300, `0` disables. In-process: with N tasks the ceiling is N x this, so add an ALB / WAF rate-based rule for a hard global limit |

CI/CD authenticates to AWS with OIDC: create an IAM role that trusts GitHub's OIDC provider and set its ARN as the
`AWS_ROLE_ARN` repository secret. No long-lived access keys are stored in GitHub.

The `src/common/security/secrets.py` module will automatically fetch secrets from Secrets Manager at runtime.

---

## 5. Container Image Build & Push

### Manual build and push

```bash
# Authenticate to ECR
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS \
    --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com

# Build and push API image
docker build -t retailnexus-api -f docker/api/Dockerfile .
docker tag retailnexus-api:latest \
  <account-id>.dkr.ecr.us-east-1.amazonaws.com/retailnexus-api:latest
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/retailnexus-api:latest

# Build and push Spark image
docker build -t retailnexus-spark -f docker/spark/Dockerfile .
docker tag retailnexus-spark:latest \
  <account-id>.dkr.ecr.us-east-1.amazonaws.com/retailnexus-spark:latest
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/retailnexus-spark:latest
```

In practice, GitHub Actions handles this automatically on every tag push.

---

## 6. Database Initialisation

Run once after provisioning RDS:

```bash
psql "$PROD_DATABASE_URL" -f scripts/setup/init_postgres.sql
```

This creates all schemas, tables, indexes and the per-domain watermark rows. It deliberately seeds
**no** credentials: the well-known dev key lives in `scripts/setup/seed_dev_data.sql`, which only the
local docker-compose stack mounts. Create real keys explicitly (random, hashed at rest, expiring, scoped):

```bash
DATABASE_URL="$PROD_DATABASE_URL" python scripts/deployment/create_api_key.py   --name grafana --permission read:pipeline --permission read:quality --expires-days 90
```

The plaintext key is printed once; store it in Secrets Manager.

### Subsequent migrations

Apply versioned migrations with Flyway:

```bash
flyway -url="jdbc:postgresql://<rds-endpoint>:5432/lakehouse" \
       -user=lakehouse \
       -password="$DB_PASSWORD" \
       -locations=filesystem:scripts/migrations \
       migrate
```

---

## 7. Airflow MWAA Bootstrap

After MWAA environment is provisioned:

```bash
# Upload DAGs and plugins to the MWAA S3 bucket
aws s3 sync airflow/dags/    s3://retailnexus-mwaa-prod/dags/
aws s3 sync airflow/plugins/ s3://retailnexus-mwaa-prod/plugins/

# Trigger the bootstrap script via MWAA CLI
aws mwaa create-cli-token --name retailnexus-prod --region us-east-1

# Run the bootstrap
python scripts/airflow/bootstrap_airflow.py \
  --connections-only \
  --skip-existing
```

### MWAA Environment Variables

Set via the MWAA console or Terraform:

```
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__FERNET_KEY=<from-secrets-manager>
AIRFLOW__WEBSERVER__SECRET_KEY=<from-secrets-manager>
SECRETS_BACKEND=aws_secrets_manager
ENVIRONMENT=prod
```

---

## 8. API Service Deployment (ECS Fargate)

The ECS task definition is managed by Terraform. For a manual update:

```bash
# Register a new task definition revision
aws ecs register-task-definition \
  --cli-input-json file://terraform/modules/ecs/task-definition.json

# Update the service to use the new revision
aws ecs update-service \
  --cluster retailnexus-prod \
  --service retailnexus-api \
  --task-definition retailnexus-api:REVISION \
  --force-new-deployment
```

### ECS Task Configuration

```json
{
  "cpu": "1024",
  "memory": "2048",
  "networkMode": "awsvpc",
  "environment": [
    {"name": "ENVIRONMENT",       "value": "prod"},
    {"name": "SECRETS_BACKEND",   "value": "aws_secrets_manager"},
    {"name": "ENABLE_METRICS",    "value": "true"}
  ],
  "portMappings": [{"containerPort": 8000, "protocol": "tcp"}],
  "user": "1001",
  "readonlyRootFilesystem": true
}
```

### Health check

The Application Load Balancer health check targets `GET /health/ready`.
The API returns `503` if the database is unreachable, preventing unhealthy tasks from receiving traffic.

---

## 9. Monitoring Setup

### Prometheus (Amazon Managed Prometheus)

```bash
# Create workspace
aws amp create-workspace --alias retailnexus-prod

# Configure remote-write in prometheus.yml
remote_write:
  - url: https://aps-workspaces.us-east-1.amazonaws.com/workspaces/<id>/api/v1/remote_write
    sigv4:
      region: us-east-1
```

### Grafana (Amazon Managed Grafana)

1. Create an AMG workspace in the console
2. Add the AMP workspace as a Prometheus data source
3. Import the dashboard: `monitoring/grafana/dashboards/pipeline_health.json`
4. Configure Alertmanager to route to the PagerDuty/Slack destinations defined in `monitoring/prometheus/rules/pipeline_alerts.yml`

### CloudWatch Log Groups

All ECS tasks and MWAA workers emit JSON logs automatically.
Log groups are created by Terraform:

- `/retailnexus/api` — FastAPI structured logs
- `/retailnexus/airflow` — Airflow task logs
- `/retailnexus/spark` — EMR Serverless Spark driver/executor logs

---

## 10. CI/CD Pipeline

### Workflows

| Workflow | File | Trigger | Action |
|---|---|---|---|
| CI | `.github/workflows/ci.yml` | Every PR | Lint → typecheck → unit tests → Docker build |
| CD Staging | `.github/workflows/cd-staging.yml` | Merge to `main` | Build → push image → deploy staging → smoke test → Slack notify |
| CD Production | `.github/workflows/cd-prod.yml` | Git tag `v*` | Build → push image → Terraform apply → ECS deploy → health gate → notify |

### Release procedure

```bash
# 1. Merge all PRs to main
# 2. Create and push a version tag
git tag -a v1.2.0 -m "Sprint 10: testing, hardening, docs"
git push origin v1.2.0
# 3. GitHub Actions cd-prod.yml fires automatically
```

---

## 11. Blue-Green Deployment

The ECS service uses a rolling update by default (one task replaced at a time).
For zero-downtime major releases:

1. Deploy the new task definition to a second ECS service (`retailnexus-api-green`)
2. Point the ALB listener rule to the green target group (weighted routing: 5% → 50% → 100%)
3. After 15 minutes at 100%, deregister the blue target group
4. Delete the blue service

This is automated by the `cd-prod.yml` workflow for any release tagged as `v*-major`.

---

## 12. Rollback Procedure

### API rollback (< 5 minutes)

```bash
# Find the previous stable task definition revision
aws ecs describe-task-definition --task-definition retailnexus-api:PREV_REVISION

# Roll back ECS service
aws ecs update-service \
  --cluster retailnexus-prod \
  --service retailnexus-api \
  --task-definition retailnexus-api:PREV_REVISION \
  --force-new-deployment
```

### DAG rollback

```bash
# Re-upload the previous DAG version
aws s3 cp s3://retailnexus-mwaa-prod/dags-backup/ \
          s3://retailnexus-mwaa-prod/dags/ --recursive
```

### Delta table rollback

Delta Lake's ACID time travel allows restoring any table to a previous version:

```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3://retailnexus-lakehouse-prod/gold/fact_sales/")
# Roll back to the version before the bad run
dt.restoreToVersion(version_number)
```

---

## 13. Post-Deployment Verification

Run the smoke test suite:

```bash
TEST_API_URL=https://api.retailnexus.internal \
TEST_API_KEY=$(aws secretsmanager get-secret-value --secret-id retailnexus/prod/api_dev_key --query SecretString --output text) \
pytest tests/integration/ -m integration --tb=short -q
```

Verify the Grafana dashboard at `https://grafana.retailnexus.internal/d/retailnexus-pipeline-health`:

- SLA misses stat shows 0
- DQ pass rate gauge shows > 0.90 for all layers
- API error rate stat shows < 1%
- Domain freshness table shows all rows green

Check the Airflow UI to confirm no DAG failures in the past 24 hours.
