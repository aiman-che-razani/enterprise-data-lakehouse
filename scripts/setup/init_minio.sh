#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# MinIO Bucket Initialisation
# Runs once on container start via minio-init service in docker-compose.
# Creates all required lakehouse buckets with appropriate retention policies.
# ═══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

MINIO_ALIAS="local"
MINIO_ENDPOINT="http://minio:9000"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
LAKEHOUSE_BUCKET="${LAKEHOUSE_BUCKET:-retailnexus-lakehouse}"

echo ">>> Waiting for MinIO to be ready..."
until mc alias set "${MINIO_ALIAS}" "${MINIO_ENDPOINT}" \
        "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" > /dev/null 2>&1; do
    echo "    MinIO not ready yet, retrying in 3s..."
    sleep 3
done
echo ">>> MinIO is ready."

# ─── Create primary lakehouse bucket ──────────────────────────────────────────
echo ">>> Creating bucket: ${LAKEHOUSE_BUCKET}"
mc mb --ignore-existing "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}"

# ─── Create directory structure (via placeholder objects) ─────────────────────
PREFIXES=(
    "bronze/"
    "silver/"
    "gold/"
    "quality-reports/"
    "checkpoints/"
    "pipeline-state/"
    "schemas/"
    "tmp/"
)

for prefix in "${PREFIXES[@]}"; do
    echo "    Creating prefix: ${LAKEHOUSE_BUCKET}/${prefix}"
    echo "placeholder" | mc pipe "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}/${prefix}.gitkeep" 2>/dev/null || true
done

# ─── Per-domain Bronze prefixes ───────────────────────────────────────────────
DOMAINS=(
    "customers" "orders" "order_lines" "products" "inventory"
    "payments" "shipments" "returns" "campaigns" "clickstream"
    "reviews" "iot_sensors" "suppliers" "employees" "finance"
)

for domain in "${DOMAINS[@]}"; do
    echo "    Creating bronze/${domain}/"
    echo "placeholder" | mc pipe \
        "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}/bronze/${domain}/.gitkeep" 2>/dev/null || true
    echo "placeholder" | mc pipe \
        "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}/silver/${domain}/.gitkeep" 2>/dev/null || true
done

# ─── Gold model prefixes ──────────────────────────────────────────────────────
GOLD_MODELS=(
    "fact_sales" "fact_inventory" "fact_marketing"
    "fact_returns" "fact_shipments" "fact_finance"
    "dim_customer" "dim_date" "dim_store" "dim_employee"
    "dim_supplier" "dim_location" "dim_product" "dim_promotion"
)

for model in "${GOLD_MODELS[@]}"; do
    echo "    Creating gold/${model}/"
    echo "placeholder" | mc pipe \
        "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}/gold/${model}/.gitkeep" 2>/dev/null || true
done

# ─── Anonymous read access is OFF by default ──────────────────────────────────
# The bucket holds the whole lakehouse. Opt in explicitly for a throwaway local stack with
# MINIO_ANONYMOUS_READ=true; production uses IAM policies instead (terraform/modules/iam).
if [[ "${MINIO_ANONYMOUS_READ:-false}" == "true" ]]; then
    mc anonymous set download "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}"
fi

# ─── Create separate bucket for Spark checkpoints ────────────────────────────
echo ">>> Creating checkpoint bucket"
mc mb --ignore-existing "${MINIO_ALIAS}/lakehouse-checkpoints"

# ─── Verify ───────────────────────────────────────────────────────────────────
echo ""
echo ">>> MinIO initialisation complete. Buckets:"
mc ls "${MINIO_ALIAS}/"
echo ""
echo ">>> Lakehouse bucket structure:"
# `head` closes the pipe early; under `pipefail` that SIGPIPE (exit 141) would fail the whole init.
{ mc ls --recursive "${MINIO_ALIAS}/${LAKEHOUSE_BUCKET}/" || true; } | head -40 || true
echo ""
echo ">>> Done."
