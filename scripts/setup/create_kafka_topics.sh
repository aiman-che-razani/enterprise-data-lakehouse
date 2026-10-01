#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# Kafka Topic Creation Script
# Creates all required topics for the RetailNexus streaming pipeline.
# Designed to be idempotent — safe to run multiple times.
# ═══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"

echo ">>> Waiting for Kafka broker at ${KAFKA_BOOTSTRAP}..."
until kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" --list > /dev/null 2>&1; do
    echo "    Kafka not ready, retrying in 5s..."
    sleep 5
done
echo ">>> Kafka is ready."

create_topic() {
    local topic=$1
    local partitions=$2
    local retention_ms=$3

    if kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" \
            --describe --topic "${topic}" > /dev/null 2>&1; then
        echo "    [SKIP] Topic already exists: ${topic}"
    else
        kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" \
            --create \
            --topic "${topic}" \
            --partitions "${partitions}" \
            --replication-factor "${REPLICATION_FACTOR}" \
            --config retention.ms="${retention_ms}" \
            --config compression.type=lz4 \
            --config min.insync.replicas=1
        echo "    [OK]   Created topic: ${topic} (partitions=${partitions}, retention=${retention_ms}ms)"
    fi
}

echo ""
echo ">>> Creating streaming topics..."

# ─── Core event topics ────────────────────────────────────────────────────────
# retention: 7 days = 604800000ms, 3 days = 259200000ms, 1 day = 86400000ms

create_topic "retailnexus.orders"        12  604800000   # 7 days
create_topic "retailnexus.payments"      12  604800000   # 7 days
create_topic "retailnexus.clickstream"   24  259200000   # 3 days (high volume)
create_topic "retailnexus.inventory"      6  604800000   # 7 days
create_topic "retailnexus.iot-sensors"   24   86400000   # 1 day (very high volume)

# ─── CDC topics ───────────────────────────────────────────────────────────────
create_topic "retailnexus.cdc.customers"  6  604800000
create_topic "retailnexus.cdc.products"   3  604800000

# ─── Dead letter topics ───────────────────────────────────────────────────────
create_topic "retailnexus.dlq.orders"    3  2592000000  # 30 days (DLQ kept longer)
create_topic "retailnexus.dlq.payments"  3  2592000000
create_topic "retailnexus.dlq.all"       3  2592000000

# ─── Internal / system topics ─────────────────────────────────────────────────
create_topic "retailnexus.pipeline.metrics"  3  86400000   # 1 day

echo ""
echo ">>> All topics created. Current topic list:"
kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP}" --list | grep "retailnexus"
echo ""
echo ">>> Done."
