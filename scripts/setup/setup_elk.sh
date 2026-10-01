#!/usr/bin/env bash
# Install the lakehouse ILM policy, index template and Kibana saved objects.
# Idempotent — safe to re-run. Usage: scripts/setup/setup_elk.sh
set -euo pipefail

ES_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

wait_for() {
  local name="$1" url="$2"
  for _ in $(seq 1 60); do
    if curl -sf "$url" >/dev/null; then return 0; fi
    sleep 5
  done
  echo "ERROR: $name did not become ready at $url" >&2
  exit 1
}

wait_for Elasticsearch "$ES_URL/_cluster/health"
wait_for Kibana "$KIBANA_URL/api/status"

echo "→ ILM policy"
curl -sf -X PUT "$ES_URL/_ilm/policy/lakehouse-logs" \
  -H 'Content-Type: application/json' \
  --data-binary "@$ROOT/configs/elasticsearch/ilm-policy.json" >/dev/null

echo "→ Index template"
curl -sf -X PUT "$ES_URL/_index_template/lakehouse-logs" \
  -H 'Content-Type: application/json' \
  --data-binary "@$ROOT/configs/elasticsearch/index-template.json" >/dev/null

echo "→ Kibana saved objects"
curl -sf -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
  -H 'kbn-xsrf: true' \
  --form "file=@$ROOT/monitoring/elk/kibana/lakehouse_saved_objects.ndjson" >/dev/null

echo "ELK setup complete. Kibana dashboard: $KIBANA_URL/app/dashboards#/view/lakehouse-data-quality-logs"
