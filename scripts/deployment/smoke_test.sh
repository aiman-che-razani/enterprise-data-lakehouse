#!/usr/bin/env bash
# Post-deploy smoke test for the RetailNexus API.
# Usage: scripts/deployment/smoke_test.sh <base-url> <api-key> [retries]
# Exits non-zero on the first failing check.
set -euo pipefail

BASE_URL="${1:?usage: smoke_test.sh <base-url> <api-key> [retries]}"
API_KEY="${2:?usage: smoke_test.sh <base-url> <api-key> [retries]}"
RETRIES="${3:-10}"
BASE_URL="${BASE_URL%/}"

status_of() {  # status_of <path> [extra curl args...]
  local path="$1"; shift
  curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@" "$BASE_URL$path" || true
}

expect() {  # expect <expected-status> <label> <path> [extra curl args...]
  local expected="$1" label="$2" path="$3"; shift 3
  local got
  got="$(status_of "$path" "$@")"
  if [[ "$got" != "$expected" ]]; then
    echo "FAIL  $label ($path): expected $expected, got $got" >&2
    return 1
  fi
  echo "ok    $label ($path) -> $got"
}

# Readiness gates everything else — retry while the new tasks warm up.
for attempt in $(seq 1 "$RETRIES"); do
  if [[ "$(status_of /health/ready)" == "200" ]]; then
    echo "ok    readiness (attempt $attempt)"
    break
  fi
  if [[ "$attempt" == "$RETRIES" ]]; then
    echo "FAIL  /health/ready never returned 200 after $RETRIES attempts" >&2
    exit 1
  fi
  echo "…     not ready yet (attempt $attempt/$RETRIES), retrying in 10s"
  sleep 10
done

expect 200 "liveness"                  /health/
expect 401 "auth rejects missing key"  /pipeline/status
expect 401 "auth rejects bad key"      /pipeline/status -H "X-API-Key: not-a-real-key"
expect 200 "pipeline status"           /pipeline/status -H "X-API-Key: $API_KEY"
expect 200 "quality summary"           /quality/summary -H "X-API-Key: $API_KEY"
expect 200 "catalog tables"            /catalog/tables  -H "X-API-Key: $API_KEY"
expect 200 "pipeline watermarks"       /pipeline/watermarks -H "X-API-Key: $API_KEY"
expect 401 "runs endpoint requires a key"  /pipeline/runs
expect 200 "prometheus metrics"        /metrics

echo "Smoke test passed for $BASE_URL"
