#!/usr/bin/env bash
# scripts/smoke_test.sh
#
# End-to-end smoke test:
#   1. Ingest a transaction from two sources with conflicting data
#   2. Poll until the engine marks it MISMATCHED
#   3. Resolve via the resolution service
#   4. Poll until the engine marks it RESOLVED
#
# Requires: curl, jq
# Environment:
#   GATEWAY_URL  – base URL of the HTTP gateway (default: http://localhost:8080)
#   MAX_WAIT     – seconds to wait per polling phase   (default: 30)

set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
MAX_WAIT="${MAX_WAIT:-30}"
TX_REF="smoke-$(date +%s)-$$"

echo "================================================="
echo " ReconX smoke test"
echo " gateway : $GATEWAY_URL"
echo " tx_ref  : $TX_REF"
echo "================================================="

# ── helpers ──────────────────────────────────────────────────────────────────

die() { echo "FAIL: $*" >&2; exit 1; }

# poll_status <expected>
# Polls GET /v1/recon/{TX_REF} until .status == expected or timeout.
poll_status() {
  local expected="$1"
  local deadline=$(( $(date +%s) + MAX_WAIT ))
  local status=""

  while true; do
    status=$(curl -sfS "${GATEWAY_URL}/v1/recon/${TX_REF}" | jq -r '.status // "UNKNOWN"')

    if [[ "$status" == "$expected" ]]; then
      echo "    status = $status  [OK]"
      return 0
    fi

    if (( $(date +%s) >= deadline )); then
      die "timed out after ${MAX_WAIT}s waiting for $expected (last seen: $status)"
    fi

    echo "    status = $status, waiting for $expected …"
    sleep 2
  done
}

# ── step 1: ingest from system_a ─────────────────────────────────────────────
echo ""
echo "[1/4] Ingesting record from system_a (amount: 100) …"

curl -sfS -X POST "${GATEWAY_URL}/v1/ingest" \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc \
    --arg ref "$TX_REF" \
    --arg key "smoke-a-${TX_REF}" \
    --arg pay "$(echo -n '{"amount":100,"currency":"USD"}' | base64 -w0)" \
    '{
      transaction_ref:  $ref,
      idempotency_key:  $key,
      payload:          $pay,
      metadata: {
        source_system: "system_a",
        trace_id:      "smoke-trace"
      }
    }')"

echo "  -> ingested OK"

# ── step 2: ingest from system_b (conflicting amount) ────────────────────────
echo ""
echo "[2/4] Ingesting conflicting record from system_b (amount: 200) …"

curl -sfS -X POST "${GATEWAY_URL}/v1/ingest" \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc \
    --arg ref "$TX_REF" \
    --arg key "smoke-b-${TX_REF}" \
    --arg pay "$(echo -n '{"amount":200,"currency":"USD"}' | base64 -w0)" \
    '{
      transaction_ref:  $ref,
      idempotency_key:  $key,
      payload:          $pay,
      metadata: {
        source_system: "system_b",
        trace_id:      "smoke-trace"
      }
    }')"

echo "  -> ingested OK"

# ── step 3: assert MISMATCHED ────────────────────────────────────────────────
echo ""
echo "[3/4] Polling for MISMATCHED …"
poll_status "MISMATCHED"

# ── step 4: resolve (choose system_a as canonical) ───────────────────────────
echo ""
echo "[4/4] Resolving — choosing system_a as canonical …"

curl -sfS -X POST "${GATEWAY_URL}/v1/resolve" \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc \
    --arg ref "$TX_REF" \
    '{
      transaction_ref:     $ref,
      chosen_source:       "system_a",
      resolution_reason:   "smoke test — system_a is canonical source",
      resolver_id:         "ci-smoke"
    }')"

echo "  -> resolved OK"

# ── step 5: assert RESOLVED ───────────────────────────────────────────────────
echo ""
echo "[5/4] Polling for RESOLVED …"
poll_status "RESOLVED"

echo ""
echo "================================================="
echo " Smoke test PASSED"
echo "================================================="
