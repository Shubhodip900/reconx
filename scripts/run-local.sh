#!/usr/bin/env bash
# run-local.sh — start the full ReconX stack locally (no Docker for services).
#
# What it does:
#   1. Starts PostgreSQL via "docker compose up postgres -d" (if not already running).
#   2. Builds all four service binaries.
#   3. Starts each service as a background process with its default env vars.
#   4. Traps SIGINT / SIGTERM and kills all service processes on exit.
#
# Usage:
#   ./scripts/run-local.sh          # start everything
#   ./scripts/run-local.sh --no-build   # skip build step
#
# Prerequisites:
#   - Docker (for postgres)
#   - Go 1.23+  (for ingestion / resolution / gateway)
#   - Rust 1.82+ (for engine)
#   - All services must be buildable (run "make build-all" once to verify)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD=true

for arg in "$@"; do
  case "$arg" in
    --no-build) BUILD=false ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log()  { echo -e "${GREEN}[run-local]${NC} $*"; }
warn() { echo -e "${YELLOW}[run-local]${NC} $*"; }
err()  { echo -e "${RED}[run-local]${NC} $*" >&2; }

# ── Track background PIDs for cleanup ────────────────────────────────────────
PIDS=()

cleanup() {
  echo ""
  warn "Shutting down services..."
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  # Give processes a moment to exit gracefully
  sleep 1
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  log "All services stopped."
}

trap cleanup SIGINT SIGTERM EXIT

# ── 1. Start PostgreSQL ───────────────────────────────────────────────────────
log "Starting PostgreSQL via Docker Compose..."
docker compose -f "$REPO_ROOT/docker-compose.yml" up postgres -d

log "Waiting for PostgreSQL to be healthy..."
for i in $(seq 1 20); do
  if docker compose -f "$REPO_ROOT/docker-compose.yml" exec -T postgres \
      pg_isready -U reconx -d reconx -q 2>/dev/null; then
    log "PostgreSQL is ready."
    break
  fi
  if [ "$i" -eq 20 ]; then
    err "PostgreSQL did not become healthy in time. Aborting."
    exit 1
  fi
  sleep 2
done

DSN="postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable"

# ── 2. Build ──────────────────────────────────────────────────────────────────
if [ "$BUILD" = true ]; then
  log "Building all services..."
  make -C "$REPO_ROOT" build-all
  log "Build complete."
fi

# ── 3. Start services ─────────────────────────────────────────────────────────

# Ingestion Service
log "Starting Ingestion Service on :50051 (gRPC) :8080 (HTTP) :9090 (metrics)..."
RECONX_DATABASE_DSN="$DSN" \
RECONX_GRPC_PORT="50051" \
RECONX_HTTP_PORT="8080" \
RECONX_METRICS_PORT="9090" \
RECONX_LOG_LEVEL="info" \
RECONX_KAFKA_ENABLED="false" \
  "$REPO_ROOT/services/ingestion/bin/reconx-ingestion" \
  >> /tmp/reconx-ingestion.log 2>&1 &
PIDS+=($!)
log "  Ingestion PID=$! — logs: /tmp/reconx-ingestion.log"

# Give ingestion a moment before engine starts (engine reads ingestion_records)
sleep 1

# Reconciliation Engine (Rust)
log "Starting Reconciliation Engine on :50052 (gRPC) :9091 (metrics)..."
RECONX_ENGINE__DATABASE__DSN="$DSN" \
RECONX_ENGINE__GRPC__PORT="50052" \
RECONX_ENGINE__METRICS__PORT="9091" \
RECONX_ENGINE__LOG__LEVEL="info" \
  "$REPO_ROOT/services/engine/target/release/reconx-engine" \
  >> /tmp/reconx-engine.log 2>&1 &
PIDS+=($!)
log "  Engine PID=$! — logs: /tmp/reconx-engine.log"

# Resolution Service
log "Starting Resolution Service on :50053 (gRPC) :8082 (HTTP) :9092 (metrics)..."
RECONX_RESOLUTION_DATABASE_DSN="$DSN" \
RECONX_RESOLUTION_GRPC_PORT="50053" \
RECONX_RESOLUTION_HTTP_PORT="8082" \
RECONX_RESOLUTION_METRICS_PORT="9092" \
RECONX_RESOLUTION_ENGINE_ADDRESS="localhost:50052" \
RECONX_RESOLUTION_LOG_LEVEL="info" \
  "$REPO_ROOT/services/resolution/bin/reconx-resolution" \
  >> /tmp/reconx-resolution.log 2>&1 &
PIDS+=($!)
log "  Resolution PID=$! — logs: /tmp/reconx-resolution.log"

# API Gateway
log "Starting API Gateway on :8090 (HTTP) :9093 (metrics)..."
RECONX_GATEWAY_HTTP_PORT="8090" \
RECONX_GATEWAY_METRICS_PORT="9093" \
RECONX_GATEWAY_INGESTION_ADDRESS="localhost:50051" \
RECONX_GATEWAY_ENGINE_ADDRESS="localhost:50052" \
RECONX_GATEWAY_RESOLUTION_ADDRESS="localhost:50053" \
RECONX_GATEWAY_LOG_LEVEL="info" \
  "$REPO_ROOT/services/gateway/bin/reconx-gateway" \
  >> /tmp/reconx-gateway.log 2>&1 &
PIDS+=($!)
log "  Gateway PID=$! — logs: /tmp/reconx-gateway.log"

# ── 4. Summary ────────────────────────────────────────────────────────────────
echo ""
log "All services started. Port map:"
echo "  Ingestion  gRPC :50051 | HTTP :8080 | metrics :9090"
echo "  Engine     gRPC :50052 |             metrics :9091"
echo "  Resolution gRPC :50053 | HTTP :8082 | metrics :9092"
echo "  Gateway              HTTP :8090 | metrics :9093"
echo ""
log "Press Ctrl+C to stop all services."

# Wait forever (until Ctrl-C / trap fires)
wait
