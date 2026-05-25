#!/usr/bin/env bash
# scripts/run-integration-tests.sh
#
# Run the integration test suite against a locally running ReconX stack.
#
# Prerequisites:
#   - All services are already running (via `make run-all` or `docker compose up`).
#   - Go 1.23+ is on PATH.
#
# Usage:
#   ./scripts/run-integration-tests.sh
#   INTEGRATION_GATEWAY_URL=http://localhost:8090 ./scripts/run-integration-tests.sh
#
# Environment variables (all optional):
#   INTEGRATION_GATEWAY_URL     gateway base URL       (default: http://localhost:8090)
#   RECONX_GATEWAY_API_KEY      X-API-Key header value (default: empty → no auth)
#   INTEGRATION_POLL_TIMEOUT    max wait per status poll (default: 60s)
#   INTEGRATION_POLL_INTERVAL   sleep between polls      (default: 2s)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_DIR="$REPO_ROOT/tests/integration"

: "${INTEGRATION_GATEWAY_URL:=http://localhost:8090}"
export INTEGRATION_GATEWAY_URL

echo "Running integration tests against $INTEGRATION_GATEWAY_URL …"
echo ""

cd "$TEST_DIR"
go test -v -timeout 120s ./...
