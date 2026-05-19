# ── Stage 1: Build ────────────────────────────────────────────────────────────
# Build context is the repo root (set in docker-compose.yml).
# All COPY paths are relative to that root.
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /workspace

# Cache dependency downloads as a separate layer.
COPY proto/gen/go/go.mod  proto/gen/go/go.sum  ./proto/gen/go/
COPY services/gateway/go.mod ./services/gateway/

WORKDIR /workspace/services/gateway
RUN go mod download

# Copy full source.
WORKDIR /workspace
COPY proto/gen/go/   ./proto/gen/go/
COPY services/gateway/ ./services/gateway/

WORKDIR /workspace/services/gateway
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-w -s -extldflags '-static'" \
    -o /bin/reconx-gateway ./cmd/

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata wget

COPY --from=builder /bin/reconx-gateway /reconx-gateway

# Service ports:
#   8090 — Public REST API
#   9093 — Prometheus metrics
EXPOSE 8090 9093

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD wget -qO- http://localhost:8090/health || exit 1

ENTRYPOINT ["/reconx-gateway"]
