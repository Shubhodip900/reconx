# ── Stage 1: Build ────────────────────────────────────────────────────────────
# Build context is the repo root (set in docker-compose.yml).
# All COPY paths are relative to that root.
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /workspace

# Cache dependency downloads as a separate layer.
# Copy go.mod for both the proto module and this service.
COPY proto/gen/go/go.mod  proto/gen/go/go.sum  ./proto/gen/go/
COPY services/resolution/go.mod ./services/resolution/

WORKDIR /workspace/services/resolution
RUN go mod download

# Copy full source (proto stubs + service code).
WORKDIR /workspace
COPY proto/gen/go/   ./proto/gen/go/
COPY services/resolution/ ./services/resolution/

WORKDIR /workspace/services/resolution
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-w -s -extldflags '-static'" \
    -o /bin/reconx-resolution ./cmd/

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata wget

COPY --from=builder /bin/reconx-resolution /reconx-resolution

# Service ports:
#   50053 — gRPC (ResolveManually / ListMismatches)
#   9092  — Prometheus metrics + /health
EXPOSE 50053 9092

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD wget -qO- http://localhost:9092/health || exit 1

ENTRYPOINT ["/reconx-resolution"]
