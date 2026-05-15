# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /workspace

# Cache module downloads separately from source code.
# Copy go.mod files for both the service and the proto module it depends on.
COPY proto/gen/go/go.mod  proto/gen/go/go.sum  ./proto/gen/go/
COPY services/ingestion/go.mod services/ingestion/go.sum ./services/ingestion/

# Download dependencies (cached layer)
WORKDIR /workspace/services/ingestion
RUN go mod download

# Copy source code
WORKDIR /workspace
COPY proto/gen/go/   ./proto/gen/go/
COPY services/ingestion/ ./services/ingestion/

# Build the binary with security flags
WORKDIR /workspace/services/ingestion
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-w -s -extldflags '-static'" \
    -o /bin/reconx-ingestion ./cmd/

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM scratch

# Copy timezone data and certificates for HTTPS + time zone support.
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /bin/reconx-ingestion /reconx-ingestion

# Service ports:
#   50051 — gRPC (SubmitRecord / BulkStreamIngest)
#   8080  — HTTP (webhooks / file upload / health)
#   9090  — Prometheus metrics
EXPOSE 50051 8080 9090

ENTRYPOINT ["/reconx-ingestion"]
