# ─────────────────────────────────────────────────────────────────────────────
# ReconX Reconciliation Engine — Dockerfile
#
# Multi-stage build:
#   Stage 1 (builder): compile Rust binary using cargo with full build deps
#   Stage 2 (runtime): minimal debian-slim image with only the binary + config
#
# Build from repo root:
#   docker build -f services/engine/Dockerfile -t reconx/engine:latest .
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM rust:1.82-slim-bookworm AS builder

# Install build-time dependencies
# - pkg-config + libssl-dev  : required by sqlx (TLS, native-tls path)
# - protobuf-compiler        : required by tonic-build (prost codegen)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Copy proto definitions first (build.rs references them)
COPY proto/ proto/

# Copy engine service source
COPY services/engine/ services/engine/

WORKDIR /workspace/services/engine

# Build the release binary
# SQLX_OFFLINE=true tells sqlx to skip compile-time query verification
# (we use runtime query dispatch, so this is a no-op for us, but
#  prevents accidental compile-time DB connection attempts)
ENV SQLX_OFFLINE=true
RUN cargo build --release

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# Install runtime SSL certificates (required for TLS PostgreSQL connections)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN addgroup --system --gid 1001 reconx \
    && adduser --system --uid 1001 --ingroup reconx --no-create-home reconx

WORKDIR /app

# Copy binary
COPY --from=builder \
    /workspace/services/engine/target/release/reconx-engine \
    /app/reconx-engine

# Copy default configuration
COPY --from=builder \
    /workspace/services/engine/config/ \
    /app/config/

# Run as non-root
USER reconx

# gRPC API
EXPOSE 50052
# Prometheus metrics + /health
EXPOSE 9091

# Health check using the /health HTTP endpoint
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:9091/health || exit 1

ENTRYPOINT ["/app/reconx-engine"]
