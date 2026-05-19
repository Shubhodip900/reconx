# ─────────────────────────────────────────────────────────────────────────────
# ReconX Reconciliation Engine — Makefile
# ─────────────────────────────────────────────────────────────────────────────
.DEFAULT_GOAL := help

BINARY      := reconx-engine
BUILD_DIR   := target/release
DOCKER_IMG  := reconx/engine
DOCKER_TAG  := latest

# ─────────────────────────────────────────────────────────────────────────────
# Development targets
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: build
build: ## Build release binary
	cargo build --release
	@echo "Binary: $(BUILD_DIR)/$(BINARY)"

.PHONY: build-dev
build-dev: ## Build debug binary (faster compile, richer debug info)
	cargo build

.PHONY: run
run: ## Run the engine with development config (requires PostgreSQL)
	RECONX_ENGINE__LOG__FORMAT=text \
	RECONX_ENGINE__LOG__LEVEL=debug \
	cargo run

.PHONY: run-release
run-release: build ## Run the release binary
	./$(BUILD_DIR)/$(BINARY)

# ─────────────────────────────────────────────────────────────────────────────
# Testing
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run all unit tests
	cargo test

.PHONY: test-verbose
test-verbose: ## Run tests with stdout output
	cargo test -- --nocapture

.PHONY: test-cover
test-cover: ## Run tests with llvm-cov coverage (requires cargo-llvm-cov)
	cargo llvm-cov --html --open

# ─────────────────────────────────────────────────────────────────────────────
# Quality
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: lint
lint: ## Run clippy lints
	cargo clippy -- -D warnings

.PHONY: fmt
fmt: ## Format all source files
	cargo fmt

.PHONY: fmt-check
fmt-check: ## Check formatting (CI gate)
	cargo fmt --check

.PHONY: audit
audit: ## Run cargo-audit to check for known vulnerabilities
	cargo audit

.PHONY: tidy
tidy: fmt lint ## Format + lint in one step

# ─────────────────────────────────────────────────────────────────────────────
# Docker
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: docker
docker: ## Build Docker image
	docker build \
		--file Dockerfile \
		--tag $(DOCKER_IMG):$(DOCKER_TAG) \
		../..

.PHONY: docker-push
docker-push: docker ## Push Docker image to registry
	docker push $(DOCKER_IMG):$(DOCKER_TAG)

# ─────────────────────────────────────────────────────────────────────────────
# Proto (regeneration — run from repo root with protoc available)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: proto
proto: ## Recompile proto files (handled automatically by build.rs, use only to verify)
	cargo build 2>&1 | grep -E "(Compiling|Finished|error)"

# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: clean
clean: ## Remove build artifacts
	cargo clean

.PHONY: deps
deps: ## List dependency tree
	cargo tree

.PHONY: check
check: ## Type-check without building
	cargo check

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
