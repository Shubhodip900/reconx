# ── ReconX Resolution Service Makefile ──────────────────────────────────────

BINARY       := reconx-resolution
BUILD_DIR    := ./bin
CMD_DIR      := ./cmd
DOCKER_IMAGE := reconx/resolution
VERSION      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
GO           := go

.PHONY: all build run test lint tidy docker clean help

## all: Build the binary (default)
all: build

## build: Compile the resolution service binary
build:
	@mkdir -p $(BUILD_DIR)
	$(GO) build -ldflags="-X main.Version=$(VERSION) -s -w" -o $(BUILD_DIR)/$(BINARY) $(CMD_DIR)
	@echo "==> Binary: $(BUILD_DIR)/$(BINARY)"

## run: Run the service locally (requires PostgreSQL + engine running)
run: build
	$(BUILD_DIR)/$(BINARY)

## test: Run all unit tests
test:
	$(GO) test -race -count=1 ./...

## lint: Run golangci-lint
lint:
	golangci-lint run ./...

## tidy: Tidy Go module dependencies
tidy:
	$(GO) mod tidy

## docker: Build Docker image
docker:
	docker build -t $(DOCKER_IMAGE):$(VERSION) -t $(DOCKER_IMAGE):latest .

## clean: Remove build artefacts
clean:
	rm -rf $(BUILD_DIR) coverage.out coverage.html

## help: Print this help message
help:
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'
