# ── ReconX Ingestion Service Makefile ───────────────────────────────────────
# Works on Linux/macOS (WSL on Windows). Run `make help` for usage.

BINARY        := reconx-ingestion
BUILD_DIR     := ./bin
CMD_DIR       := ./cmd
MODULE        := github.com/reconx/services/ingestion
PROTO_ROOT    := ../../proto
PROTO_GEN_GO  := ../../proto/gen/go
DOCKER_IMAGE  := reconx/ingestion
VERSION       ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")

GO            := go
GOFLAGS       ?=

# Protoc tooling (assumes installed via `make proto-deps`)
PROTOC        ?= protoc
PROTOC_GEN_GO ?= $(shell which protoc-gen-go)
PROTOC_GEN_GRPC ?= $(shell which protoc-gen-go-grpc)

.PHONY: all build clean test lint proto proto-deps docker docker-push run help

## all: Build the binary (default)
all: build

## build: Compile the ingestion service binary
build:
	@echo "==> Building $(BINARY) ..."
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) \
		-ldflags="-X main.Version=$(VERSION) -s -w" \
		-o $(BUILD_DIR)/$(BINARY) $(CMD_DIR)
	@echo "==> Binary: $(BUILD_DIR)/$(BINARY)"

## run: Run the service locally (requires PostgreSQL running)
run: build
	@echo "==> Running $(BINARY) ..."
	$(BUILD_DIR)/$(BINARY)

## test: Run all unit tests
test:
	$(GO) test -race -count=1 ./...

## test-cover: Run tests with coverage report
test-cover:
	$(GO) test -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "==> Coverage report: coverage.html"

## lint: Run golangci-lint (install from https://golangci-lint.run)
lint:
	golangci-lint run ./...

## proto-deps: Install protoc-gen-go and protoc-gen-go-grpc
proto-deps:
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

## proto: Regenerate Go code from .proto files
proto:
	@echo "==> Regenerating proto bindings ..."
	mkdir -p $(PROTO_GEN_GO)/common $(PROTO_GEN_GO)/ingestion \
	         $(PROTO_GEN_GO)/engine  $(PROTO_GEN_GO)/resolution

	$(PROTOC) --proto_path=$(PROTO_ROOT) \
		--go_out=$(PROTO_GEN_GO)/common --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_GEN_GO)/common --go-grpc_opt=paths=source_relative \
		$(PROTO_ROOT)/common.proto

	$(PROTOC) --proto_path=$(PROTO_ROOT) \
		--go_out=$(PROTO_GEN_GO)/ingestion --go_opt=paths=source_relative \
		--go_opt=Mcommon.proto=github.com/reconx/proto/gen/go/common \
		--go-grpc_out=$(PROTO_GEN_GO)/ingestion --go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mcommon.proto=github.com/reconx/proto/gen/go/common \
		$(PROTO_ROOT)/ingestion.proto

	$(PROTOC) --proto_path=$(PROTO_ROOT) \
		--go_out=$(PROTO_GEN_GO)/engine --go_opt=paths=source_relative \
		--go_opt=Mcommon.proto=github.com/reconx/proto/gen/go/common \
		--go-grpc_out=$(PROTO_GEN_GO)/engine --go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mcommon.proto=github.com/reconx/proto/gen/go/common \
		$(PROTO_ROOT)/engine.proto

	$(PROTOC) --proto_path=$(PROTO_ROOT) \
		--go_out=$(PROTO_GEN_GO)/resolution --go_opt=paths=source_relative \
		--go_opt=Mcommon.proto=github.com/reconx/proto/gen/go/common \
		--go_opt=Mengine.proto=github.com/reconx/proto/gen/go/engine \
		--go-grpc_out=$(PROTO_GEN_GO)/resolution --go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mcommon.proto=github.com/reconx/proto/gen/go/common \
		--go-grpc_opt=Mengine.proto=github.com/reconx/proto/gen/go/engine \
		$(PROTO_ROOT)/resolution.proto
	@echo "==> Proto generation complete"

## tidy: Tidy Go module dependencies
tidy:
	$(GO) mod tidy

## docker: Build Docker image
docker:
	docker build -t $(DOCKER_IMAGE):$(VERSION) -t $(DOCKER_IMAGE):latest .

## docker-push: Push Docker image to registry
docker-push: docker
	docker push $(DOCKER_IMAGE):$(VERSION)
	docker push $(DOCKER_IMAGE):latest

## clean: Remove build artefacts
clean:
	rm -rf $(BUILD_DIR) coverage.out coverage.html

## help: Print this help message
help:
	@echo "ReconX Ingestion Service — available targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'
