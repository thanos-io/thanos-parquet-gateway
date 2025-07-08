.PHONY: all ci build proto lint test-norace test deps docker-build docker-test docker-push docker-manifest crossbuild tarballs-release

# Docker variables
DOCKER_IMAGE_REPO ?= quay.io/thanos-io/thanos-parquet-gateway
DOCKER_IMAGE_TAG ?= latest
DOCKER_CI_TAG ?= $(shell git rev-parse --short HEAD)
DOCKER_PLATFORM ?= linux/amd64,linux/arm64
DOCKER_BUILDX_BUILDER ?= multi-arch-builder

all: build

ci: test-norace build lint

build: protos parquet-gateway

GO = go
GOTOOL = $(GO) tool -modfile=go.tools.mod
GOIMPORTS = $(GOTOOL) golang.org/x/tools/cmd/goimports
REVIVE = $(GOTOOL) github.com/mgechev/revive
MODERNIZE = $(GOTOOL) golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize
PROTOC = protoc

lint: $(wildcard **/*.go)
	@echo ">> running lint..."
	$(REVIVE) -config revive.toml ./...
	$(MODERNIZE) -test ./...
	find . -name '*.go' ! -path './proto/*' | xargs $(GOIMPORTS) -l -w -local $(head -n 1 go.mod | cut -d ' ' -f 2)

test-norace: $(wildcard **/*.go)
	@echo ">> running tests without checking for races..."
	@mkdir -p .cover
	$(GO) test -v -tags stringlabels -short -count=1 ./... -coverprofile .cover/cover.out

test: $(wildcard **/*.go)
	@echo ">> running tests..."
	@mkdir -p .cover
	$(GO) test -v -tags stringlabels -race -short -count=1 ./... -coverprofile .cover/cover.out

parquet-gateway: $(shell find . -type f -name '*.go')
	@echo ">> building binaries..."
	@$(GO) build -tags stringlabels -o parquet-gateway github.com/thanos-io/thanos-parquet-gateway/cmd

protos: proto/metapb/meta.pb.go

proto/metapb/meta.pb.go: proto/metapb/meta.proto
	@echo ">> compiling protos..."
	@$(PROTOC) -I=proto/metapb/ --go_out=paths=source_relative:./proto/metapb/ proto/metapb/meta.proto

# Docker targets
docker-build-local:
	@echo ">> building docker image (local)"
	@docker build -t $(DOCKER_IMAGE_REPO):$(DOCKER_CI_TAG) .

docker-build:
	@echo ">> building docker image"
	@docker buildx create --name $(DOCKER_BUILDX_BUILDER) --use || true
	@docker buildx build --platform=$(DOCKER_PLATFORM) -t $(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG) -t $(DOCKER_IMAGE_REPO):$(DOCKER_CI_TAG) --load .

docker-test:
	@echo ">> testing docker image"
	@docker run --rm $(DOCKER_IMAGE_REPO):$(DOCKER_CI_TAG) --version

docker-push:
	@echo ">> pushing docker image"
	@docker buildx build --platform=$(DOCKER_PLATFORM) -t $(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG) -t $(DOCKER_IMAGE_REPO):$(DOCKER_CI_TAG) --push .

docker-manifest:
	@echo ">> creating docker manifest"
	@docker buildx imagetools create -t $(DOCKER_IMAGE_REPO):$(DOCKER_IMAGE_TAG) $(DOCKER_IMAGE_REPO):$(DOCKER_CI_TAG)

# Cross-compilation targets
crossbuild:
	@echo ">> cross-building binaries"
	@mkdir -p .build
	@for os in linux darwin windows; do \
		for arch in amd64 arm64; do \
			echo "Building for $$os/$$arch"; \
			GOOS=$$os GOARCH=$$arch $(GO) build -tags stringlabels -o .build/thanos-parquet-gateway-$$os-$$arch github.com/thanos-io/thanos-parquet-gateway/cmd; \
		done; \
	done

tarballs-release:
	@echo ">> creating release tarballs"
	@mkdir -p .tarballs
	@for binary in .build/thanos-parquet-gateway-*; do \
		if [ -f "$$binary" ]; then \
			basename=$$(basename $$binary); \
			tar -czf .tarballs/$$basename.tar.gz -C .build $$basename; \
		fi; \
	done
