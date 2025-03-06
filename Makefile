.PHONY: all build proto lint test

all: build

build: proto parquet-gateway

GO = go
GOIMPORTS = goimports
REVIVE = revive
PROTOC = protoc

lint: $(wildcard **/*.go)
	@echo ">> running lint..."
	@$(REVIVE) -config revive.toml ./...
	find . -name '*.go' ! -path './proto/*' | xargs $(GOIMPORTS) -l -w -local $(head -n 1 go.mod | cut -d ' ' -f 2)

test: $(wildcard **/*.go)
	@echo ">> running tests..."
	@mkdir -p .cover
	$(GO) test -v -race -count=1 ./... -coverprofile .cover/cover.out

parquet-gateway: $(wildcard **/*.go)
	@echo ">> building binaries..."
	@$(GO) build -o parquet-gateway github.com/cloudflare/parquet-tsdb-poc/cmd

proto: proto/metapb/meta.pb.go

proto/metapb/meta.pb.go: proto/metapb/meta.proto
	@echo ">> compiling protos..."
	@$(PROTOC) -I=proto/metapb/ --go_out=paths=source_relative:./proto/metapb/ proto/metapb/meta.proto
