# Build stage
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

# Version arguments for build-time injection
ARG VERSION="unknown"
ARG REVISION="unknown"
ARG BRANCH="unknown"
ARG BUILD_USER="docker"
ARG BUILD_DATE="unknown"

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum go.tools.mod go.tools.sum ./

# Download dependencies
RUN go mod download && go mod download -modfile=go.tools.mod

# Copy source code
COPY . .

# Build with version injection
RUN PKG="github.com/thanos-io/thanos-parquet-gateway/pkg/version" && \
    LDFLAGS="-s -w \
    -X '${PKG}.Version=${VERSION}' \
    -X '${PKG}.Revision=${REVISION}' \
    -X '${PKG}.Branch=${BRANCH}' \
    -X '${PKG}.BuildUser=${BUILD_USER}' \
    -X '${PKG}.BuildDate=${BUILD_DATE}'" && \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -tags stringlabels -ldflags="${LDFLAGS}" -o thanos-parquet-gateway ./cmd/

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates

# Create non-root user
RUN addgroup -g 1001 -S thanos && \
    adduser -u 1001 -S thanos -G thanos

# Create data directory
RUN mkdir -p /data && chown -R thanos:thanos /data

# Copy the binary from builder stage
COPY --from=builder /app/thanos-parquet-gateway /usr/local/bin/thanos-parquet-gateway

# Make binary executable
RUN chmod +x /usr/local/bin/thanos-parquet-gateway

# Switch to non-root user
USER thanos

# Set working directory
WORKDIR /data

# Expose ports (based on the README example)
# 6060: internal metrics and readiness
# 9090: Prometheus HTTP API
# 9091: Thanos gRPC services
EXPOSE 6060 9090 9091

# Default command
ENTRYPOINT ["/usr/local/bin/thanos-parquet-gateway"]
CMD ["--help"]
