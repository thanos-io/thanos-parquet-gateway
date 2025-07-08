# CircleCI Setup for thanos-parquet-gateway

This document provides a comprehensive overview of the CircleCI configuration set up for the thanos-parquet-gateway project, similar to the Thanos project's CI/CD pipeline.

## Overview

The CircleCI configuration provides:
- **Automated testing** on every commit
- **Docker image building and publishing** to Quay.io
- **Multi-architecture support** (linux/amd64, linux/arm64)
- **Release automation** with GitHub releases and artifacts

## File Structure

```
.circleci/
├── config.yml          # Main CircleCI configuration

.github/workflows/       # Alternative GitHub Actions (optional)
├── ci.yml              # GitHub Actions workflow

scripts/
├── build-and-test.sh   # Local development script

Dockerfile              # Multi-stage Docker build
.dockerignore           # Docker build optimization
```

## CircleCI Jobs

### 1. `test`
- Runs on every commit
- Executes unit tests with `make test-norace`
- Runs linting with `make lint`
- Generates and stores test coverage

### 2. `cross_build`
- Runs on tagged releases only
- Builds binaries for multiple OS/architecture combinations
- Uses promu for cross-compilation
- Stores build artifacts

### 3. `publish_main`
- Runs on `main` branch commits
- Builds multi-architecture Docker images
- Pushes to `quay.io/thanos-io/thanos-parquet-gateway`
- Tags with `latest` and git commit SHA

### 4. `publish_release`
- Runs on version tags (v*.*.*)
- Creates release tarballs
- Builds and publishes Docker images with version tags
- Stores release artifacts

## Version Management

### How Versioning Works

The project uses build-time version injection, following the same approach as Thanos and other Prometheus ecosystem projects:

1. **Version Variables**: Defined in `pkg/version/version.go`
2. **Build-time Injection**: Go's `-ldflags` injects values at build time
3. **Prometheus Compatibility**: Uses `github.com/prometheus/common/version` for consistent output

### Version Sources

- **Git Tags**: Primary source for releases (`git describe --tags`)
- **Development Fallback**: Uses `v0.0.0-dev` when no tags exist
- **VERSION File**: Optional local override (not committed, for development)
- **Git Info**: Automatic fallback using `runtime/debug` build info

### Version Components

```bash
make version  # Show current version information
```

Output includes:
- **VERSION**: Git tag or development version
- **REVISION**: Git commit SHA (short)
- **BRANCH**: Current git branch
- **BUILD_USER**: User@hostname who built the binary
- **BUILD_DATE**: ISO 8601 timestamp

### Local Development

For local development without git tags, you can optionally create a VERSION file:
```bash
echo "v0.1.0-dev" > VERSION  # Optional: Set your development version
make build                   # Build with version injection
./parquet-gateway --version  # Verify version
```

**Note**: The VERSION file is ignored by git and is only for local development convenience.

### Release Process

1. **Tag Release**: `git tag v1.0.0`
2. **Push Tag**: `git push origin v1.0.0`
3. **CI/CD**: Automatically builds and publishes with proper version

## Docker Configuration

### Multi-stage Build
The Dockerfile uses a multi-stage build pattern:
1. **Builder stage**: Go 1.24 Alpine with build tools and version injection
2. **Runtime stage**: Minimal Alpine Linux with CA certificates

### Version Injection
Version information is injected at build time using Go's `-ldflags`:
- **Version**: Git tag or VERSION file content
- **Revision**: Git commit SHA (short)
- **Branch**: Git branch name
- **Build User**: User@hostname who built the binary
- **Build Date**: ISO 8601 timestamp of build

This follows Thanos's approach using the Prometheus version package for consistent output format.

### Security Features
- Non-root user (`thanos:thanos`)
- Minimal attack surface
- CA certificates for HTTPS
- Proper file permissions

### Build Targets
- `make docker-build-local`: Simple local build (single architecture)
- `make docker-build`: Multi-architecture build for CI
- `make docker-test`: Test Docker image functionality
- `make docker-push`: Push to registry
- `make docker-manifest`: Create multi-arch manifest

## Environment Variables

The following environment variables must be configured in CircleCI:

| Variable | Description | Required |
|----------|-------------|----------|
| `QUAY_USERNAME` | Quay.io username or robot account | Yes |
| `QUAY_PASSWORD` | Quay.io password or token | Yes |

## Docker Registry

Images are published to:
- **Registry**: `quay.io`
- **Repository**: `thanos-io/thanos-parquet-gateway`
- **Tags**:
  - `latest`: Latest main branch
  - `<commit-sha>`: Specific commits
  - `v*.*.*`: Release versions

## Local Development

### Prerequisites
- Docker with BuildKit support
- Go 1.24+
- Make

### Quick Start
```bash
# Build and test everything
./scripts/build-and-test.sh

# Or manually:
make build                    # Build binary
make docker-build-local      # Build Docker image
make docker-test             # Test Docker image
```

### Testing CircleCI Locally
Install CircleCI CLI and run:
```bash
circleci config validate .circleci/config.yml
circleci local execute --job test
```

## Makefile Targets

### Core Targets
- `make build`: Build the Go binary with version injection
- `make test`: Run tests with race detection
- `make test-norace`: Run tests without race detection
- `make lint`: Run linting and formatting
- `make version`: Display version information that will be injected

### Version Targets
- `make version`: Show current version variables
- `make deps`: Ensure Go dependencies are up to date
- `make clean`: Clean build artifacts

### Docker Targets
- `make docker-build-local`: Local Docker build
- `make docker-build`: Multi-arch Docker build
- `make docker-test`: Test Docker image
- `make docker-push`: Push to registry
- `make docker-manifest`: Create manifest

### Release Targets
- `make crossbuild`: Build for multiple platforms
- `make tarballs-release`: Create release archives

## GitHub Actions Alternative

An alternative GitHub Actions workflow is provided in `.github/workflows/ci.yml` that offers:
- Similar functionality to CircleCI
- Better GitHub integration
- Free for public repositories
- Automatic release creation

To use GitHub Actions instead of CircleCI:
1. Set up the same environment variables as GitHub secrets
2. Disable CircleCI
3. The workflow will trigger automatically

## Workflow Triggers

### CircleCI
- **All branches**: Run tests
- **Main branch**: Run tests + publish Docker images
- **Version tags**: Run tests + cross-build + publish release

### GitHub Actions
- **All PRs**: Run tests
- **Main branch**: Run tests + publish Docker images
- **Version tags**: Run tests + publish release + create GitHub release

## Troubleshooting

### Common Issues

1. **Docker rate limiting**
   - Use Docker Hub credentials in CI
   - Consider using a Docker registry mirror

2. **Build failures**
   - Check Go version compatibility
   - Verify all dependencies are available
   - Review test failures in CI logs

3. **Registry push failures**
   - Verify Quay.io credentials
   - Check repository permissions
   - Ensure registry URLs are correct

### Debug Commands
```bash
# Test local build
make version                  # Show version info
make build && ./parquet-gateway --version

# Test Docker build
make docker-build-local
make docker-test

# Validate CircleCI config
circleci config validate .circleci/config.yml
```

## Security Considerations

1. **Container Security**
   - Non-root user execution
   - Minimal base image (Alpine)
   - No unnecessary packages

2. **CI/CD Security**
   - Environment variables for secrets
   - Limited scope tokens for registry access
   - Signed Docker images (future enhancement)

3. **Access Control**
   - Proper repository permissions
   - Robot accounts for CI access
   - Regular credential rotation

## Future Enhancements

1. **Security**
   - Docker image signing with Cosign
   - SBOM generation
   - Vulnerability scanning

2. **Performance**
   - Build caching optimization
   - Parallel test execution
   - Registry mirror usage

3. **Monitoring**
   - Build notifications
   - Performance metrics
   - Dependency update automation

## Support

For issues with the CI/CD setup:
1. Check the CircleCI or GitHub Actions logs
2. Verify environment variables are set correctly
3. Test the build process locally
4. Review this documentation for common solutions
