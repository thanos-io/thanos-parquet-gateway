# Tracing Configuration

The thanos-parquet-gateway supports distributed tracing using OpenTelemetry. There are two ways to configure tracing:

## 1. Thanos-Compatible Configuration File (Recommended)

Use a YAML configuration file that follows the [Thanos tracing configuration format](https://thanos.io/tip/thanos/tracing.md/#opentelemetry-otlp).

### Usage

```bash
./thanos-parquet-gateway serve \
  --tracing.config-file=/path/to/tracing-config.yaml \
  --parquet.objstore-config-file=/path/to/bucket-config.yaml
```

Or provide the configuration inline:

```bash
./thanos-parquet-gateway serve \
  --tracing.config='
type: OTLP
config:
  client_type: grpc
  service_name: parquet-gateway
  endpoint: localhost:4317
  insecure: true
  sampler_type: parentbasedtraceidratiobased
  sampler_param: "0.1"
' \
  --parquet.objstore-config-file=/path/to/bucket-config.yaml
```

### Configuration Format

```yaml
# Tracing provider type: OTLP or JAEGER
type: OTLP

config:
  # OTLP client type: grpc or http
  client_type: grpc

  # Service name for traces
  service_name: parquet-gateway

  # Additional resource attributes
  resource_attributes:
    environment: production
    version: v1.0.0

  # OTLP endpoint (host:port without protocol)
  endpoint: localhost:4317

  # Use insecure connection (no TLS)
  insecure: true

  # Optional: compression (gzip)
  compression: gzip

  # Optional: custom headers
  headers:
    authorization: "Bearer $(AUTH_TOKEN)"

  # Optional: timeout
  timeout: 10s

  # Sampler configuration
  # Types: alwayssample, neversample, traceidratiobased,
  #        parentbasedalwayssample, parentbasedneversample,
  #        parentbasedtraceidratiobased
  sampler_type: parentbasedtraceidratiobased
  sampler_param: "0.1"  # 10% sampling rate
```

### Environment Variable Expansion

Configuration files support environment variable expansion using the `$(VAR_NAME)` syntax:

```yaml
type: OTLP
config:
  endpoint: $(OTEL_EXPORTER_OTLP_ENDPOINT)
  headers:
    authorization: "Bearer $(AUTH_TOKEN)"
```

### Sampler Types

- **alwayssample** / **always**: Sample all traces
- **neversample** / **never**: Never sample traces
- **traceidratiobased** / **probabilistic**: Sample based on trace ID ratio (requires `sampler_param`)
- **parentbasedalwayssample**: Respect parent sampling decision, always sample if no parent
- **parentbasedneversample**: Respect parent sampling decision, never sample if no parent
- **parentbasedtraceidratiobased**: Respect parent sampling decision, use ratio-based sampling if no parent

### Examples

See the included example files:
- `tracing-config-example.yaml` - Full OTLP configuration example
- `tracing-config-stdout-example.yaml` - Simple stdout tracer for debugging

## 2. Legacy Flag-Based Configuration

For backward compatibility, you can still use individual flags:

```bash
./thanos-parquet-gateway serve \
  --tracing.exporter.type=OTLP \
  --tracing.otlp.protocol=grpc \
  --tracing.otlp.endpoint=localhost:4317 \
  --tracing.otlp.insecure \
  --tracing.sampling.type=PROBABILISTIC \
  --tracing.sampling.param=0.1 \
  --parquet.objstore-config-file=/path/to/bucket-config.yaml
```

### Available Flags

- `--tracing.exporter.type`: Exporter type (OTLP, JAEGER, or empty for no tracing)
- `--tracing.jaeger.endpoint`: Jaeger collector endpoint
- `--tracing.otlp.protocol`: OTLP protocol (grpc or http)
- `--tracing.otlp.endpoint`: OTLP endpoint (host:port)
- `--tracing.otlp.insecure`: Use insecure connection
- `--tracing.sampling.type`: Sampling type (PROBABILISTIC, ALWAYS, NEVER)
- `--tracing.sampling.param`: Sampling parameter (0.0-1.0 for PROBABILISTIC)

## Trace Attributes

The parquet-gateway automatically adds the following trace attributes to relevant spans:

### Query Operations
- `series.selector`: Series selector matchers
- `result.series`: Number of series in result
- `result.samples`: Number of samples in result
- `result.histograms`: Number of histogram samples in result
- `result.memory_bytes`: Estimated memory usage of result
- `result.wire_bytes`: Size of gRPC wire protocol response

### Series Operations
- `series.selector`: Series selector matchers
- `series.count`: Number of series returned
- `series.sample_count`: Total number of samples
- `result.wire_bytes`: Size of gRPC wire protocol response

These attributes are automatically computed and added when trace spans are recording.
