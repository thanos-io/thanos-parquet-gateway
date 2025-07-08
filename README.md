# thanos-parquet-gateway

POC for a parquet based TSDB in object storage.

## Why

This project was inspired by [this excellent talk](https://www.youtube.com/watch?v=V8Y4VuUwg8I) by Shopify's Filip Petkovski. It is an attempt to build a service that can convert Prometheus TSDB blocks into [parquet](https://parquet.apache.org/) files and serve [PromQL](https://prometheus.io) queries to backfill a [Thanos deployment](https://thanos.io/).

## Developing

We recommend to use `nix` to fulfill all development dependencies. Visit [Nix Download](https://nixos.org/download/) to get started. To activate the development environment simply run `nix-shell` in the project root.

- to build the binary run `nix-shell --run 'make build'`
- to run tests run `nix-shell --run 'make test'`

## Configuration

This gateway supports the same object storage configuration as the main Thanos project. You can use the `--objstore.config-file` or `--objstore.config` flags to specify your storage backend.

### Supported Storage Backends

Thanks to the integration with [thanos-io/objstore](https://github.com/thanos-io/objstore), the gateway supports all major cloud storage providers:

- **Amazon S3** (and S3-compatible like MinIO)
- **Google Cloud Storage (GCS)**
- **Microsoft Azure Blob Storage**
- **OpenStack Swift**
- **Tencent Cloud Object Storage (COS)**
- **Alibaba Cloud Object Storage Service (OSS)**
- **Local Filesystem**

### Configuration Examples

See the [`examples/`](examples/) directory for configuration examples:

- [`objstore-s3.yaml`](examples/objstore-s3.yaml) - Amazon S3 configuration
- [`objstore-gcs.yaml`](examples/objstore-gcs.yaml) - Google Cloud Storage configuration  
- [`objstore-azure.yaml`](examples/objstore-azure.yaml) - Azure Blob Storage configuration
- [`objstore-filesystem.yaml`](examples/objstore-filesystem.yaml) - Local filesystem configuration
- [`objstore-minio.yaml`](examples/objstore-minio.yaml) - MinIO/S3-compatible configuration

For complete configuration format details, see the [Thanos Storage Documentation](https://thanos.io/tip/thanos/storage.md/#configuration).

## Running

### Server

Once built, you can run the server using:

```bash
# Using config file (recommended)
thanos-parquet-gateway serve \
    --objstore.config-file=examples/objstore-s3.yaml \
    --http.internal.port=6060 \
    --http.prometheus.port=9090 \
    --http.thanos.port=9091 \
    --block.syncer.interval=30m \
    --block.syncer.concurrency=32 \
    --block.discovery.interval=30m \
    --block.discovery.concurrency=32 \
    --query.external-label=prometheus=my-prometheus \
    --query.external-label=replica=ha-1

# Using inline config
thanos-parquet-gateway serve \
    --objstore.config="
type: S3
config:
  bucket: my-bucket
  endpoint: s3.amazonaws.com
" \
    --http.internal.port=6060 \
    --http.prometheus.port=9090 \
    --http.thanos.port=9091
```

This will:

- load blocks from your configured object storage
- expose internal metrics and readiness handlers on port 6060
- expose a subset of the Prometheus HTTP API on port 9090
- expose an Thanos Info, Series and Query gRPC service on port 9091

You can now query it by pointing a Thanos Querier at it or through curl:

```bash
curl 'http://0.0.0.0:9000/api/v1/query' \
  -sq \
  -H 'content-type: application/x-www-form-urlencoded' \
  --data-urlencode 'query=vector(1)' | jq
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {},
        "value": [
          1741267893.103,
          "1"
        ]
      }
    ]
  }
}
```

### Converter

To convert TSDB blocks from one storage to another, you can specify separate configurations for source (TSDB) and destination (Parquet) storage:

```bash
# Using separate config files
thanos-parquet-gateway convert \
    --tsdb.objstore.config-file=tsdb-storage.yaml \
    --parquet.objstore.config-file=parquet-storage.yaml \
    --convert.sorting.label=__name__ \
    --convert.sorting.label=namespace

# Using inline configs
thanos-parquet-gateway convert \
    --tsdb.objstore.config="
type: S3
config:
  bucket: tsdb-source-bucket
" \
    --parquet.objstore.config="
type: S3  
config:
  bucket: parquet-destination-bucket
" \
    --convert.sorting.label=__name__
```

## Note

The code has significant overlap with the work in "https://github.com/prometheus-community/parquet-common". We are in the process of upstreaming and eventually plan to use it as a library.
