// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v3"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
)

type bucketOpts struct {
	storage string
	prefix  string

	// filesystem options
	filesystemDirectory string

	// s3 options
	s3Bucket    string
	s3Endpoint  string
	s3AccessKey string
	s3SecretKey string
	s3Insecure  bool

	retries int
}

func (opts *bucketOpts) registerFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("storage.type", "type of storage").Default("filesystem").EnumVar(&opts.storage, "filesystem", "s3")
	cmd.Flag("storage.prefix", "prefix for the storage").Default("").StringVar(&opts.prefix)
	cmd.Flag("storage.filesystem.directory", "directory for filesystem").Default(".data").StringVar(&opts.filesystemDirectory)
	cmd.Flag("storage.s3.bucket", "bucket for s3").Default("").StringVar(&opts.s3Bucket)
	cmd.Flag("storage.s3.endpoint", "endpoint for s3").Default("").StringVar(&opts.s3Endpoint)
	cmd.Flag("storage.s3.access_key", "access key for s3").Default("").Envar("STORAGE_S3_ACCESS_KEY").StringVar(&opts.s3AccessKey)
	cmd.Flag("storage.s3.secret_key", "secret key for s3").Default("").Envar("STORAGE_S3_SECRET_KEY").StringVar(&opts.s3SecretKey)
	cmd.Flag("storage.s3.insecure", "use http").Default("false").BoolVar(&opts.s3Insecure)
	cmd.Flag("storage.retries", "how many retries to perform").Default("2").IntVar(&opts.retries)
}

func (opts *bucketOpts) registerParquetFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("parquet.storage.type", "type of storage").Default("filesystem").EnumVar(&opts.storage, "filesystem", "s3")
	cmd.Flag("parquet.storage.prefix", "prefix for the storage").Default("").StringVar(&opts.prefix)
	cmd.Flag("parquet.storage.filesystem.directory", "directory for filesystem").Default(".data").StringVar(&opts.filesystemDirectory)
	cmd.Flag("parquet.storage.s3.bucket", "bucket for s3").Default("").StringVar(&opts.s3Bucket)
	cmd.Flag("parquet.storage.s3.endpoint", "endpoint for s3").Default("").StringVar(&opts.s3Endpoint)
	cmd.Flag("parquet.storage.s3.access_key", "access key for s3").Default("").Envar("PARQUET_STORAGE_S3_ACCESS_KEY").StringVar(&opts.s3AccessKey)
	cmd.Flag("parquet.storage.s3.secret_key", "secret key for s3").Default("").Envar("PARQUET_STORAGE_S3_SECRET_KEY").StringVar(&opts.s3SecretKey)
	cmd.Flag("parquet.storage.s3.insecure", "use http").Default("false").BoolVar(&opts.s3Insecure)
	cmd.Flag("parquet.storage.retries", "how many retries to perform").Default("2").IntVar(&opts.retries)
}

func (opts *bucketOpts) registerTSDBFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("tsdb.storage.type", "type of storage").Default("filesystem").EnumVar(&opts.storage, "filesystem", "s3")
	cmd.Flag("tsdb.storage.prefix", "prefix for the storage").Default("").StringVar(&opts.prefix)
	cmd.Flag("tsdb.storage.filesystem.directory", "directory for filesystem").Default(".data").StringVar(&opts.filesystemDirectory)
	cmd.Flag("tsdb.storage.s3.bucket", "bucket for s3").Default("").StringVar(&opts.s3Bucket)
	cmd.Flag("tsdb.storage.s3.endpoint", "endpoint for s3").Default("").StringVar(&opts.s3Endpoint)
	cmd.Flag("tsdb.storage.s3.access_key", "access key for s3").Default("").Envar("TSDB_STORAGE_S3_ACCESS_KEY").StringVar(&opts.s3AccessKey)
	cmd.Flag("tsdb.storage.s3.secret_key", "secret key for s3").Default("").Envar("TSDB_STORAGE_S3_SECRET_KEY").StringVar(&opts.s3SecretKey)
	cmd.Flag("tsdb.storage.s3.insecure", "use http").Default("false").BoolVar(&opts.s3Insecure)
	cmd.Flag("tsdb.storage.retries", "how many retries to perform").Default("2").IntVar(&opts.retries)
}

func setupBucket(log *slog.Logger, opts bucketOpts) (objstore.Bucket, error) {
	prov := client.ObjProvider(strings.ToUpper(opts.storage))
	cfg := client.BucketConfig{
		Type:   prov,
		Prefix: opts.prefix,
	}
	var subCfg any
	switch prov {
	case client.FILESYSTEM:
		subCfg = struct {
			Directory string `yaml:"directory"`
		}{
			Directory: opts.filesystemDirectory,
		}
	case client.S3:
		subCfg = struct {
			Bucket     string `yaml:"bucket"`
			Endpoint   string `yaml:"endpoint"`
			AccessKey  string `yaml:"access_key"`
			SecretKey  string `yaml:"secret_key"`
			MaxRetries int    `yaml:"max_retries"`
			Insecure   bool   `yaml:"insecure"`
		}{
			Bucket:     opts.s3Bucket,
			Endpoint:   opts.s3Endpoint,
			AccessKey:  opts.s3AccessKey,
			SecretKey:  opts.s3SecretKey,
			Insecure:   opts.s3Insecure,
			MaxRetries: opts.retries,
		}
	default:
		return nil, fmt.Errorf("unknown bucket type: %s", prov)
	}

	cfg.Config = subCfg
	bytes, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal bucket config yaml: %w", err)
	}

	bkt, err := client.NewBucket(slogAdapter{log}, bytes, "parquet-gateway", nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create bucket client: %w", err)
	}

	return bkt, nil
}

type slogAdapter struct {
	log *slog.Logger
}

func (s slogAdapter) Log(args ...interface{}) error {
	s.log.Debug("", args...)
	return nil
}

type tracingOpts struct {
	exporterType string

	// jaeger opts
	jaegerEndpoint string

	samplingParam float64
	samplingType  string
}

func (opts *tracingOpts) registerFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("tracing.exporter.type", "type of tracing exporter").Default("STDOUT").EnumVar(&opts.exporterType, "JAEGER", "STDOUT")
	cmd.Flag("tracing.jaeger.endpoint", "endpoint to send traces, eg. https://example.com:4318/v1/traces").StringVar(&opts.jaegerEndpoint)
	cmd.Flag("tracing.sampling.param", "sample of traces to send").Default("0.1").Float64Var(&opts.samplingParam)
	cmd.Flag("tracing.sampling.type", "type of sampling").Default("PROBABILISTIC").EnumVar(&opts.samplingType, "PROBABILISTIC", "ALWAYS", "NEVER")
}

func setupTracing(ctx context.Context, opts tracingOpts) error {
	var (
		exporter trace.SpanExporter
		err      error
	)
	switch opts.exporterType {
	case "JAEGER":
		exporter, err = jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(opts.jaegerEndpoint)))
		if err != nil {
			return err
		}
	case "STDOUT":
		exporter, err = stdouttrace.New()
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid exporter type %s", opts.exporterType)
	}
	var sampler trace.Sampler
	switch opts.samplingType {
	case "PROBABILISTIC":
		sampler = trace.TraceIDRatioBased(opts.samplingParam)
	case "ALWAYS":
		sampler = trace.AlwaysSample()
	case "NEVER":
		sampler = trace.NeverSample()
	default:
		return fmt.Errorf("invalid sampling type %s", opts.samplingType)
	}
	r, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("parquet-gateway"),
			semconv.ServiceVersion("v0.0.0"),
		),
	)
	if err != nil {
		return err
	}

	tracerProvider := trace.NewTracerProvider(
		trace.WithSampler(trace.ParentBased(sampler)),
		trace.WithBatcher(exporter),
		trace.WithResource(r),
	)
	otel.SetTracerProvider(tracerProvider)
	return nil
}
