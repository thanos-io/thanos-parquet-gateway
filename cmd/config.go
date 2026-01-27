// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger" //nolint:staticcheck
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	"github.com/alecthomas/units"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/thanos-io/thanos-parquet-gateway/locate"
)

func setupInterrupt(ctx context.Context, g *run.Group, log *slog.Logger) {
	ctx, cancel := context.WithCancel(ctx)
	g.Add(func() error {
		<-ctx.Done()
		log.Info("Canceling actors")
		return nil
	}, func(error) {
		cancel()
	})
}

type bucketOpts struct {
	objStoreConfigFile string
	objStoreConfig     string
}

func setupBucket(log *slog.Logger, opts bucketOpts) (objstore.Bucket, error) {
	var confContentYaml []byte
	var err error

	// Read from file if provided, otherwise use inline content
	if opts.objStoreConfigFile != "" {
		confContentYaml, err = os.ReadFile(opts.objStoreConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read objstore config file: %w", err)
		}
	} else if opts.objStoreConfig != "" {
		confContentYaml = []byte(opts.objStoreConfig)
	} else {
		return nil, fmt.Errorf("objstore config is required (use --parquet.objstore-config or --parquet.objstore-config-file)")
	}

	// If config is empty, return error
	if len(confContentYaml) == 0 {
		return nil, fmt.Errorf("objstore config is required")
	}

	bkt, err := client.NewBucket(slogAdapter{log}, confContentYaml, "parquet-gateway", nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create bucket client: %w", err)
	}

	return bkt, nil
}

type slogAdapter struct {
	log *slog.Logger
}

func (s slogAdapter) Log(args ...any) error {
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

type apiOpts struct {
	port int

	shutdownTimeout time.Duration
}

func setupInternalAPI(g *run.Group, log *slog.Logger, reg *prometheus.Registry, opts apiOpts) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "OK")
	})
	mux.HandleFunc("/-/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "OK")
	})

	server := &http.Server{Addr: fmt.Sprintf(":%d", opts.port), Handler: mux}
	g.Add(func() error {
		log.Info("Serving internal api", slog.Int("port", opts.port))
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	}, func(error) {
		log.Info("Shutting down internal api", slog.Int("port", opts.port))
		ctx, cancel := context.WithTimeout(context.Background(), opts.shutdownTimeout)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Error("Error shutting down internal server", slog.Any("err", err))
		}
	})
}

type discoveryOpts struct {
	discoveryInterval    time.Duration
	discoveryConcurrency int
}

func setupDiscovery(ctx context.Context, g *run.Group, log *slog.Logger, bkt objstore.Bucket, opts discoveryOpts) (*locate.Discoverer, error) {
	discoverer := locate.NewDiscoverer(bkt, locate.MetaConcurrency(opts.discoveryConcurrency), locate.Logger(log))

	log.Info("Running initial discovery")

	iterCtx, iterCancel := context.WithTimeout(ctx, opts.discoveryInterval)
	defer iterCancel()
	if err := discoverer.Discover(iterCtx); err != nil {
		return nil, fmt.Errorf("unable to run initial discovery: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		return runutil.Repeat(opts.discoveryInterval, ctx.Done(), func() error {
			log.Debug("Running discovery")

			iterCtx, iterCancel := context.WithTimeout(ctx, opts.discoveryInterval)
			defer iterCancel()
			if err := discoverer.Discover(iterCtx); err != nil {
				log.Warn("Unable to discover new blocks", slog.Any("err", err))
			}
			return nil
		})
	}, func(error) {
		log.Info("Stopping discovery")
		cancel()
	})
	return discoverer, nil
}

type tsdbDiscoveryOpts struct {
	discoveryInterval    time.Duration
	discoveryConcurrency int
	discoveryMinBlockAge time.Duration

	externalLabelMatchers matcherSlice
}

func setupTSDBDiscovery(ctx context.Context, g *run.Group, log *slog.Logger, bkt objstore.Bucket, opts tsdbDiscoveryOpts) (*locate.TSDBDiscoverer, error) {
	discoverer := locate.NewTSDBDiscoverer(
		bkt,
		locate.TSDBMetaConcurrency(opts.discoveryConcurrency),
		locate.TSDBMinBlockAge(opts.discoveryMinBlockAge),
		locate.TSDBMatchExternalLabels(opts.externalLabelMatchers...),
		locate.WithLogger(log),
	)

	log.Info("Running initial tsdb discovery")

	iterCtx, iterCancel := context.WithTimeout(ctx, opts.discoveryInterval)
	defer iterCancel()
	if err := discoverer.Discover(iterCtx); err != nil {
		return nil, fmt.Errorf("unable to run initial discovery: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		return runutil.Repeat(opts.discoveryInterval, ctx.Done(), func() error {
			log.Debug("Running tsdb discovery")

			iterCtx, iterCancel := context.WithTimeout(ctx, opts.discoveryInterval)
			defer iterCancel()
			if err := discoverer.Discover(iterCtx); err != nil {
				log.Warn("Unable to discover new tsdb blocks", slog.Any("err", err))
			}
			return nil
		})
	}, func(error) {
		log.Info("Stopping tsdb discovery")
		cancel()
	})
	return discoverer, nil
}

type syncerOpts struct {
	syncerInterval       time.Duration
	syncerConcurrency    int
	syncerReadBufferSize units.Base2Bytes
	syncerLabelFilesDir  string

	filterType                         string
	filterThanosBackfillEndpoint       string
	filterThanosBackfillUpdateInterval time.Duration
	filterThanosBackfillOverlap        time.Duration
}

func setupMetaFilter(ctx context.Context, g *run.Group, log *slog.Logger, opts syncerOpts) (locate.MetaFilter, error) {
	switch opts.filterType {
	case "all-metas":
		return locate.AllMetasMetaFilter, nil
	case "thanos-backfill":
		thanosBackfillMetaFilter := locate.NewThanosBackfillMetaFilter(opts.filterThanosBackfillEndpoint, opts.filterThanosBackfillOverlap)

		log.Info("Initializing thanos-backfill meta filter")

		iterCtx, iterCancel := context.WithTimeout(ctx, opts.filterThanosBackfillUpdateInterval)
		defer iterCancel()
		if err := thanosBackfillMetaFilter.Update(iterCtx); err != nil {
			return nil, fmt.Errorf("unable to initialize thanos-backfill meta filter: %w", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(opts.filterThanosBackfillUpdateInterval, ctx.Done(), func() error {
				log.Debug("Updating thanos-backfill meta filter")

				iterCtx, iterCancel := context.WithTimeout(ctx, opts.filterThanosBackfillUpdateInterval)
				defer iterCancel()
				if err := thanosBackfillMetaFilter.Update(iterCtx); err != nil {
					log.Warn("Unable to update thanos-backfill meta filter", slog.Any("err", err))
				}
				return nil
			})
		}, func(error) {
			log.Info("Stopping thanos-backfill meta filter updates")
			cancel()
		})
		return thanosBackfillMetaFilter, nil
	default:
		return nil, fmt.Errorf("unknown meta filter type: %s", opts.filterType)
	}
}

func setupSyncer(ctx context.Context, g *run.Group, log *slog.Logger, bkt objstore.Bucket, discoverer *locate.Discoverer, metaFilter locate.MetaFilter, opts syncerOpts) (*locate.Syncer, error) {
	syncer := locate.NewSyncer(
		bkt,
		locate.FilterMetas(metaFilter),
		locate.BlockConcurrency(opts.syncerConcurrency),
		locate.BlockOptions(
			locate.ReadBufferSize(opts.syncerReadBufferSize),
			locate.LabelFilesDir(opts.syncerLabelFilesDir),
		),
	)

	log.Info("Running initial sync")

	iterCtx, iterCancel := context.WithTimeout(ctx, opts.syncerInterval)
	defer iterCancel()
	if err := syncer.Sync(iterCtx, discoverer.Streams()); err != nil {
		return nil, fmt.Errorf("unable to run initial sync: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		return runutil.Repeat(opts.syncerInterval, ctx.Done(), func() error {
			log.Debug("Running sync")

			iterCtx, iterCancel := context.WithTimeout(ctx, opts.syncerInterval)
			defer iterCancel()
			if err := syncer.Sync(iterCtx, discoverer.Streams()); err != nil {
				log.Warn("Unable to sync new blocks", slog.Any("err", err))
			}
			return nil
		})
	}, func(error) {
		log.Info("Stopping syncer")
		cancel()
	})
	return syncer, nil
}
