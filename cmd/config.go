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
	"regexp"
	"time"

	"github.com/alecthomas/units"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/thanos/pkg/runutil"

	cftracing "github.com/thanos-io/thanos-parquet-gateway/internal/tracing"
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

var envPat = regexp.MustCompile(`\$\(([A-Za-z_][A-Za-z0-9_]*)\)`)

func ExpandEnvParens(b []byte) []byte {
	return envPat.ReplaceAllFunc(b, func(m []byte) []byte {
		sub := envPat.FindSubmatch(m)
		if len(sub) != 2 {
			return m
		}
		return []byte(os.Getenv(string(sub[1])))
	})
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

	confContentYaml = ExpandEnvParens(confContentYaml)

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
	// Config file options (Thanos-compatible)
	configFile string
	config     string

	// Legacy flag-based options
	exporterType string

	// jaeger opts
	jaegerEndpoint string

	samplingParam float64
	samplingType  string
}

func setupTracing(ctx context.Context, logger *slog.Logger, opts tracingOpts) error {
	// First, check if config file is provided (Thanos-compatible format)
	if opts.configFile != "" || opts.config != "" {
		var confContentYaml []byte
		var err error

		if opts.configFile != "" {
			confContentYaml, err = os.ReadFile(opts.configFile)
			if err != nil {
				return fmt.Errorf("unable to read tracing config file: %w", err)
			}
		} else {
			confContentYaml = []byte(opts.config)
		}

		confContentYaml = ExpandEnvParens(confContentYaml)
		return cftracing.SetupTracingFromConfig(ctx, logger, confContentYaml)
	}

	// Convert legacy flag-based configuration to TracingConfig
	tracingConfig, err := convertLegacyTracingFlags(logger, opts)
	if err != nil {
		return err
	}

	// Use the parsed config loader to avoid serialization/deserialization
	return cftracing.SetupTracingFromParsedConfig(ctx, logger, tracingConfig)
}

// convertLegacyTracingFlags converts legacy flag-based tracing configuration
// to the new TracingConfig format.
func convertLegacyTracingFlags(logger *slog.Logger, opts tracingOpts) (*cftracing.TracingConfig, error) {
	// Handle no tracing
	if opts.exporterType == "" {
		return &cftracing.TracingConfig{}, nil
	}

	// Handle STDOUT - not supported by thanos packages
	if opts.exporterType == "STDOUT" {
		logger.Warn("STDOUT tracing is not supported by file-based configuration. Tracing will be disabled. Please use OTLP or JAEGER providers instead.")
		return &cftracing.TracingConfig{}, nil
	}

	// Handle JAEGER
	if opts.exporterType == "JAEGER" {
		config := map[string]any{
			"service_name": "parquet-gateway",
		}

		// Add endpoint if provided
		if opts.jaegerEndpoint != "" {
			config["endpoint"] = opts.jaegerEndpoint
		}

		// Convert sampling type
		samplerType, samplerParam := convertSamplingToJaeger(opts.samplingType, opts.samplingParam)
		if samplerType != "" {
			config["sampler_type"] = samplerType
		}
		if samplerParam != 0 {
			config["sampler_param"] = samplerParam
		}

		return &cftracing.TracingConfig{
			Type:   cftracing.ProviderJaeger,
			Config: config,
		}, nil
	}

	return nil, fmt.Errorf("invalid exporter type %s", opts.exporterType)
}

// convertSamplingToJaeger converts legacy sampling configuration to Jaeger-compatible format.
func convertSamplingToJaeger(samplingType string, samplingParam float64) (string, float64) {
	switch samplingType {
	case "PROBABILISTIC":
		return "probabilistic", samplingParam
	case "ALWAYS":
		return "const", 1.0
	case "NEVER":
		return "const", 0.0
	default:
		// Default to const sampler with always sample
		return "const", 1.0
	}
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
