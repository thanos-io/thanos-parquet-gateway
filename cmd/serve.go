// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/alecthomas/units"
	"github.com/oklog/run"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	_ "github.com/mostynb/go-grpc-compression/snappy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/propagators/autoprop"

	cfgrpc "github.com/thanos-io/thanos-parquet-gateway/api/grpc"
	cfhttp "github.com/thanos-io/thanos-parquet-gateway/api/http"
	"github.com/thanos-io/thanos-parquet-gateway/cardinality"
	cfdb "github.com/thanos-io/thanos-parquet-gateway/db"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type serveOpts struct {
	bucket  bucketOpts
	tracing tracingOpts

	discovery discoveryOpts
	syncer    syncerOpts

	query       queryOpts
	promAPI     apiOpts
	thanosAPI   apiOpts
	internalAPI apiOpts

	cardinality cardinalityOpts

	corsAllowedOrigins []string
}

type cardinalityOpts struct {
	enabled     bool
	cacheDir    string
	concurrency int
	timeout     time.Duration

	preloadEnabled     bool
	preloadDays        int
	preloadConcurrency int
	preloadInterval    time.Duration
}

func (opts *serveOpts) registerFlags(cmd *kingpin.CmdClause) {
	opts.bucket.registerServeFlags(cmd)
	opts.tracing.registerServeFlags(cmd)
	opts.discovery.registerServeFlags(cmd)
	opts.syncer.registerServeFlags(cmd)
	opts.query.registerServeFlags(cmd)
	opts.promAPI.registerServePrometheusAPIFlags(cmd)
	opts.thanosAPI.registerServeThanosAPIFlags(cmd)
	opts.internalAPI.registerServeInternalAPIFlags(cmd)
	opts.cardinality.registerServeFlags(cmd)

	cmd.Flag("http.cors.allowed-origins", "comma-separated list of allowed CORS origins (use * for all)").StringsVar(&opts.corsAllowedOrigins)
}

func (opts *cardinalityOpts) registerServeFlags(cmd *kingpin.CmdClause) {
	defaultConcurrency := strconv.Itoa(runtime.NumCPU() * 2)

	cmd.Flag("cardinality.enabled", "enable cardinality explorer API").Default("true").BoolVar(&opts.enabled)
	cmd.Flag("cardinality.cache-dir", "directory to cache parquet files for cardinality analysis").Default(".data/cache/cardinality").StringVar(&opts.cacheDir)
	cmd.Flag("cardinality.concurrency", "number of concurrent workers for cardinality queries").Default(defaultConcurrency).IntVar(&opts.concurrency)
	cmd.Flag("cardinality.timeout", "timeout for cardinality queries (labels queries may need longer timeouts)").Default("5m").DurationVar(&opts.timeout)
	cmd.Flag("cardinality.preload.enabled", "enable background preloading of cardinality indexes on startup").Default("true").BoolVar(&opts.preloadEnabled)
	cmd.Flag("cardinality.preload.days", "number of days to preload (from today backwards)").Default("60").IntVar(&opts.preloadDays)
	cmd.Flag("cardinality.preload.concurrency", "number of concurrent workers for preloading").Default(defaultConcurrency).IntVar(&opts.preloadConcurrency)
	cmd.Flag("cardinality.preload.interval", "how often to check for new blocks to preload").Default("1h").DurationVar(&opts.preloadInterval)
}

func (opts *bucketOpts) registerServeFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("parquet.objstore-config-file", "YAML file that contains object store configuration. See format details: https://thanos.io/tip/thanos/storage.md/#configuration").StringVar(&opts.objStoreConfigFile)
	cmd.Flag("parquet.objstore-config", "Alternative to 'parquet.objstore-config-file'. YAML content for object store configuration.").StringVar(&opts.objStoreConfig)
}

func (opts *tracingOpts) registerServeFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("tracing.exporter.type", "type of tracing exporter").Default("STDOUT").EnumVar(&opts.exporterType, "JAEGER", "STDOUT")
	cmd.Flag("tracing.jaeger.endpoint", "endpoint to send traces, eg. https://example.com:4318/v1/traces").StringVar(&opts.jaegerEndpoint)
	cmd.Flag("tracing.sampling.param", "sample of traces to send").Default("0.1").Float64Var(&opts.samplingParam)
	cmd.Flag("tracing.sampling.type", "type of sampling").Default("PROBABILISTIC").EnumVar(&opts.samplingType, "PROBABILISTIC", "ALWAYS", "NEVER")
}

func (opts *discoveryOpts) registerServeFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("block.discovery.interval", "interval to discover blocks").Default("1m").DurationVar(&opts.discoveryInterval)
	cmd.Flag("block.discovery.concurrency", "concurrency for loading metadata").Default("1").IntVar(&opts.discoveryConcurrency)
}

func (opts *syncerOpts) registerServeFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("block.syncer.interval", "interval to sync blocks").Default("1m").DurationVar(&opts.syncerInterval)
	cmd.Flag("block.syncer.concurrency", "concurrency for loading blocks").Default("1").IntVar(&opts.syncerConcurrency)
	cmd.Flag("block.syncer.read-buffer-size", "read buffer size for blocks").Default("2MiB").BytesVar(&opts.syncerReadBufferSize)
	cmd.Flag("block.filter.type", "").Default("all-metas").EnumVar(&opts.filterType, "thanos-backfill", "all-metas")
	cmd.Flag("block.filter.thanos-backfill.endpoint", "endpoint to ignore for backfill").StringVar(&opts.filterThanosBackfillEndpoint)
	cmd.Flag("block.filter.thanos-backfill.interval", "interval to update thanos-backfill timerange").Default("1m").DurationVar(&opts.filterThanosBackfillUpdateInterval)
	cmd.Flag("block.filter.thanos-backfill.overlap", "overlap interval to leave for backfill").Default("24h").DurationVar(&opts.filterThanosBackfillOverlap)
}

func (opts *queryOpts) registerServeFlags(cmd *kingpin.CmdClause) {
	// We need to initialize the externalLabels map here
	opts.externalLabels = make(map[string]string)

	cmd.Flag("query.step", "default step for range queries").Default("30s").DurationVar(&opts.defaultStep)
	cmd.Flag("query.lookback", "default lookback for queries").Default("5m").DurationVar(&opts.defaultLookback)
	cmd.Flag("query.timeout", "default timeout for queries").Default("30s").DurationVar(&opts.defaultTimeout)
	cmd.Flag("query.external-label", "external label to add to results").StringMapVar(&opts.externalLabels)
	cmd.Flag("query.limits.select.max-chunk-bytes", "the amount of chunk bytes a query can fetch in 'Select' operations. (0B is unlimited)").Default("0B").BytesVar(&opts.selectChunkBytesQuota)
	cmd.Flag("query.limits.select.max-row-count", "the amount of rows a query can fetch in 'Select' operations. (0 is unlimited)").Default("0").Int64Var(&opts.selectRowCountQuota)
	cmd.Flag("query.limits.queries.max-concurrent", "the amount of concurrent queries we can execute").Default("100").IntVar(&opts.concurrentQueryQuota)
	cmd.Flag("query.storage.select.chunk-partition.max-range-bytes", "coalesce chunk reads into ranges of this length to be scheduled concurrently.").Default("64MiB").BytesVar(&opts.selectChunkPartitionMaxRange)
	cmd.Flag("query.storage.select.chunk-partition.max-gap-bytes", "the maximum acceptable gap when coalescing chunk ranges.").Default("64MiB").BytesVar(&opts.selectChunkPartitionMaxGap)
	cmd.Flag("query.storage.select.chunk-partition.max-concurrency", "the maximum amount of concurrent fetches to object storage per partition.").Default("2").IntVar(&opts.selectChunkPartitionMaxConcurrency)
}

func (opts *apiOpts) registerServeThanosAPIFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("http.thanos.port", "port to host thanos gRPC api").Default("9001").IntVar(&opts.port)
	cmd.Flag("http.thanos.shutdown-timeout", "timeout on shutdown").Default("10s").DurationVar(&opts.shutdownTimeout)
}

func (opts *apiOpts) registerServePrometheusAPIFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("http.prometheus.port", "port to host prometheus query HTTP api").Default("9000").IntVar(&opts.port)
	cmd.Flag("http.prometheus.shutdown-timeout", "timeout on shutdown").Default("10s").DurationVar(&opts.shutdownTimeout)
}

func (opts *apiOpts) registerServeInternalAPIFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("http.internal.port", "port to host query api").Default("6060").IntVar(&opts.port)
	cmd.Flag("http.internal.shutdown-timeout", "timeout on shutdown").Default("10s").DurationVar(&opts.shutdownTimeout)
}

func registerServeApp(app *kingpin.Application) (*kingpin.CmdClause, func(context.Context, *slog.Logger, *prometheus.Registry) error) {
	cmd := app.Command("serve", "serve Prometheus HTTP and thanos gRPC from parquet files in object storage")

	var opts serveOpts
	opts.registerFlags(cmd)

	return cmd, func(ctx context.Context, log *slog.Logger, reg *prometheus.Registry) error {
		var g run.Group

		setupInterrupt(ctx, &g, log)

		if err := setupTracing(ctx, opts.tracing); err != nil {
			return fmt.Errorf("unable to setup tracing: %w", err)
		}

		bkt, err := setupBucket(log, opts.bucket)
		if err != nil {
			return fmt.Errorf("unable to setup bucket: %w", err)
		}

		discoverer, err := setupDiscovery(ctx, &g, log, bkt, opts.discovery)
		if err != nil {
			return fmt.Errorf("unable to setup discovery: %w", err)
		}

		metaFilter, err := setupMetaFilter(ctx, &g, log, opts.syncer)
		if err != nil {
			return fmt.Errorf("unable to set up meta filter: %w", err)
		}

		syncer, err := setupSyncer(ctx, &g, log, bkt, discoverer, metaFilter, opts.syncer)
		if err != nil {
			return fmt.Errorf("unable to setup syncer: %w", err)
		}

		db := cfdb.NewDB(
			syncer,
			cfdb.ExternalLabels(labels.FromMap(opts.query.externalLabels)),
		)

		if err := setupPromAPI(&g, log, bkt, db, opts.promAPI, opts.query, opts.cardinality, opts.corsAllowedOrigins); err != nil {
			return fmt.Errorf("setup prometheus API: %w", err)
		}
		setupThanosAPI(&g, log, db, opts.thanosAPI, opts.query)
		setupInternalAPI(&g, log, reg, opts.internalAPI)

		return g.Run()
	}
}

type queryOpts struct {
	defaultStep     time.Duration
	defaultLookback time.Duration
	defaultTimeout  time.Duration
	externalLabels  map[string]string

	// Limits
	selectChunkBytesQuota units.Base2Bytes
	selectRowCountQuota   int64
	concurrentQueryQuota  int

	// Storage
	selectChunkPartitionMaxRange       units.Base2Bytes
	selectChunkPartitionMaxGap         units.Base2Bytes
	selectChunkPartitionMaxConcurrency int
}

func engineFromQueryOpts(opts queryOpts) promql.QueryEngine {
	return engine.New(engine.Opts{
		DisableDuplicateLabelChecks: true,
		LogicalOptimizers: []logicalplan.Optimizer{
			&logicalplan.ProjectionOptimizer{SeriesHashLabel: schema.SeriesHashLabel},
		},

		EngineOpts: promql.EngineOpts{
			Logger:                   nil,
			Reg:                      nil,
			MaxSamples:               10_000_000,
			Timeout:                  opts.defaultTimeout,
			NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
			EnableAtModifier:         true,
			EnableNegativeOffset:     true,
			EnablePerStepStats:       true,
			LookbackDelta:            opts.defaultLookback,
			EnableDelayedNameRemoval: false,
		},
	})
}

func setupThanosAPI(g *run.Group, log *slog.Logger, db *cfdb.DB, opts apiOpts, qOpts queryOpts) {
	server := grpc.NewServer(
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithPropagators(autoprop.NewTextMapPropagator()))),
		grpc.UnaryInterceptor(cfgrpc.ServerMetrics.UnaryServerInterceptor()),
		grpc.StreamInterceptor(cfgrpc.ServerMetrics.StreamServerInterceptor()),
	)

	queryServer := cfgrpc.NewQueryServer(
		db,
		engineFromQueryOpts(qOpts),
		cfgrpc.ConcurrentQueryQuota(qOpts.concurrentQueryQuota),
		cfgrpc.SelectChunkBytesQuota(qOpts.selectChunkBytesQuota),
		cfgrpc.SelectRowCountQuota(qOpts.selectRowCountQuota),
		cfgrpc.SelectChunkPartitionMaxRange(qOpts.selectChunkPartitionMaxRange),
		cfgrpc.SelectChunkPartitionMaxGap(qOpts.selectChunkPartitionMaxGap),
		cfgrpc.SelectChunkPartitionMaxConcurrency(qOpts.selectChunkPartitionMaxConcurrency),
	)

	infopb.RegisterInfoServer(server, queryServer)
	storepb.RegisterStoreServer(server, queryServer)
	querypb.RegisterQueryServer(server, queryServer)

	reflection.Register(server)

	g.Add(func() error {
		log.Info("Serving thanos api", slog.Int("port", opts.port))

		l, err := net.Listen("tcp", fmt.Sprintf(":%d", opts.port))
		if err != nil {
			return fmt.Errorf("unable to listen: %w", err)
		}
		return server.Serve(l)
	}, func(error) {
		log.Info("Shutting down thanos api", slog.Int("port", opts.port))
		ctx, cancel := context.WithTimeout(context.Background(), opts.shutdownTimeout)
		defer cancel()

		stopped := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(stopped)
		}()

		select {
		case <-ctx.Done():
			server.Stop()
			return
		case <-stopped:
			cancel()
		}
	})
}

func setupPromAPI(g *run.Group, log *slog.Logger, bkt objstore.Bucket, db *cfdb.DB, opts apiOpts, qOpts queryOpts, cOpts cardinalityOpts, corsOrigins []string) error {
	apiOpts := []cfhttp.APIOption{
		cfhttp.QueryOptions(
			cfhttp.DefaultStep(qOpts.defaultStep),
			cfhttp.DefaultLookback(qOpts.defaultLookback),
			cfhttp.DefaultTimeout(qOpts.defaultTimeout),
			cfhttp.ConcurrentQueryQuota(qOpts.concurrentQueryQuota),
			cfhttp.SelectChunkBytesQuota(qOpts.selectChunkBytesQuota),
			cfhttp.SelectRowCountQuota(qOpts.selectRowCountQuota),
			cfhttp.SelectChunkPartitionMaxRange(qOpts.selectChunkPartitionMaxRange),
			cfhttp.SelectChunkPartitionMaxGap(qOpts.selectChunkPartitionMaxGap),
			cfhttp.SelectChunkPartitionMaxConcurrency(qOpts.selectChunkPartitionMaxConcurrency),
		),
	}

	var cardSvc cardinality.Service
	if cOpts.enabled {
		svcOpts := []cardinality.ServiceOption{
			cardinality.WithConcurrency(cOpts.concurrency),
		}

		if cOpts.preloadEnabled {
			svcOpts = append(svcOpts, cardinality.WithPreloader(cardinality.PreloaderConfig{
				Days:        cOpts.preloadDays,
				Concurrency: cOpts.preloadConcurrency,
				Interval:    cOpts.preloadInterval,
			}))
		}

		var err error
		cardSvc, err = cardinality.NewService(bkt, cOpts.cacheDir, svcOpts...)
		if err != nil {
			return fmt.Errorf("create cardinality service: %w", err)
		}
		apiOpts = append(apiOpts, cfhttp.CardinalityService(cardSvc))
		log.Info("Cardinality explorer enabled",
			slog.String("cache_dir", cOpts.cacheDir),
			slog.Int("concurrency", cOpts.concurrency),
			slog.Duration("timeout", cOpts.timeout),
			slog.Bool("preloader", cOpts.preloadEnabled),
		)
		if cOpts.preloadEnabled {
			log.Info("Cardinality preloader started",
				slog.Int("days", cOpts.preloadDays),
				slog.Int("concurrency", cOpts.preloadConcurrency),
				slog.Duration("interval", cOpts.preloadInterval),
			)
		}
	}

	if len(corsOrigins) > 0 {
		apiOpts = append(apiOpts, cfhttp.CORSAllowedOrigins(corsOrigins))
		log.Info("CORS enabled", slog.Any("allowed_origins", corsOrigins))
	}

	handler := cfhttp.NewAPI(db, engineFromQueryOpts(qOpts), apiOpts...)

	server := &http.Server{Addr: fmt.Sprintf(":%d", opts.port), Handler: handler}
	g.Add(func() error {
		log.Info("Serving prometheus api", slog.Int("port", opts.port))
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	}, func(error) {
		log.Info("Shutting down prometheus api", slog.Int("port", opts.port))
		ctx, cancel := context.WithTimeout(context.Background(), opts.shutdownTimeout)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Error("Error shutting down prometheus server", slog.Any("err", err))
		}

		if cardSvc != nil {
			if err := cardSvc.Close(); err != nil {
				log.Error("Error closing cardinality service", slog.Any("err", err))
			}
		}
	})

	return nil
}
