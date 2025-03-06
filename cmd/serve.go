// Copyright (c) 2025 Cloudflare, Inc.
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
	"net/http/pprof"
	"time"

	"github.com/alecthomas/units"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/runutil"

	_ "github.com/mostynb/go-grpc-compression/snappy"

	cfgrpc "github.com/cloudflare/parquet-tsdb-poc/api/grpc"
	cfhttp "github.com/cloudflare/parquet-tsdb-poc/api/http"
	cfdb "github.com/cloudflare/parquet-tsdb-poc/db"
)

type serveOpts struct {
	block   blockOpts
	bucket  bucketOpts
	tracing tracingOpts

	query       queryOpts
	promAPI     promAPIOpts
	thanosAPI   thanosAPIOpts
	internalAPI internalAPIOpts
}

func (opts *serveOpts) registerFlags(cmd *kingpin.CmdClause) {
	opts.block.registerFlags(cmd)
	opts.bucket.registerFlags(cmd)
	opts.tracing.registerFlags(cmd)
	opts.query.registerFlags(cmd)
	opts.promAPI.registerFlags(cmd)
	opts.thanosAPI.registerFlags(cmd)
	opts.internalAPI.registerFlags(cmd)
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

		discoverer, err := setupDiscovery(ctx, &g, log, bkt, opts.block)
		if err != nil {
			return fmt.Errorf("unable to setup discovery: %w", err)
		}

		metaFilter, err := setupMetaFilter(ctx, &g, log, opts.block)
		if err != nil {
			return fmt.Errorf("unable to set up meta filter: %w", err)
		}

		syncer, err := setupSyncer(ctx, &g, log, bkt, discoverer, metaFilter, opts.block)
		if err != nil {
			return fmt.Errorf("unable to setup syncer: %w", err)
		}

		db := cfdb.NewDB(
			syncer,
			cfdb.ExternalLabels(labels.FromMap(opts.query.externalLabels)),
		)

		setupPromAPI(&g, log, db, opts.promAPI, opts.query)
		setupThanosAPI(&g, log, db, opts.thanosAPI, opts.query)
		setupInternalAPI(&g, log, reg, opts.internalAPI)

		return g.Run()
	}
}

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

type queryOpts struct {
	defaultStep     time.Duration
	defaultLookback time.Duration
	defaultTimeout  time.Duration
	externalLabels  map[string]string
}

func (opts *queryOpts) registerFlags(cmd *kingpin.CmdClause) {
	// We need to initialize the externalLabels map here
	opts.externalLabels = make(map[string]string)

	cmd.Flag("query.step", "default step for range queries").Default("30s").DurationVar(&opts.defaultStep)
	cmd.Flag("query.lookback", "default lookback for queries").Default("5m").DurationVar(&opts.defaultLookback)
	cmd.Flag("query.timeout", "default timeout for queries").Default("30s").DurationVar(&opts.defaultTimeout)
	cmd.Flag("query.external-label", "external label to add to results").StringMapVar(&opts.externalLabels)
}

func engineFromQueryOpts(opts queryOpts) promql.QueryEngine {
	return engine.New(engine.Opts{
		DisableFallback:             false,
		DisableDuplicateLabelChecks: true,
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

type blockOpts struct {
	discoveryInterval    time.Duration
	discoveryConcurrency int

	syncerInterval       time.Duration
	syncerConcurrency    int
	syncerReadBufferSize units.Base2Bytes

	filterType                         string
	filterThanosBackfillEndpoint       string
	filterThanosBackfillUpdateInterval time.Duration
}

func (opts *blockOpts) registerFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("block.discovery.interval", "interval to discover blocks").Default("1m").DurationVar(&opts.discoveryInterval)
	cmd.Flag("block.discovery.concurrency", "concurrency for loading metadata").Default("1").IntVar(&opts.discoveryConcurrency)
	cmd.Flag("block.syncer.interval", "interval to sync blocks").Default("1m").DurationVar(&opts.syncerInterval)
	cmd.Flag("block.syncer.concurrency", "concurrency for loading blocks").Default("1").IntVar(&opts.syncerConcurrency)
	cmd.Flag("block.syncer.read-buffer-size", "read buffer size for blocks").Default("2MiB").BytesVar(&opts.syncerReadBufferSize)
	cmd.Flag("block.filter.type", "").Default("all-metas").EnumVar(&opts.filterType, "thanos-backfill", "all-metas")
	cmd.Flag("block.filter.thanos-backfill.endpoint", "endpoint to ignore for backfill").StringVar(&opts.filterThanosBackfillEndpoint)
	cmd.Flag("block.filter.thanos-backfill.interval", "interval to update thanos-backfill timerange").Default("1m").DurationVar(&opts.filterThanosBackfillUpdateInterval)
}

func setupDiscovery(ctx context.Context, g *run.Group, log *slog.Logger, bkt objstore.Bucket, opts blockOpts) (*cfdb.Discoverer, error) {
	discoverer := cfdb.NewDiscoverer(bkt, cfdb.MetaConcurrency(opts.discoveryConcurrency))

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

func setupMetaFilter(ctx context.Context, g *run.Group, log *slog.Logger, opts blockOpts) (cfdb.MetaFilter, error) {
	switch opts.filterType {
	case "all-metas":
		return cfdb.AllMetasMetaFilter, nil
	case "thanos-backfill":
		thanosBackfillMetaFilter := cfdb.NewThanosBackfillMetaFilter(opts.filterThanosBackfillEndpoint)

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

func setupSyncer(ctx context.Context, g *run.Group, log *slog.Logger, bkt objstore.Bucket, discoverer *cfdb.Discoverer, metaFilter cfdb.MetaFilter, opts blockOpts) (*cfdb.Syncer, error) {
	syncer := cfdb.NewSyncer(
		bkt,
		cfdb.FilterMetas(metaFilter),
		cfdb.BlockConcurrency(opts.syncerConcurrency),
		cfdb.BlockOptions(
			cfdb.ReadBufferSize(opts.syncerReadBufferSize),
		),
	)

	log.Info("Running initial sync")

	iterCtx, iterCancel := context.WithTimeout(ctx, opts.syncerInterval)
	defer iterCancel()
	if err := syncer.Sync(iterCtx, discoverer.Metas()); err != nil {
		return nil, fmt.Errorf("unable to run initial discovery: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		return runutil.Repeat(opts.syncerInterval, ctx.Done(), func() error {
			log.Debug("Running sync")

			iterCtx, iterCancel := context.WithTimeout(ctx, opts.syncerInterval)
			defer iterCancel()
			if err := syncer.Sync(iterCtx, discoverer.Metas()); err != nil {
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

type thanosAPIOpts struct {
	port int

	shutdownTimeout time.Duration
}

func (opts *thanosAPIOpts) registerFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("http.thanos.port", "port to host query api").Default("9001").IntVar(&opts.port)
	cmd.Flag("http.thanos.shutdown-timeout", "timeout on shutdown").Default("10s").DurationVar(&opts.shutdownTimeout)
}

func setupThanosAPI(g *run.Group, log *slog.Logger, db *cfdb.DB, opts thanosAPIOpts, qOpts queryOpts) {
	server := grpc.NewServer(
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.MaxRecvMsgSize(math.MaxInt32),
	)

	infopb.RegisterInfoServer(server, cfgrpc.NewInfoServer(db))
	querypb.RegisterQueryServer(server, cfgrpc.NewQueryServer(db, engineFromQueryOpts(qOpts)))

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

type promAPIOpts struct {
	port int

	shutdownTimeout time.Duration
}

func (opts *promAPIOpts) registerFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("http.prometheus.port", "port to host query api").Default("9000").IntVar(&opts.port)
	cmd.Flag("http.prometheus.shutdown-timeout", "timeout on shutdown").Default("10s").DurationVar(&opts.shutdownTimeout)
}

func setupPromAPI(g *run.Group, log *slog.Logger, db *cfdb.DB, opts promAPIOpts, qOpts queryOpts) {
	handler := cfhttp.NewAPI(db.Queryable(), engineFromQueryOpts(qOpts),
		cfhttp.QueryOptions(
			cfhttp.DefaultStep(qOpts.defaultStep),
			cfhttp.DefaultLookback(qOpts.defaultLookback),
			cfhttp.DefaultTimeout(qOpts.defaultTimeout),
		))

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
	})
}

type internalAPIOpts struct {
	port int

	shutdownTimeout time.Duration
}

func (opts *internalAPIOpts) registerFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("http.internal.port", "port to host query api").Default("6060").IntVar(&opts.port)
	cmd.Flag("http.internal.shutdown-timeout", "timeout on shutdown").Default("10s").DurationVar(&opts.shutdownTimeout)
}

func setupInternalAPI(g *run.Group, log *slog.Logger, reg *prometheus.Registry, opts internalAPIOpts) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	})
	mux.HandleFunc("/-/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
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
