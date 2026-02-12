// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/oklog/run"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/thanos-io/thanos-parquet-gateway/convert"
	"github.com/thanos-io/thanos-parquet-gateway/internal/slogerrcapture"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/locate"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

const (
	conversionModeBoth       string = "both"
	conversionModeHistorical string = "historical"
	conversionModePartition  string = "partition"
)

type convertOpts struct {
	parquetBucket   bucketOpts
	tsdbBucket      bucketOpts
	parquetDiscover discoveryOpts
	tsdbDiscover    tsdbDiscoveryOpts
	conversion      conversionOpts
	internalAPI     apiOpts
}

type conversionOpts struct {
	mode string

	partitionRunInterval time.Duration
	partitionRunTimeout  time.Duration
	partitionMaxSteps    int
	partitionDuration    time.Duration
	partitionLookback    time.Duration

	historicalRunInterval time.Duration
	historicalRunTimeout  time.Duration
	historicalMaxSteps    int

	retryInterval            time.Duration
	gracePeriod              time.Duration
	recompress               bool
	sortLabels               []string
	rowGroupSize             int
	rowGroupCount            int
	downloadConcurrency      int
	blockDownloadConcurrency int
	encodingConcurrency      int
	writeConcurrency         int
	tempDir                  string
}

func (opts *conversionOpts) isPartition() bool {
	return opts.mode == conversionModeBoth || opts.mode == conversionModePartition
}

func (opts *conversionOpts) isHistorical() bool {
	return opts.mode == conversionModeBoth || opts.mode == conversionModeHistorical
}

func (opts *convertOpts) registerFlags(cmd *kingpin.CmdClause) {
	opts.conversion.registerFlags(cmd)
	opts.parquetBucket.registerConvertParquetFlags(cmd)
	opts.tsdbBucket.registerConvertTSDBFlags(cmd)
	opts.parquetDiscover.registerConvertParquetFlags(cmd)
	opts.tsdbDiscover.registerConvertTSDBFlags(cmd)
	opts.internalAPI.registerConvertFlags(cmd)
}

func (opts *conversionOpts) registerFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("convert.mode", "conversion mode: 'both' runs historical and partition loops, 'historical' runs only historical daily conversion, 'partition' runs only partition conversion").
		Default(conversionModeHistorical).
		EnumVar(&opts.mode, conversionModeBoth, conversionModeHistorical, conversionModePartition)

	cmd.Flag("convert.partition.run-interval", "how often to run partition conversion cycle").Default("30m").DurationVar(&opts.partitionRunInterval)
	cmd.Flag("convert.partition.run-timeout", "max duration for a single partition conversion cycle").Default("2h").DurationVar(&opts.partitionRunTimeout)
	cmd.Flag("convert.partition.max-steps", "max partitions to convert per cycle (0 = unlimited)").Default("1").IntVar(&opts.partitionMaxSteps)
	cmd.Flag("convert.partition.duration", "size of each partition; smaller values mean fresher data but more files (must evenly divide 24h)").Default("2h").DurationVar(&opts.partitionDuration)
	cmd.Flag("convert.partition.lookback", "time window from now to consider for partition conversion; data older than this is handled by historical conversion").Default("24h").DurationVar(&opts.partitionLookback)

	cmd.Flag("convert.historical.run-interval", "how often to run the historical daily conversion cycle").Default("1h").DurationVar(&opts.historicalRunInterval)
	cmd.Flag("convert.historical.run-timeout", "max duration for a single historical conversion cycle").Default("24h").DurationVar(&opts.historicalRunTimeout)
	cmd.Flag("convert.historical.max-steps", "max days to convert per cycle").Default("2").IntVar(&opts.historicalMaxSteps)

	cmd.Flag("convert.retry-interval", "interval to retry a single conversion after an error").Default("1m").DurationVar(&opts.retryInterval)
	cmd.Flag("convert.tempdir", "directory for temporary state").Default(os.TempDir()).StringVar(&opts.tempDir)
	cmd.Flag("convert.recompress", "recompress chunks").Default("true").BoolVar(&opts.recompress)
	cmd.Flag("convert.grace-period", "dont convert for dates younger than this").Default("48h").DurationVar(&opts.gracePeriod)

	cmd.Flag("convert.rowgroup.size", "size of rowgroups").Default("1_000_000").IntVar(&opts.rowGroupSize)
	cmd.Flag("convert.rowgroup.count", "rowgroups per shard").Default("6").IntVar(&opts.rowGroupCount)
	cmd.Flag("convert.sorting.label", "label to sort by").Default("__name__").StringsVar(&opts.sortLabels)
	cmd.Flag("convert.download.concurrency", "concurrency for downloading files in parallel per tsdb block").Default("4").IntVar(&opts.downloadConcurrency)
	cmd.Flag("convert.download.block-concurrency", "concurrency for downloading & opening multiple blocks in parallel").Default("1").IntVar(&opts.blockDownloadConcurrency)
	cmd.Flag("convert.encoding.concurrency", "concurrency for encoding chunks").Default("4").IntVar(&opts.encodingConcurrency)
	cmd.Flag("convert.write.concurrency", "concurrency for writer").Default("4").IntVar(&opts.writeConcurrency)
}

func (opts *bucketOpts) registerConvertParquetFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("parquet.objstore-config-file", "YAML file that contains object store configuration for parquet storage. See format details: https://thanos.io/tip/thanos/storage.md/#configuration").StringVar(&opts.objStoreConfigFile)
	cmd.Flag("parquet.objstore-config", "Alternative to 'parquet.objstore-config-file'. YAML content for parquet storage configuration.").StringVar(&opts.objStoreConfig)
}

func (opts *bucketOpts) registerConvertTSDBFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("tsdb.objstore-config-file", "YAML file that contains object store configuration for TSDB storage. See format details: https://thanos.io/tip/thanos/storage.md/#configuration").StringVar(&opts.objStoreConfigFile)
	cmd.Flag("tsdb.objstore-config", "Alternative to 'tsdb.objstore-config-file'. YAML content for TSDB storage configuration.").StringVar(&opts.objStoreConfig)
}

func (opts *discoveryOpts) registerConvertParquetFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("parquet.discovery.interval", "interval to discover blocks").Default("30m").DurationVar(&opts.discoveryInterval)
	cmd.Flag("parquet.discovery.concurrency", "concurrency for loading metadata").Default("1").IntVar(&opts.discoveryConcurrency)
}

func (opts *tsdbDiscoveryOpts) registerConvertTSDBFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("tsdb.discovery.interval", "interval to discover blocks").Default("30m").DurationVar(&opts.discoveryInterval)
	cmd.Flag("tsdb.discovery.concurrency", "concurrency for loading metadata").Default("1").IntVar(&opts.discoveryConcurrency)
	cmd.Flag("tsdb.discovery.min-block-age", "blocks that have metrics that are youner then this won't be loaded").Default("0s").DurationVar(&opts.discoveryMinBlockAge)
	MatchersVar(cmd.Flag("tsdb.discovery.select-external-labels", "only external labels matching this selector will be discovered").PlaceHolder("SELECTOR"), &opts.externalLabelMatchers)
}

func (opts *apiOpts) registerConvertFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("http.internal.port", "port to host query api").Default("6060").IntVar(&opts.port)
	cmd.Flag("http.internal.shutdown-timeout", "timeout on shutdown").Default("10s").DurationVar(&opts.shutdownTimeout)
}

func registerConvertApp(app *kingpin.Application) (*kingpin.CmdClause, func(context.Context, *slog.Logger, *prometheus.Registry) error) {
	cmd := app.Command("convert", "convert TSDB Block to parquet file")

	var opts convertOpts
	opts.registerFlags(cmd)

	return cmd, func(ctx context.Context, log *slog.Logger, reg *prometheus.Registry) error {
		var g run.Group
		var partitionTsdbDiscoverer, histTsdbDiscoverer *locate.TSDBDiscoverer
		var partitionParquetDiscoverer, histParquetDiscoverer *locate.Discoverer

		setupInterrupt(ctx, &g, log)
		setupInternalAPI(&g, log, reg, opts.internalAPI)

		// init buckets
		tsdbBkt, err := setupBucket(log, opts.tsdbBucket)
		if err != nil {
			return fmt.Errorf("unable to setup tsdb bucket: %s", err)
		}
		parquetBkt, err := setupBucket(log, opts.parquetBucket)
		if err != nil {
			return fmt.Errorf("unable to setup parquet bucket: %s", err)
		}

		// init discoverers (separate for each loop to avoid concurrent map access)
		if opts.conversion.isPartition() {
			partitionTsdbDiscoverer, partitionParquetDiscoverer = newDiscoverers(tsdbBkt, parquetBkt, log, opts.tsdbDiscover, opts.parquetDiscover)
		}
		if opts.conversion.isHistorical() {
			histTsdbDiscoverer, histParquetDiscoverer = newDiscoverers(tsdbBkt, parquetBkt, log, opts.tsdbDiscover, opts.parquetDiscover)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if opts.conversion.isPartition() {
			g.Add(func() error {
				return runPartitionLoop(ctx, log, tsdbBkt, parquetBkt,
					partitionTsdbDiscoverer, partitionParquetDiscoverer,
					opts.conversion)
			}, func(error) {
				log.With(slog.String("mode", conversionModePartition)).Info("Stopping conversion")
				cancel()
			})
		}

		if opts.conversion.isHistorical() {
			g.Add(func() error {
				return runHistoricalLoop(ctx, log, tsdbBkt, parquetBkt,
					histTsdbDiscoverer, histParquetDiscoverer,
					opts.conversion)
			}, func(error) {
				log.With(slog.String("mode", conversionModeHistorical)).Info("Stopping conversion")
				cancel()
			})
		}

		return g.Run()
	}
}

// newDiscoverers creates a pair of discoverers for TSDB and parquet.
func newDiscoverers(
	tsdbBkt, parquetBkt objstore.Bucket,
	log *slog.Logger,
	tsdbOpts tsdbDiscoveryOpts,
	parquetOpts discoveryOpts,
) (*locate.TSDBDiscoverer, *locate.Discoverer) {
	tsdbDiscoverer := locate.NewTSDBDiscoverer(
		tsdbBkt,
		locate.TSDBMetaConcurrency(tsdbOpts.discoveryConcurrency),
		locate.TSDBMinBlockAge(tsdbOpts.discoveryMinBlockAge),
		locate.TSDBMatchExternalLabels(tsdbOpts.externalLabelMatchers...),
		locate.WithLogger(log),
	)
	parquetDiscoverer := locate.NewDiscoverer(
		parquetBkt,
		locate.MetaConcurrency(parquetOpts.discoveryConcurrency),
		locate.Logger(log),
	)
	return tsdbDiscoverer, parquetDiscoverer
}

// runPartitionLoop runs the partition conversion loop.
func runPartitionLoop(
	ctx context.Context,
	log *slog.Logger,
	tsdbBkt, parquetBkt objstore.Bucket,
	tsdbDiscoverer *locate.TSDBDiscoverer,
	parquetDiscoverer *locate.Discoverer,
	opts conversionOpts,
) error {
	l := log.With(slog.String("mode", conversionModePartition))
	l.Info("Starting conversion loop", slog.String("interval", opts.partitionRunInterval.String()))

	return runutil.Repeat(opts.partitionRunInterval, ctx.Done(), func() error {
		iterCtx, iterCancel := context.WithTimeout(ctx, opts.partitionRunTimeout)
		defer iterCancel()

		if err := runutil.Retry(opts.retryInterval, iterCtx.Done(), func() error {
			tsdbStreams, parquetStreams, err := runDiscovery(iterCtx, l, tsdbDiscoverer, parquetDiscoverer)
			if err != nil {
				l.Error("Discovery failed, skipping conversion cycle", slog.Any("error", err))
				return nil // skip cycle on discovery failure
			}

			notAfter := time.Now().Add(-opts.gracePeriod)
			planner := convert.NewPlannerWithPartitions(notAfter, 1, opts.partitionDuration, opts.partitionMaxSteps, opts.partitionLookback)
			plan := planner.PlanPartitions(tsdbStreams, parquetStreams)

			if err := advanceConversion(iterCtx, l, tsdbBkt, parquetBkt, plan, parquetStreams, opts, conversionModePartition, false); err != nil {
				l.Error("Unable to convert", "error", err)
				return err
			}
			return nil
		}); err != nil {
			l.Warn("Error during conversion", slog.Any("err", err))
			return nil
		}
		return nil
	})
}

// runHistoricalLoop runs the historical conversion loop.
func runHistoricalLoop(
	ctx context.Context,
	log *slog.Logger,
	tsdbBkt, parquetBkt objstore.Bucket,
	tsdbDiscoverer *locate.TSDBDiscoverer,
	parquetDiscoverer *locate.Discoverer,
	opts conversionOpts,
) error {
	l := log.With(slog.String("mode", conversionModeHistorical))
	l.Info("Starting conversion loop", slog.String("interval", opts.historicalRunInterval.String()))

	return runutil.Repeat(opts.historicalRunInterval, ctx.Done(), func() error {
		iterCtx, iterCancel := context.WithTimeout(ctx, opts.historicalRunTimeout)
		defer iterCancel()

		if err := runutil.Retry(opts.retryInterval, iterCtx.Done(), func() error {
			tsdbStreams, parquetStreams, err := runDiscovery(iterCtx, l, tsdbDiscoverer, parquetDiscoverer)
			if err != nil {
				l.Error("Discovery failed, skipping conversion cycle", slog.Any("error", err))
				return nil // skip cycle on discovery failure
			}

			notAfter := time.Now().Add(-opts.gracePeriod)
			planner := convert.NewPlannerWithPartitions(notAfter, opts.historicalMaxSteps, opts.partitionDuration, 0, opts.partitionLookback)
			plan := planner.PlanHistorical(tsdbStreams, parquetStreams)

			if err := advanceConversion(iterCtx, l, tsdbBkt, parquetBkt, plan, parquetStreams, opts, conversionModeHistorical, true); err != nil {
				l.Error("Unable to convert", "error", err)
				return err
			}
			return nil
		}); err != nil {
			l.Warn("Error during conversion", slog.Any("err", err))
			return nil
		}
		return nil
	})
}

// runDiscovery runs TSDB and parquet discovery in parallel.
func runDiscovery(
	ctx context.Context,
	log *slog.Logger,
	tsdbDiscoverer *locate.TSDBDiscoverer,
	parquetDiscoverer *locate.Discoverer,
) (map[schema.ExternalLabelsHash]schema.TSDBBlocksStream, map[schema.ExternalLabelsHash]schema.ParquetBlocksStream, error) {
	dg, dCtx := errgroup.WithContext(ctx)
	dg.Go(func() error {
		log.Debug("Running TSDB discovery")
		if err := tsdbDiscoverer.Discover(dCtx); err != nil {
			return fmt.Errorf("TSDB discovery failed: %w", err)
		}
		return nil
	})
	dg.Go(func() error {
		log.Debug("Running parquet discovery")
		if err := parquetDiscoverer.Discover(dCtx); err != nil {
			return fmt.Errorf("parquet discovery failed: %w", err)
		}
		return nil
	})
	if err := dg.Wait(); err != nil {
		log.Warn("Discovery failed, skipping cycle to prevent incorrect data", "error", err)
		return nil, nil, err
	}
	return tsdbDiscoverer.Streams(), parquetDiscoverer.Streams(), nil
}

func advanceConversion(
	ctx context.Context,
	log *slog.Logger,
	tsdbBkt objstore.Bucket,
	parquetBkt objstore.Bucket,
	plan convert.Plan,
	parquetStreams map[schema.ExternalLabelsHash]schema.ParquetBlocksStream,
	opts conversionOpts,
	tempDirSuffix string,
	cleanupStale bool,
) error {
	blkDir := filepath.Join(opts.tempDir, ".blocks-"+tempDirSuffix)
	bufferDir := filepath.Join(opts.tempDir, ".buffers-"+tempDirSuffix)

	log.Info("Cleaning up previous state", "block_directory", blkDir, "buffer_directory", bufferDir)
	if err := cleanupDirectory(blkDir); err != nil {
		return fmt.Errorf("unable to clean up block directory: %w", err)
	}
	if err := cleanupDirectory(bufferDir); err != nil {
		return fmt.Errorf("unable to clean up buffer directory: %w", err)
	}

	if len(plan.Steps) == 0 {
		log.Info("Nothing to do")
		return nil
	}

	log.Info("Planned steps to convert", slog.Int("steps", len(plan.Steps)))
	for _, step := range plan.Steps {
		attrs := []any{
			slog.String("date", step.Date.String()),
			slog.Any("ulids", ulidsFromMetas(step.Sources)),
		}
		if step.Partition != nil {
			attrs = append(attrs, slog.String("partition", step.Partition.String()))
		}
		log.Info("Plan step", attrs...)
	}

	log.Info("Starting conversions")
	var stepBlocks, prevBlocks []convert.Convertible
	var err error
	for _, step := range plan.Steps {
		// Close blocks that were open in the previous step but are no longer needed.
		prevBlocks = closeUnused(log, step.Sources, prevBlocks)

		ulids := ulidsFromMetas(step.Sources)
		log.Info("Converting", slog.String("step", step.String()), slog.Any("ulids", ulids))

		toDownload := blocksToDownload(step.Sources, prevBlocks)
		log.Info("Blocks from previous step", slog.Any("ulids", ulidsFromConvertible(prevBlocks)))
		log.Info("Blocks to download", slog.Any("ulids", ulidsFromMetas(toDownload)))
		stepBlocks, err = downloadedBlocks(ctx, log, tsdbBkt, toDownload, blkDir, opts)
		if err != nil {
			// NOTE: we might have managed to open a few blocks, make sure to close them too.
			closeBlocks(log, prevBlocks...)
			closeBlocks(log, stepBlocks...)
			return fmt.Errorf("unable to download tsdb blocks: %w", err)
		}
		log.Info("Blocks downloaded", slog.Int("count", len(stepBlocks)))
		// So far stepBlocks is only blocks we needed to download, add blocks already downloaded in previous step.
		stepBlocks = append(stepBlocks, prevBlocks...)

		for _, blk := range stepBlocks {
			meta := blk.Meta()
			log.Info("TSDB block details",
				slog.String("ulid", meta.ULID.String()),
				slog.String("minTime", time.UnixMilli(meta.MinTime).UTC().Format(time.RFC3339)),
				slog.String("maxTime", time.UnixMilli(meta.MaxTime).UTC().Format(time.RFC3339)),
				slog.Uint64("series", meta.Stats.NumSeries),
				slog.Uint64("chunks", meta.Stats.NumChunks),
				slog.Uint64("samples", meta.Stats.NumSamples),
			)
		}

		log.Info("Starting conversion", slog.String("step", step.String()))
		convOpts := []convert.ConvertOption{
			convert.SortBy(opts.sortLabels...),
			convert.RowGroupSize(opts.rowGroupSize),
			convert.RowGroupCount(opts.rowGroupCount),
			convert.EncodingConcurrency(opts.encodingConcurrency),
			convert.WriteConcurrency(opts.writeConcurrency),
			convert.ChunkBufferPool(parquet.NewFileBufferPool(bufferDir, "chunkbuf-*")),
		}
		if err := convert.ConvertTSDBBlock(ctx, parquetBkt, step.Date, step.Partition, step.ExternalLabels.Hash(), stepBlocks, convOpts...); err != nil {
			closeBlocks(log, stepBlocks...)
			return fmt.Errorf("unable to convert blocks for %q: %s", step.String(), err)
		}
		if err := convert.WriteStreamFile(ctx, parquetBkt, step.ExternalLabels); err != nil {
			closeBlocks(log, stepBlocks...)
			return fmt.Errorf("unable to write stream file: %w", err)
		}
		log.Info("Conversion completed", slog.String("step", step.String()))

		prevBlocks = stepBlocks
	}
	log.Info("Plan completed")

	if cleanupStale {
		cleanupStalePartitions(ctx, log, parquetBkt, parquetStreams)
	}
	closeBlocks(log, prevBlocks...)
	return nil
}

func closeBlocks(log *slog.Logger, openBlocks ...convert.Convertible) {
	for _, blk := range openBlocks {
		log.Info("Closing open block", slog.String("ulid", blk.Meta().ULID.String()))
		if err := blk.Close(); err != nil {
			log.Warn("Unable to close block", slog.String("block", blk.Meta().ULID.String()), slog.Any("err", err))
		}

		log.Info("Removing block directory", slog.String("block", blk.Meta().ULID.String()), slog.String("dir", blk.Dir()))
		if err := os.RemoveAll(blk.Dir()); err != nil {
			log.Warn("Unable to remove block directory", slog.String("block", blk.Meta().ULID.String()), slog.Any("err", err))
		}
	}
}

// closeUnused takes the list of TSDB metas used to convert given step and a list of already
// open blocks, it will look for blocks that are no longer needed, close them, and then return
// the list of blocks that are still used.
func closeUnused(log *slog.Logger, metas []metadata.Meta, openBlocks []convert.Convertible) []convert.Convertible {
	used := make([]convert.Convertible, 0, len(openBlocks))
L:
	for _, openBlock := range openBlocks {
		for _, meta := range metas {
			if meta.ULID == openBlock.Meta().ULID {
				used = append(used, openBlock)
				continue L
			}
		}
		log.Info("Block no longer needed, closing", slog.String("block", openBlock.Meta().ULID.String()))
		closeBlocks(log, openBlock)
	}
	return used
}

// blocksToDownload takes the list of TSDB metas used to convert given step and a list of already
// open blocks and returns a list of blocks that need downloading.
func blocksToDownload(metas []metadata.Meta, openBlocks []convert.Convertible) []metadata.Meta {
	pending := make([]metadata.Meta, 0, len(metas))
L:
	for _, meta := range metas {
		for _, openBlock := range openBlocks {
			if meta.ULID == openBlock.Meta().ULID {
				continue L
			}
		}
		pending = append(pending, meta)
	}
	return pending
}

func cleanupDirectory(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("unable to delete directory: %w", err)
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("unable to recreate block directory: %w", err)
	}
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("unable to stat block directory: %w", err)
	}
	return nil
}

func ulidsFromMetas(metas []metadata.Meta) []string {
	res := make([]string, len(metas))
	for i := range metas {
		res[i] = metas[i].ULID.String()
	}
	return res
}

func ulidsFromConvertible(blocks []convert.Convertible) []string {
	res := make([]string, len(blocks))
	for i := range blocks {
		res[i] = blocks[i].Meta().ULID.String()
	}
	return res
}

func downloadedBlocks(ctx context.Context, log *slog.Logger, bkt objstore.BucketReader, metas []metadata.Meta, blkDir string, opts conversionOpts) ([]convert.Convertible, error) {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.blockDownloadConcurrency)

	mu := sync.Mutex{}
	res := make([]convert.Convertible, 0)
	for _, m := range metas {
		g.Go(func() error {
			src := m.ULID.String()
			dst := filepath.Join(blkDir, src)

			log.Debug("block download start", "ulid", src)

			if err := runutil.Retry(5*time.Second, ctx.Done(), func() error {
				return downloadBlock(ctx, bkt, m, blkDir, opts, log)
			}); err != nil {
				return fmt.Errorf("unable to download block %q: %w", src, err)
			}
			blk, err := tsdb.OpenBlock(log, dst, chunkenc.NewPool(), tsdb.DefaultPostingsDecoderFactory)
			if err != nil {
				return fmt.Errorf("unable to open block %q: %w", m.ULID, err)
			}
			mu.Lock()
			res = append(res, blk)
			mu.Unlock()

			log.Debug("block download complete", "ulid", src)
			return nil

		})
	}
	if err := g.Wait(); err != nil {
		return res, err
	}
	return res, nil
}

func downloadBlock(ctx context.Context, bkt objstore.BucketReader, meta metadata.Meta, blkDir string, opts conversionOpts, l *slog.Logger) error {
	src := meta.ULID.String()
	dst := filepath.Join(blkDir, src)

	fmap := make(map[string]metadata.File, len(meta.Thanos.Files))
	for _, fl := range meta.Thanos.Files {
		if fl.SizeBytes == 0 || fl.RelPath == "" {
			continue
		}
		fmap[fl.RelPath] = fl
	}

	// order is not guaranteed in "Iter" so we need to create directory structure beforehand
	if err := os.MkdirAll(dst, 0750); err != nil {
		return fmt.Errorf("unable to create block directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dst, "chunks"), 0750); err != nil {
		return fmt.Errorf("unable to create chunks directory: %w", err)
	}

	// we reimplement download dir from objstore to skip the cleanup part on partial downloads
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.downloadConcurrency)

	err := bkt.Iter(ctx, src, func(name string) error {
		g.Go(func() error {
			dst := filepath.Join(dst, strings.TrimPrefix(name, src))
			if strings.HasSuffix(name, objstore.DirDelim) {
				return nil
			}
			// In case the previous upload failed we dont download the files that have the correct size.
			// Size is not the best indicator, but its good enough. ideally we would want a hash but we
			// dont write those currently.
			// If the file was corrupted, then opening the block will very likely fail anyway.
			if stat, err := os.Stat(dst); err == nil {
				if known, ok := fmap[strings.TrimPrefix(name, src+objstore.DirDelim)]; ok {
					if stat.Size() == known.SizeBytes && stat.Size() != 0 {
						return nil
					}
				}
			}

			rc, err := bkt.Get(ctx, name)
			if err != nil {
				return fmt.Errorf("unable to get file %q: %w", name, err)
			}
			defer slogerrcapture.Do(l, rc.Close, "closing %s", name)

			f, err := os.Create(dst)
			if err != nil {
				return fmt.Errorf("unable to create file %q: %w", dst, err)
			}
			if _, err := io.Copy(f, rc); err != nil {
				return fmt.Errorf("unable to copy file %q: %w", dst, err)
			}
			return nil
		})
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return fmt.Errorf("unable to iter bucket: %w", err)
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("unable to download directory: %w", err)
	}
	return nil
}

// cleanupPartitionsForDay removes all partition files under the parts/ directory for a given day and stream.
func cleanupPartitionsForDay(ctx context.Context, log *slog.Logger, bkt objstore.Bucket, day util.Date, extLabelsHash schema.ExternalLabelsHash) {
	// Build path with external labels hash prefix for multi-tenant support
	dayPath := schema.BlockNameForDay(day)
	if extLabelsHash != 0 {
		dayPath = fmt.Sprintf("%s/%s", extLabelsHash.String(), dayPath)
	}
	partsDir := dayPath + "/parts/"

	deleted := 0
	err := bkt.Iter(ctx, partsDir, func(name string) error {
		if err := bkt.Delete(ctx, name); err != nil {
			log.Warn("Failed to delete partition file", "file", name, "error", err)
		} else {
			deleted++
		}
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		log.Warn("Failed to iterate partition files", "dir", partsDir, "error", err)
		return
	}
	if deleted > 0 {
		log.Info("Cleaned up partitions", "day", dayPath, "files", deleted)
	}
}

// cleanupStalePartitions scans the bucket for partitions that have a corresponding daily block
// and deletes them. Only cleans partitions for days before yesterday to ensure serve component
// has had sufficient time to discover the daily block.
// Now properly handles multi-tenant scenarios by iterating over streams.
func cleanupStalePartitions(ctx context.Context, log *slog.Logger, bkt objstore.Bucket, parquetStreams map[schema.ExternalLabelsHash]schema.ParquetBlocksStream) {
	// Only clean partitions for days before yesterday.
	// This ensures at least 24-48 hours pass after daily block creation
	yesterday := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)

	for extLabelsHash, stream := range parquetStreams {
		if ctx.Err() != nil {
			return
		}

		// Find daily blocks that are old enough in this stream
		dailyBlocks := make(map[string]struct{})
		for _, meta := range stream.Metas {
			if schema.IsPartition(meta.Name) {
				continue
			}
			blockDay := time.UnixMilli(meta.Mint).UTC().Truncate(24 * time.Hour)
			if blockDay.Before(yesterday) {
				dailyBlocks[meta.Name] = struct{}{}
			}
		}

		if len(dailyBlocks) == 0 {
			continue
		}

		// Find partitions in this stream that should be cleaned up
		cleaned := make(map[string]struct{}) // track cleaned days to avoid duplicates
		for _, meta := range stream.Metas {
			if ctx.Err() != nil {
				return
			}
			if !schema.IsPartition(meta.Name) {
				continue
			}

			dailyName := schema.DailyBlockNameForPartition(meta.Name)
			if _, alreadyCleaned := cleaned[dailyName]; alreadyCleaned {
				continue
			}
			if _, hasDailyBlock := dailyBlocks[dailyName]; !hasDailyBlock {
				continue
			}

			day, err := util.DateFromString(dailyName)
			if err != nil {
				log.Warn("Failed to parse day from partition", "partition", meta.Name, "error", err)
				continue
			}

			log.Info("Cleaning up stale partitions for day with existing daily block",
				slog.String("day", day.String()),
				slog.String("daily_block", dailyName),
				slog.String("external_labels_hash", extLabelsHash.String()))
			cleanupPartitionsForDay(ctx, log, bkt, day, extLabelsHash)
			cleaned[dailyName] = struct{}{}
		}
	}
}
