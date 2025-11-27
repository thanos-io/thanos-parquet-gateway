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
	"sort"
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
	"github.com/thanos-io/thanos-parquet-gateway/locate"
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
	runInterval   time.Duration
	runTimeout    time.Duration
	retryInterval time.Duration

	gracePeriod              time.Duration
	maxDays                  int
	recompress               bool
	sortLabels               []string
	rowGroupSize             int
	rowGroupCount            int
	downloadConcurrency      int
	blockDownloadConcurrency int
	encodingConcurrency      int
	writeConcurrency         int

	tempDir string
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
	cmd.Flag("convert.run-interval", "interval to run conversion on").Default("1h").DurationVar(&opts.runInterval)
	cmd.Flag("convert.run-timeout", "timeout for a single conversion step").Default("24h").DurationVar(&opts.runTimeout)
	cmd.Flag("convert.retry-interval", "interval to retry a single conversion after an error").Default("1m").DurationVar(&opts.retryInterval)
	cmd.Flag("convert.tempdir", "directory for temporary state").Default(os.TempDir()).StringVar(&opts.tempDir)
	cmd.Flag("convert.recompress", "recompress chunks").Default("true").BoolVar(&opts.recompress)
	cmd.Flag("convert.grace-period", "dont convert for dates younger than this").Default("48h").DurationVar(&opts.gracePeriod)
	cmd.Flag("convert.max-plan-days", "soft limit for the number of days to plan conversions for").Default("2").IntVar(&opts.maxDays)

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

		setupInterrupt(ctx, &g, log)
		setupInternalAPI(&g, log, reg, opts.internalAPI)

		tsdbBkt, err := setupBucket(log, opts.tsdbBucket)
		if err != nil {
			return fmt.Errorf("unable to setup tsdb bucket: %s", err)
		}
		parquetBkt, err := setupBucket(log, opts.parquetBucket)
		if err != nil {
			return fmt.Errorf("unable to setup parquet bucket: %s", err)
		}

		tsdbDiscoverer, err := setupTSDBDiscovery(ctx, &g, log, tsdbBkt, opts.tsdbDiscover)
		if err != nil {
			return fmt.Errorf("unable to setup tsdb discovery: %s", err)
		}
		parquetDiscoverer, err := setupDiscovery(ctx, &g, log, parquetBkt, opts.parquetDiscover)
		if err != nil {
			return fmt.Errorf("unable to setup parquet discovery: %s", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			log.Info("Starting conversion", "sort_by", opts.conversion.sortLabels)
			return runutil.Repeat(opts.conversion.runInterval, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(ctx, opts.conversion.runTimeout)
				defer iterCancel()

				if err := runutil.Retry(opts.conversion.retryInterval, iterCtx.Done(), func() error {
					// Sync parquet files once here so we have the latest view
					log.Info("Discovering parquet blocks before conversion")
					if err := parquetDiscoverer.Discover(iterCtx); err != nil {
						log.Error("Unable to discover parquet blocks", "error", err)
						return err
					}
					log.Info("Converting next blocks", "sort_by", opts.conversion.sortLabels)
					if err := advanceConversion(iterCtx, log, tsdbBkt, parquetBkt, tsdbDiscoverer, parquetDiscoverer, opts.conversion); err != nil {
						log.Error("Unable to convert blocks", "error", err)
						return err
					}
					return nil
				}); err != nil {
					log.Warn("Error during conversion", slog.Any("err", err))
					return nil
				}
				return nil
			})
		}, func(error) {
			log.Info("Stopping conversion")
			cancel()
		})
		return g.Run()
	}
}

func advanceConversion(
	ctx context.Context,
	log *slog.Logger,
	tsdbBkt objstore.Bucket,
	parquetBkt objstore.Bucket,
	tsdbDiscoverer *locate.TSDBDiscoverer,
	parquetDiscoverer *locate.Discoverer,
	opts conversionOpts,
) error {
	blkDir := filepath.Join(opts.tempDir, ".blocks")
	bufferDir := filepath.Join(opts.tempDir, ".buffers")

	log.Info("Cleaning up previous state", "block_directory", blkDir, "buffer_directory", bufferDir)
	if err := cleanupDirectory(blkDir); err != nil {
		return fmt.Errorf("unable to clean up block directory: %w", err)
	}
	if err := cleanupDirectory(bufferDir); err != nil {
		return fmt.Errorf("unable to clean up buffer directory: %w", err)
	}

	parquetMetas := parquetDiscoverer.Metas()
	tsdbMetas := tsdbDiscoverer.Metas()

	plan := convert.NewPlanner(time.Now().Add(-opts.gracePeriod), opts.maxDays).Plan(tsdbMetas, parquetMetas)
	if len(plan.Steps) == 0 {
		log.Info("Nothing to do")
		return nil
	}
	log.Info("Planned dates to convert", slog.Int("days", len(plan.Steps)))
	for _, step := range plan.Steps {
		log.Info("Plan step", slog.String("date", step.Date.String()), slog.Any("ulids", ulidsFromMetas(step.Sources)))
	}

	log.Info("Starting plan conversions")
	var (
		stepBlocks, prevBlocks []convert.Convertible
		err                    error
	)
	// Process each step (day) one by one, keeping blocks shared between steps on disk.
	for _, step := range plan.Steps {
		// Close blocks that were open in the previous step but are no longer needed.
		prevBlocks = closeUnused(log, step.Sources, prevBlocks)

		ulids := ulidsFromMetas(step.Sources)
		log.Info("Converting date", slog.String("date", step.Date.String()), slog.Any("ulids", ulids))

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

		log.Info("Starting conversion", slog.String("date", step.Date.String()))
		convOpts := []convert.ConvertOption{
			convert.SortBy(opts.sortLabels...),
			convert.RowGroupSize(opts.rowGroupSize),
			convert.RowGroupCount(opts.rowGroupCount),
			convert.EncodingConcurrency(opts.encodingConcurrency),
			convert.WriteConcurrency(opts.writeConcurrency),
			convert.ChunkBufferPool(parquet.NewFileBufferPool(bufferDir, "chunkbuf-*")),
		}
		if err := convert.ConvertTSDBBlock(ctx, parquetBkt, step.Date, stepBlocks, convOpts...); err != nil {
			closeBlocks(log, stepBlocks...)
			return fmt.Errorf("unable to convert blocks for date %q: %s", step.Date, err)
		}
		log.Info("Conversion completed", slog.String("date", step.Date.String()))

		prevBlocks = stepBlocks
	}
	log.Info("Plan completed")

	// Close all open blocks.
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

func overlappingBlocks(blocks []convert.Convertible, date time.Time) []convert.Convertible {
	res := make([]convert.Convertible, 0)
	for _, m := range blocks {
		if date.AddDate(0, 0, 1).UnixMilli() >= m.Meta().MinTime && date.UnixMilli() <= m.Meta().MaxTime {
			res = append(res, m)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Meta().MaxTime <= res[j].Meta().MaxTime
	})
	return res
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
				return downloadBlock(ctx, bkt, m, blkDir, opts)
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

func downloadBlock(ctx context.Context, bkt objstore.BucketReader, meta metadata.Meta, blkDir string, opts conversionOpts) error {
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
			defer rc.Close()

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
