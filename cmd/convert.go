// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/cloudflare/parquet-tsdb-poc/convert"
	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
)

type convertOpts struct {
	parquetBucket bucketOpts
	tsdbBucket    bucketOpts
	conversion    conversionOpts
}

func (opts *convertOpts) registerFlags(cmd *kingpin.CmdClause) {
	opts.conversion.registerFlags(cmd)
	opts.parquetBucket.registerParquetFlags(cmd)
	opts.tsdbBucket.registerTSDBFlags(cmd)
}

func registerConvertApp(app *kingpin.Application) (*kingpin.CmdClause, func(ctx context.Context, log *slog.Logger) error) {
	cmd := app.Command("convert", "convert TSDB Block to parquet file")

	var opts convertOpts
	opts.registerFlags(cmd)

	return cmd, func(ctx context.Context, log *slog.Logger) error {
		blkDir := filepath.Join(opts.conversion.tempDir, "blocks")

		start, end, err := getStartEnd(opts.conversion)
		if err != nil {
			return fmt.Errorf("unable to get start, end: %s", err)
		}

		tsdbBkt, err := setupBucket(log, opts.tsdbBucket)
		if err != nil {
			return fmt.Errorf("unable to setup tsdb bucket: %s", err)
		}
		parquetBkt, err := setupBucket(log, opts.parquetBucket)
		if err != nil {
			return fmt.Errorf("unable to setup parquet bucket: %s", err)
		}

		// TODO: this is kinda horrible logic here that is not reentrant or robust against errors
		// But for sake of getting started we can use it to convert the first blocks and then iterate

		log.Info("Fetching metas", "start", start, "end", end)
		metas, err := fetchTSDBMetas(ctx, tsdbBkt, start, end)
		if err != nil {
			return fmt.Errorf("unable to fetch tsdb metas: %s", err)
		}

		log.Info("Downloading blocks", "metas", metas)
		blocks, err := downloadBlocks(ctx, tsdbBkt, metas, blkDir)
		if err != nil {
			return fmt.Errorf("unable to download blocks: %s", err)
		}

		for next := start; next != end; next = next.AddDate(0, 0, 1) {
			log.Info("Converting next parquet file", "day", next)

			candidates := overlappingBlocks(blocks, next)
			if len(candidates) == 0 {
				continue
			}
			convOpts := []convert.ConvertOption{
				convert.SortBufSize(opts.conversion.sortBufSize),
				convert.BufferPool(parquet.NewFileBufferPool(opts.conversion.tempDir, "convert-*")),
			}
			if err := convert.ConvertTSDBBlock(ctx, parquetBkt, next, candidates, convOpts...); err != nil {
				return fmt.Errorf("unable to convert blocks for date %q: %s", next, err)
			}
		}
		return nil
	}
}

type conversionOpts struct {
	sortBufSize int
	tempDir     string

	start, end string
}

func (opts *conversionOpts) registerFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("convert.start", "timestamp of the first parquet block to convert (rounded to start of day)").StringVar(&opts.start)
	cmd.Flag("convert.end", "timestamp of the last parquet block to convert(rounded to start of day)").StringVar(&opts.end)
	cmd.Flag("convert.tempdir", "directory for temporary state").StringVar(&opts.tempDir)
	cmd.Flag("convert.sortbuf", "size of sorting buffer").Default("64_000").IntVar(&opts.sortBufSize)
}

func getStartEnd(opts conversionOpts) (time.Time, time.Time, error) {
	from, err := time.Parse(time.RFC3339, opts.start)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("unable to parse start: %w", err)
	}
	to, err := time.Parse(time.RFC3339, opts.end)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("unable to parse end: %w", err)
	}
	return util.BeginOfDay(from), util.BeginOfDay(to), nil
}

func fetchTSDBMetas(ctx context.Context, bkt objstore.BucketReader, from, to time.Time) ([]tsdb.BlockMeta, error) {
	metas := make([]tsdb.BlockMeta, 0)
	err := bkt.Iter(ctx, "", func(name string) error {
		split := strings.Split(name, "/")
		f := split[1]
		if f != "meta.json" {
			return nil
		}
		content, err := bkt.Get(ctx, name)
		if err != nil {
			return err
		}
		defer content.Close()

		var m tsdb.BlockMeta
		if err := json.NewDecoder(content).Decode(&m); err != nil {
			return err
		}

		metas = append(metas, m)

		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return nil, fmt.Errorf("unable to fetch metas")
	}

	startMillis := util.BeginOfDay(from).UnixMilli()
	endMillis := util.EndOfDay(to).UnixMilli()

	res := make([]tsdb.BlockMeta, 0)
	for _, m := range metas {
		if endMillis >= m.MinTime && startMillis <= m.MaxTime {
			res = append(res, m)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].MinTime < res[j].MinTime
	})

	return res, nil
}

func overlappingBlocks(blocks []*tsdb.Block, date time.Time) []convert.Convertable {
	dateStartMillis := date.UnixMilli()
	dateEndMillis := date.AddDate(0, 0, 1).UnixMilli()

	res := make([]convert.Convertable, 0)
	for _, blk := range blocks {
		m := blk.Meta()
		if dateEndMillis >= m.MinTime && dateStartMillis <= m.MaxTime {
			res = append(res, blk)
		}
	}
	return res
}

func downloadBlocks(ctx context.Context, bkt objstore.BucketReader, metas []tsdb.BlockMeta, blkDir string) ([]*tsdb.Block, error) {
	logAdapter := slogAdapter{log: slog.New(slog.NewJSONHandler(io.Discard, nil))}

	opts := []objstore.DownloadOption{objstore.WithFetchConcurrency(runtime.GOMAXPROCS(0))}
	res := make([]*tsdb.Block, 0)
	for _, m := range metas {
		src := m.ULID.String()
		dst := filepath.Join(blkDir, src)

		if err := objstore.DownloadDir(ctx, logAdapter, bkt, src, src, dst, opts...); err != nil {
			return nil, fmt.Errorf("unable to download %q: %s", src, err)
		}
		blk, err := tsdb.OpenBlock(logAdapter, dst, chunkenc.NewPool())
		if err != nil {
			return nil, fmt.Errorf("unable to open block %q: %s", src, err)
		}
		res = append(res, blk)
	}
	return res, nil
}
