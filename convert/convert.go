// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/alecthomas/units"
	"github.com/efficientgo/core/errcapture"
	"github.com/oklog/ulid/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/util/zeropool"
	"github.com/thanos-io/objstore"
	"google.golang.org/protobuf/proto"

	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/proto/metapb"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type Convertible interface {
	Index() (tsdb.IndexReader, error)
	Chunks() (tsdb.ChunkReader, error)
	Tombstones() (tombstones.Reader, error)
	Meta() tsdb.BlockMeta
	Dir() string
	Close() error
}

// This is mostly used for testing when using tsdb.Head as Convertible.
type HeadBlock struct {
	*tsdb.Head
}

func (hb *HeadBlock) Dir() string {
	return ""
}

type convertOpts struct {
	numRowGroups        int
	rowGroupSize        int
	encodingConcurrency int

	sortLabels         []string
	sortingColumns     [][]string
	bloomfilterColumns [][]string
	labelBufferPool    parquet.BufferPool
	chunkbufferPool    parquet.BufferPool

	labelPageBufferSize int
	chunkPageBufferSize int
	writeConcurrency    int

	metricsTTL time.Duration
}

func (cfg convertOpts) buildBloomfilterColumns() []parquet.BloomFilterColumn {
	cols := make([]parquet.BloomFilterColumn, 0, len(cfg.bloomfilterColumns))

	for i := range cfg.bloomfilterColumns {
		cols = append(cols,
			parquet.SplitBlockFilter(10, cfg.bloomfilterColumns[i]...))
	}
	return cols
}

func (cfg convertOpts) buildSortingColumns() []parquet.SortingColumn {
	cols := make([]parquet.SortingColumn, 0, len(cfg.bloomfilterColumns))

	for i := range cfg.sortingColumns {
		cols = append(cols,
			parquet.Ascending(cfg.sortingColumns[i]...))
	}
	return cols
}

type ConvertOption func(*convertOpts)

func RowGroupSize(rbs int) ConvertOption {
	return func(opts *convertOpts) {
		opts.rowGroupSize = rbs
	}
}

func RowGroupCount(rc int) ConvertOption {
	return func(opts *convertOpts) {
		opts.numRowGroups = rc
	}
}

func SortBy(labels ...string) ConvertOption {
	return func(opts *convertOpts) {
		sortingColumns := make([][]string, len(labels))
		for i := range labels {
			sortingColumns[i] = []string{schema.LabelNameToColumn(labels[i])}
		}
		opts.sortingColumns = sortingColumns
		opts.sortLabels = labels
	}
}

func LabelPageBufferSize(pb units.Base2Bytes) ConvertOption {
	return func(opts *convertOpts) {
		opts.labelPageBufferSize = int(pb)
	}
}

func ChunkPageBufferSize(pb units.Base2Bytes) ConvertOption {
	return func(opts *convertOpts) {
		opts.chunkPageBufferSize = int(pb)
	}
}

func LabelBufferPool(p parquet.BufferPool) ConvertOption {
	return func(opts *convertOpts) {
		opts.labelBufferPool = p
	}
}

func ChunkBufferPool(p parquet.BufferPool) ConvertOption {
	return func(opts *convertOpts) {
		opts.chunkbufferPool = p
	}
}

func EncodingConcurrency(c int) ConvertOption {
	return func(opts *convertOpts) {
		opts.encodingConcurrency = c
	}
}

func WriteConcurrency(c int) ConvertOption {
	return func(opts *convertOpts) {
		opts.writeConcurrency = c
	}
}

func ConvertTSDBBlock(
	ctx context.Context,
	bkt objstore.Bucket,
	day util.Date,
	blks []Convertible,
	opts ...ConvertOption,
) (rerr error) {
	cfg := &convertOpts{
		rowGroupSize:        1_000_000,
		numRowGroups:        6,
		sortingColumns:      [][]string{{schema.LabelNameToColumn(labels.MetricName)}},
		bloomfilterColumns:  [][]string{{schema.LabelNameToColumn(labels.MetricName)}},
		labelBufferPool:     parquet.NewBufferPool(),
		chunkbufferPool:     parquet.NewBufferPool(),
		encodingConcurrency: 1,
		labelPageBufferSize: int(256 * units.KiB),
		chunkPageBufferSize: int(2 * units.MiB),
		writeConcurrency:    1,
	}
	for i := range opts {
		opts[i](cfg)
	}
	start, end := day.MinT(), day.MaxT()
	name := schema.BlockNameForDay(day)

	shardedRowReaders, err := shardedIndexRowReader(ctx, start, end, blks, *cfg)
	if err != nil {
		return fmt.Errorf("failed to create sharded TSDB row readers: %w", err)
	}
	defer func() {
		for _, rr := range shardedRowReaders {
			defer errcapture.Do(&rerr, rr.Close, "index row reader close")
		}
	}()
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(cfg.writeConcurrency)
	for shard, rr := range shardedRowReaders {
		errGroup.Go(func() error {
			converter := newConverter(
				name,
				start,
				end,
				shard,
				rr,
				bkt,
				cfg.rowGroupSize,
				cfg.numRowGroups,
				cfg.buildSortingColumns(),
				cfg.buildBloomfilterColumns(),
				cfg.labelBufferPool,
				cfg.chunkbufferPool,
				cfg.labelPageBufferSize,
				cfg.chunkPageBufferSize,
			)

			if err := converter.convert(ctx); err != nil {
				return fmt.Errorf("unable to convert block: %w", err)
			}
			return nil
		})
	}

	err = errGroup.Wait()
	if err != nil {
		return fmt.Errorf("failed to convert shards in parallel: %w", err)
	}

	if err := writeMetaFile(ctx, start, end, name, int64(len(shardedRowReaders)), bkt); err != nil {
		return fmt.Errorf("failed to write meta file: %w", err)
	}

	lastSuccessfulConvertTime.SetToCurrentTime()

	return nil
}

type blockIndexReader struct {
	blockID  ulid.ULID
	idx      int // index of the block in the input slice
	reader   tsdb.IndexReader
	postings index.Postings
}

type blockSeries struct {
	blockIdx  int // index of the block in the input slice
	seriesIdx int // index of the series in the block postings
	ref       storage.SeriesRef
	labels    labels.Labels
}

func writeMetaFile(ctx context.Context, start int64, end int64, name string, numShards int64, bkt objstore.Bucket) error {
	meta := &metapb.Metadata{
		Version: schema.V2,
		Mint:    start,
		Maxt:    end,
		Shards:  numShards,
	}

	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("unable to marshal meta bytes: %w", err)
	}
	if err := bkt.Upload(ctx, schema.MetaFileNameForBlock(name), bytes.NewReader(metaBytes)); err != nil {
		return fmt.Errorf("unable to upload meta file: %w", err)
	}

	return nil
}

func shardedIndexRowReader(
	ctx context.Context,
	mint, maxt int64,
	blocks []Convertible,
	opts convertOpts,
) (reader []*indexRowReader, rerr error) {
	// Blocks can have multiple entries with the same of ULID in the case of head blocks;
	// track all blocks by their index in the input slice rather than assuming unique ULIDs.
	indexReaders := make([]blockIndexReader, len(blocks))
	// Simpler to track and close these readers separate from those used by shard conversion reader/writers.

	defer func() {
		for _, indexReader := range indexReaders {
			errcapture.Do(&rerr, indexReader.reader.Close, "index row reader close")
		}
	}()
	for i, blk := range blocks {
		indexReader, err := blk.Index()
		if err != nil {
			return nil, fmt.Errorf("failed to get index reader from block: %w", err)
		}
		indexReaders[i] = blockIndexReader{
			blockID:  blk.Meta().ULID,
			idx:      i,
			reader:   indexReader,
			postings: tsdb.AllSortedPostings(ctx, indexReader),
		}
	}

	uniqueSeriesCount, shardedSeries, err := shardSeries(indexReaders, mint, maxt, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to determine unique series count: %w", err)
	}
	if uniqueSeriesCount == 0 {
		return nil, fmt.Errorf("no series found in the specified time range: %w", err)
	}

	shardIndexRowReader := make([]*indexRowReader, len(shardedSeries))

	// We close everything if any errors or panic occur
	allClosers := make([]io.Closer, 0, len(blocks)*3)
	defer func() {
		if rerr != nil {
			for _, closer := range allClosers {
				errcapture.Do(&rerr, closer.Close, "closer close")
			}
		}
	}()

	// For each shard, create a TSDBRowReader with:
	//	* a MergeChunkSeriesSet of all blocks' series sets for the shard
	//	* a schema built from only the label names present in the shard
	for shardIdx, shardSeries := range shardedSeries {
		// An index, chunk, and tombstone reader per block each must be closed after usage
		// in order for the prometheus block reader to not hang indefinitely when closed.
		closers := make([]io.Closer, 0, len(shardSeries)*3)
		seriesSets := make([]storage.ChunkSeriesSet, 0, len(blocks))
		labelNames := make(map[string]struct{})

		// For each block with series in the shard,
		// init readers and postings list required to create a tsdb.blockChunkSeriesSet;
		// series sets from all blocks for the shard will be merged by mergeChunkSeriesSet.
		for _, blockSeries := range shardSeries {
			blk := blocks[blockSeries[0].blockIdx]
			// Init all readers for block & add to closers

			// Init separate index readers from above indexReaders to simplify closing logic
			indexr, err := blk.Index()
			if err != nil {
				return nil, fmt.Errorf("failed to get index reader from block: %w", err)
			}
			closers = append(closers, indexr)
			allClosers = append(allClosers, indexr)

			chunkr, err := blk.Chunks()
			if err != nil {
				return nil, fmt.Errorf("failed to get chunk reader from block: %w", err)
			}
			closers = append(closers, chunkr)
			allClosers = append(allClosers, chunkr)

			tombsr, err := blk.Tombstones()
			if err != nil {
				return nil, fmt.Errorf("failed to get tombstone reader from block: %w", err)
			}
			closers = append(closers, tombsr)
			allClosers = append(allClosers, tombsr)

			// Flatten series refs and add all label columns to schema for the shard
			refs := make([]storage.SeriesRef, 0, len(blockSeries))
			for _, series := range blockSeries {
				refs = append(refs, series.ref)
				series.labels.Range(func(l labels.Label) {
					labelNames[l.Name] = struct{}{}
				})
			}
			postings := index.NewListPostings(refs)
			seriesSet := tsdb.NewBlockChunkSeriesSet(blk.Meta().ULID, indexr, chunkr, tombsr, postings, mint, maxt, false)
			seriesSets = append(seriesSets, seriesSet)
		}

		mergeSeriesSet := newMergeChunkSeriesSet(
			seriesSets, compareBySortedLabelsFunc(opts.sortLabels), storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge),
		)

		s := schema.BuildSchemaFromLabels(slices.Sorted(maps.Keys(labelNames)))
		shardIndexRowReader[shardIdx] = &indexRowReader{
			ctx:       ctx,
			seriesSet: mergeSeriesSet,
			closers:   closers,

			schema:     s,
			rowBuilder: parquet.NewRowBuilder(s),

			concurrency: opts.encodingConcurrency,

			chunksColumn0:    columnIDForKnownColumn(s, schema.ChunksColumn0),
			chunksColumn1:    columnIDForKnownColumn(s, schema.ChunksColumn1),
			chunksColumn2:    columnIDForKnownColumn(s, schema.ChunksColumn2),
			labelIndexColumn: columnIDForKnownColumn(s, schema.LabelIndexColumn),
			labelHashColumn:  columnIDForKnownColumn(s, schema.LabelHashColumn),
		}
	}
	return shardIndexRowReader, nil
}

func shardSeries(
	blockIndexReaders []blockIndexReader,
	mint, maxt int64,
	opts convertOpts,
) (int, []map[int][]blockSeries, error) {
	chks := make([]chunks.Meta, 0, 128)
	allSeries := make([]blockSeries, 0, 128*len(blockIndexReaders))
	// Collect all series from all blocks with chunks in the time range
	for _, blockIndexReader := range blockIndexReaders {
		i := 0
		scratchBuilder := labels.NewScratchBuilder(10)

		for blockIndexReader.postings.Next() {
			scratchBuilder.Reset()
			chks = chks[:0]

			if err := blockIndexReader.reader.Series(blockIndexReader.postings.At(), &scratchBuilder, &chks); err != nil {
				return 0, nil, fmt.Errorf("unable to expand series: %w", err)
			}

			hasChunks := slices.ContainsFunc(chks, func(chk chunks.Meta) bool {
				return mint <= chk.MaxTime && chk.MinTime <= maxt
			})
			if !hasChunks {
				continue
			}

			scratchBuilderLabels := scratchBuilder.Labels()
			allSeries = append(allSeries, blockSeries{
				blockIdx:  blockIndexReader.idx,
				seriesIdx: i,
				ref:       blockIndexReader.postings.At(),
				labels:    scratchBuilderLabels,
			})
		}
	}

	if len(allSeries) == 0 {
		return 0, nil, nil
	}

	slices.SortFunc(allSeries, compareBlockSeriesBySortedLabelsFunc(opts.sortLabels))

	// Count how many unique series will exist after merging across blocks.
	uniqueSeriesCount := 1
	for i := 1; i < len(allSeries); i++ {
		if labels.Compare(allSeries[i].labels, allSeries[i-1].labels) != 0 {
			uniqueSeriesCount++
		}
	}

	// Divide rows evenly across shards to avoid one small shard at the end;
	// Use (a + b - 1) / b equivalence to math.Ceil(a / b)
	// so integer division does not cut off the remainder series and to avoid floating point issues.
	targetTotalShards := (uniqueSeriesCount + (opts.numRowGroups * opts.rowGroupSize) - 1) / (opts.numRowGroups * opts.rowGroupSize)
	rowsPerShard := (uniqueSeriesCount + targetTotalShards - 1) / targetTotalShards

	// For each shard index i, shardSeries[i] is a map of blockIdx -> []series.
	shardSeries := make([]map[int][]blockSeries, 1, targetTotalShards)
	shardSeries[0] = make(map[int][]blockSeries)

	shardIdx, uniqueCount := 0, 0
	matchLabels := labels.Labels{}
	labelColumns := make(map[string]struct{})
	for _, serie := range allSeries {
		if labels.Compare(serie.labels, matchLabels) != 0 {
			// New unique series
			serie.labels.Range(func(label labels.Label) {
				labelColumns[label.Name] = struct{}{}
			})
			if uniqueCount >= rowsPerShard || len(labelColumns)+schema.ChunkColumnsPerDay+1 >= math.MaxInt16 {
				// Create a new shard if it would exceed the unique series count for the shard
				// or if number of label columns exceed max allowed parquet schema.
				// We will start the next shard with this series.
				shardIdx++
				shardSeries = append(shardSeries, make(map[int][]blockSeries))
				labelColumns = make(map[string]struct{})
				uniqueCount = 0
			}

			// Unique series limit is not hit yet for the shard; add the series.
			shardSeries[shardIdx][serie.blockIdx] = append(shardSeries[shardIdx][serie.blockIdx], serie)
			// Increment unique count, update labels to compare against, and move on to next series
			uniqueCount++
			matchLabels = serie.labels
		} else {
			// Same labelset as previous series, add it to the shard but do not increment unique count
			shardSeries[shardIdx][serie.blockIdx] = append(shardSeries[shardIdx][serie.blockIdx], serie)
			// Move on to next series
		}
	}

	return uniqueSeriesCount, shardSeries, nil
}

func compareBlockSeriesBySortedLabelsFunc(sortedLabels []string) func(a, b blockSeries) int {
	return func(a, b blockSeries) int {
		for _, lb := range sortedLabels {
			if c := strings.Compare(a.labels.Get(lb), b.labels.Get(lb)); c != 0 {
				return c
			}
		}

		return labels.Compare(a.labels, b.labels)
	}
}

func compareBySortedLabelsFunc(sortedLabels []string) func(a, b labels.Labels) int {
	return func(a, b labels.Labels) int {
		for _, lb := range sortedLabels {
			if c := strings.Compare(a.Get(lb), b.Get(lb)); c != 0 {
				return c
			}
		}

		return labels.Compare(a, b)
	}
}

type converter struct {
	name       string
	mint, maxt int64

	shard          int
	seriesPerShard int
	rowGroupSize   int
	numRowGroups   int

	bkt objstore.Bucket

	rr *indexRowReader

	sortingColumns     []parquet.SortingColumn
	bloomfilterColumns []parquet.BloomFilterColumn
	labelBufferPool    parquet.BufferPool
	chunkBufferPool    parquet.BufferPool

	labelPageBufferSize int
	chunkPageBufferSize int
}

func newConverter(
	name string,
	mint int64,
	maxt int64,
	shard int,
	rr *indexRowReader,
	bkt objstore.Bucket,
	rowGroupSize int,
	numRowGroups int,
	sortingColumns []parquet.SortingColumn,
	bloomfilterColumns []parquet.BloomFilterColumn,
	labelBufferPool parquet.BufferPool,
	chunkBufferPool parquet.BufferPool,
	labelPageBufferSize int,
	chunkPageBufferSize int,

) *converter {
	return &converter{
		name:  name,
		mint:  mint,
		maxt:  maxt,
		shard: shard,

		bkt: bkt,
		rr:  rr,

		rowGroupSize:       rowGroupSize,
		numRowGroups:       numRowGroups,
		sortingColumns:     sortingColumns,
		bloomfilterColumns: bloomfilterColumns,
		labelBufferPool:    labelBufferPool,
		chunkBufferPool:    chunkBufferPool,

		labelPageBufferSize: labelPageBufferSize,
		chunkPageBufferSize: chunkPageBufferSize,
	}
}

func (c *converter) labelWriterOptions() []parquet.WriterOption {
	return []parquet.WriterOption{
		parquet.MaxRowsPerRowGroup(int64(c.rowGroupSize)),
		parquet.SortingWriterConfig(parquet.SortingColumns(c.sortingColumns...)),
		parquet.BloomFilters(c.bloomfilterColumns...),
		parquet.SkipPageBounds(schema.LabelIndexColumn),
		parquet.ColumnPageBuffers(c.labelBufferPool),
		parquet.PageBufferSize(c.labelPageBufferSize),
	}
}

func (c *converter) chunkWriterOptions() []parquet.WriterOption {
	return []parquet.WriterOption{
		parquet.MaxRowsPerRowGroup(int64(c.rowGroupSize)),
		parquet.SkipPageBounds(schema.LabelHashColumn),
		parquet.SkipPageBounds(schema.ChunksColumn0),
		parquet.SkipPageBounds(schema.ChunksColumn1),
		parquet.SkipPageBounds(schema.ChunksColumn2),
		parquet.ColumnPageBuffers(c.chunkBufferPool),
		parquet.PageBufferSize(c.chunkPageBufferSize),
	}
}

func (c *converter) convert(ctx context.Context) error {
	if _, err := c.convertShard(ctx); err != nil {
		return fmt.Errorf("unable to convert shards: %w", err)
	}
	return nil
}

func (c *converter) convertShard(ctx context.Context) (_ bool, rerr error) {
	s := c.rr.Schema()

	w, err := newSplitFileWriter(ctx, c.bkt, s, map[string]writerConfig{
		schema.LabelsPfileNameForShard(c.name, c.shard): {
			s:    schema.WithCompression(schema.LabelsProjection(s)),
			opts: c.labelWriterOptions(),
		},
		schema.ChunksPfileNameForShard(c.name, c.shard): {
			s:    schema.WithCompression(schema.ChunkProjection(s)),
			opts: c.chunkWriterOptions(),
		},
	},
	)
	if err != nil {
		return false, fmt.Errorf("unable to build multifile writer: %w", err)
	}
	defer errcapture.Do(&rerr, w.Close, "multifile writer close")

	_, err = parquet.CopyRows(w, newBufferedReader(ctx, c.rr))
	if err != nil {
		return false, fmt.Errorf("unable to copy rows to writer: %w", err)
	}

	return true, nil
}

type limitReader struct {
	parquet.RowReader
	limit int
	cur   int
}

func (lr *limitReader) ReadRows(buf []parquet.Row) (int, error) {
	n, err := lr.RowReader.ReadRows(buf)
	if err != nil {
		return n, err
	}
	lr.cur += n

	if lr.cur > lr.limit {
		return n, io.EOF
	}
	return n, nil
}

type fileWriter struct {
	pw   *parquet.GenericWriter[any]
	conv parquet.Conversion
	w    io.WriteCloser
	bw   *bufio.Writer
}

type splitPipeFileWriter struct {
	fileWriters map[string]*fileWriter
	g           *errgroup.Group
}

type writerConfig struct {
	s    *parquet.Schema
	opts []parquet.WriterOption
}

func newSplitFileWriter(ctx context.Context, bkt objstore.Bucket, inSchema *parquet.Schema, files map[string]writerConfig) (*splitPipeFileWriter, error) {
	fileWriters := make(map[string]*fileWriter)
	g, ctx := errgroup.WithContext(ctx)
	for file, cfg := range files {
		conv, err := parquet.Convert(cfg.s, inSchema)
		if err != nil {
			return nil, fmt.Errorf("unable to convert schemas")
		}

		r, w := io.Pipe()
		bw := bufio.NewWriterSize(w, 32_000_000)
		br := bufio.NewReaderSize(r, 32_000_000)
		fileWriters[file] = &fileWriter{
			pw:   parquet.NewGenericWriter[any](bw, append(cfg.opts, cfg.s)...),
			w:    w,
			bw:   bw,
			conv: conv,
		}
		g.Go(func() (rerr error) {
			defer errcapture.Do(&rerr, r.Close, "pipe reader close")
			return bkt.Upload(ctx, file, br)
		})
	}
	return &splitPipeFileWriter{
		fileWriters: fileWriters,
		g:           g,
	}, nil

}

func (s *splitPipeFileWriter) WriteRows(rows []parquet.Row) (int, error) {
	var g errgroup.Group
	for _, writer := range s.fileWriters {
		g.Go(func() error {
			rr := make([]parquet.Row, len(rows))
			for i, row := range rows {
				rr[i] = row.Clone()
			}
			_, err := writer.conv.Convert(rr)
			if err != nil {
				return fmt.Errorf("unable to convert rows: %w", err)
			}
			n, err := writer.pw.WriteRows(rr)
			if err != nil {
				return fmt.Errorf("unable to write rows: %w", err)
			}
			if n != len(rows) {
				return fmt.Errorf("unable to write rows: %d != %d", n, len(rows))
			}
			return nil
		})
	}
	return len(rows), g.Wait()
}

func (s *splitPipeFileWriter) Close() error {
	errs := make([]error, 0)
	for _, fw := range s.fileWriters {
		if err := fw.pw.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close pipewriter: %w", err))
		}
		if err := fw.bw.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("unable to flush buffered writer: %w", err))
		}
		if err := fw.w.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close writer: %w", err))
		}
	}
	if err := s.g.Wait(); err != nil {
		errs = append(errs, fmt.Errorf("unable to wait for group: %w", err))
	}
	return errors.Join(errs...)
}

type bufferedReader struct {
	rr parquet.RowReader

	ctx     context.Context
	c       chan []parquet.Row
	errCh   chan error
	rowPool zeropool.Pool[[]parquet.Row]

	current      []parquet.Row
	currentIndex int
}

func newBufferedReader(ctx context.Context, rr parquet.RowReader) *bufferedReader {
	br := &bufferedReader{
		rr:    rr,
		ctx:   ctx,
		c:     make(chan []parquet.Row, 128),
		errCh: make(chan error, 1),
		rowPool: zeropool.New(func() []parquet.Row {
			return make([]parquet.Row, 128)
		}),
	}

	go br.readRows()

	return br
}

func (b *bufferedReader) ReadRows(rows []parquet.Row) (int, error) {
	if b.current == nil {
		select {
		case next, ok := <-b.c:
			if !ok {
				return 0, io.EOF
			}
			b.current = next
			b.currentIndex = 0
		case err := <-b.errCh:
			return 0, err
		}
	}

	current := b.current[b.currentIndex:]
	i := min(len(current), len(rows))
	copy(rows[:i], current[:i])
	b.currentIndex += i
	if b.currentIndex >= len(b.current) {
		b.rowPool.Put(b.current[0:cap(b.current)])
		b.current = nil
	}
	return i, nil
}

func (b *bufferedReader) Close() {
	close(b.c)
	close(b.errCh)
}

func (b *bufferedReader) readRows() {
	for {
		select {
		case <-b.ctx.Done():
			b.errCh <- b.ctx.Err()
			return
		default:
			rows := b.rowPool.Get()
			n, err := b.rr.ReadRows(rows)
			if n > 0 {
				b.c <- rows[:n]
			}
			if err != nil {
				if err == io.EOF {
					close(b.c)
					return
				}
				b.errCh <- err
				return
			}
		}
	}
}
