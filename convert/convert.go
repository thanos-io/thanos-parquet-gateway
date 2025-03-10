// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/thanos-io/objstore"
	"google.golang.org/protobuf/proto"

	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
	"github.com/cloudflare/parquet-tsdb-poc/proto/metapb"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type Convertible interface {
	Index() (tsdb.IndexReader, error)
	Chunks() (tsdb.ChunkReader, error)
	Tombstones() (tombstones.Reader, error)
	Meta() tsdb.BlockMeta
}

type convertOpts struct {
	numRowGroups int
	rowGroupSize int
	sortBufSize  int

	bufferPool parquet.BufferPool

	sortingColumns     [][]string
	bloomfilterColumns [][]string
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
	cols := make([]parquet.SortingColumn, 0, len(cfg.sortingColumns))

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

func SortBufSize(sbs int) ConvertOption {
	return func(opts *convertOpts) {
		opts.sortBufSize = sbs
	}
}

func SortBy(labels []string) ConvertOption {
	return func(opts *convertOpts) {
		sortingColumns := make([][]string, len(labels))
		for i := range labels {
			sortingColumns[i] = []string{schema.LabelNameToColumn(labels[i])}
		}
		opts.sortingColumns = sortingColumns
	}
}

func BufferPool(p parquet.BufferPool) ConvertOption {
	return func(opts *convertOpts) {
		opts.bufferPool = p
	}
}

func ConvertTSDBBlock(
	ctx context.Context,
	bkt objstore.Bucket,
	day time.Time,
	blks []Convertible,
	opts ...ConvertOption,
) error {
	cfg := &convertOpts{
		rowGroupSize:       1_000_000,
		numRowGroups:       6,
		sortBufSize:        128_000,
		bufferPool:         parquet.NewBufferPool(),
		sortingColumns:     [][]string{{schema.LabelNameToColumn(labels.MetricName)}},
		bloomfilterColumns: [][]string{{schema.LabelNameToColumn(labels.MetricName)}},
	}
	for i := range opts {
		opts[i](cfg)
	}
	start, end := util.BeginOfDay(day), util.EndOfDay(day)
	name, err := schema.BlockNameForDay(start)
	if err != nil {
		return fmt.Errorf("unable to get block name: %s", err)
	}
	rr, err := newIndexRowReader(ctx, start.UnixMilli(), end.UnixMilli(), blks)
	if err != nil {
		return fmt.Errorf("unable to create index rowreader: %s", err)
	}
	defer rr.Close()

	converter := newConverter(
		name,
		start.UnixMilli(),
		end.UnixMilli(),
		rr,
		bkt,
		cfg.bufferPool,
		cfg.sortBufSize,
		cfg.rowGroupSize,
		cfg.numRowGroups,
		cfg.buildSortingColumns(),
		cfg.buildBloomfilterColumns(),
	)

	if err := converter.convert(ctx); err != nil {
		return fmt.Errorf("unable to convert block: %s", err)
	}
	return nil
}

type converter struct {
	name       string
	mint, maxt int64

	currentShard   int
	seriesPerShard int
	sortBufSize    int
	rowGroupSize   int
	numRowGroups   int

	bkt objstore.Bucket

	rr *indexRowReader
	p  parquet.BufferPool

	sortingColumns     []parquet.SortingColumn
	bloomfilterColumns []parquet.BloomFilterColumn
}

func newConverter(
	name string,
	mint int64,
	maxt int64,
	rr *indexRowReader,
	bkt objstore.Bucket,
	p parquet.BufferPool,
	sortBufSize int,
	rowGroupSize int,
	numRowGroups int,
	sortingColumns []parquet.SortingColumn,
	bloomfilterColumns []parquet.BloomFilterColumn,

) *converter {
	return &converter{
		name: name,
		mint: mint,
		maxt: maxt,

		bkt: bkt,
		rr:  rr,
		p:   p,

		rowGroupSize:       rowGroupSize,
		numRowGroups:       numRowGroups,
		sortBufSize:        sortBufSize,
		sortingColumns:     sortingColumns,
		bloomfilterColumns: bloomfilterColumns,
	}
}
func (c *converter) convert(ctx context.Context) error {
	if err := c.convertShards(ctx); err != nil {
		return fmt.Errorf("unable to convert shards: %w", err)
	}
	if err := c.writeMetaFile(ctx); err != nil {
		return fmt.Errorf("unable to write meta file: %w", err)
	}
	return nil
}

func (c *converter) writeMetaFile(ctx context.Context) error {
	meta := &metapb.Metadata{
		ColumnsForName: make(map[string]*metapb.Columns),
		Mint:           c.mint,
		Maxt:           c.maxt,
		Shards:         int64(c.currentShard) + 1,
	}
	for k, v := range c.rr.NameLabelMapping() {
		cols := &metapb.Columns{Columns: make([]string, 0, len(v))}
		for lbl := range v {
			cols.Columns = append(cols.Columns, lbl)
		}
		meta.ColumnsForName[k] = cols
	}

	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("unable to marshal meta bytes: %s", err)
	}
	if err := c.bkt.Upload(ctx, schema.MetaFileNameForBlock(c.name), bytes.NewReader(metaBytes)); err != nil {
		return fmt.Errorf("unable to upload meta file: %s", err)
	}

	return nil
}

func (c *converter) convertShards(ctx context.Context) error {
	for {
		if ok, err := c.convertShard(ctx); err != nil {
			return fmt.Errorf("unable to convert shard: %s", err)
		} else if !ok {
			break
		}
	}
	return nil
}

func (c *converter) convertShard(ctx context.Context) (bool, error) {
	s := c.rr.Schema()
	rowsToWrite := c.numRowGroups * c.rowGroupSize

	in := c.p.GetBuffer()
	defer c.p.PutBuffer(in)

	sw := newSortingWriter(in, c.p, s, c.sortBufSize, c.sortingColumns...)
	n, err := parquet.CopyRows(sw, newLimitReader(c.rr, rowsToWrite))
	if err != nil {
		return false, fmt.Errorf("unable to copy rows to sorting writer: %s", err)
	}
	if err := sw.Flush(); err != nil {
		return false, fmt.Errorf("unable to flush sorting writer: %s", err)
	}

	if err := c.writeShardLabelsPfile(ctx, sw, c.currentShard); err != nil {
		return false, fmt.Errorf("unable to write label parquetfile %d: %s", c.currentShard, err)
	}
	if err := c.writeShardChunksPfile(ctx, sw, c.currentShard); err != nil {
		return false, fmt.Errorf("unable to write chunks parquetfile %d: %s", c.currentShard, err)
	}
	if n < int64(rowsToWrite) {
		return false, nil
	}
	c.currentShard++
	return true, nil
}

func (c *converter) writeShardLabelsPfile(
	ctx context.Context,
	sw *sortingWriter,
	shard int,
) error {
	out := c.p.GetBuffer()
	defer c.p.PutBuffer(out)

	inSchema := c.rr.Schema()
	outSchema := schema.LabelsProjection(schema.WithCompression(inSchema))

	conv, err := parquet.Convert(outSchema, inSchema)
	if err != nil {
		return fmt.Errorf("unable to convert schemas")
	}

	sr, err := sw.RowReader()
	if err != nil {
		return fmt.Errorf("unable to get sorted row reader: %s", err)
	}

	cr := parquet.ConvertRowReader(sr, conv)

	writer := parquet.NewGenericWriter[any](out, outSchema, parquet.BloomFilters(c.bloomfilterColumns...))
	if _, err := parquet.CopyRows(newFlushingWriter(writer, c.rowGroupSize), cr); err != nil {
		return fmt.Errorf("unable to copy rows: %s", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("unable to close writer: %s", err)
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("unable to rewind temporary buffer: %s", err)
	}
	if err := c.bkt.Upload(ctx, schema.LabelsPfileNameForShard(c.name, shard), out); err != nil {
		return fmt.Errorf("unable to upload parquet file: %s", err)
	}

	return nil
}

func (c *converter) writeShardChunksPfile(
	ctx context.Context,
	sw *sortingWriter,
	shard int,
) error {
	out := c.p.GetBuffer()
	defer c.p.PutBuffer(out)

	inSchema := c.rr.Schema()
	outSchema := schema.ChunkProjection(schema.WithCompression(inSchema))

	conv, err := parquet.Convert(outSchema, inSchema)
	if err != nil {
		return fmt.Errorf("unable to convert schemas")
	}

	sr, err := sw.RowReader()
	if err != nil {
		return fmt.Errorf("unable to get sorted row reader: %s", err)
	}

	cr := parquet.ConvertRowReader(sr, conv)

	writer := parquet.NewGenericWriter[any](out, outSchema, parquet.BloomFilters(c.bloomfilterColumns...))
	if _, err := parquet.CopyRows(newFlushingWriter(writer, c.rowGroupSize), cr); err != nil {
		return fmt.Errorf("unable to copy rows: %s", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("unable to close writer: %s", err)
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("unable to rewind temporary buffer: %s", err)
	}
	if err := c.bkt.Upload(ctx, schema.ChunksPfileNameForShard(c.name, shard), out); err != nil {
		return fmt.Errorf("unable to upload parquet file: %s", err)
	}

	return nil
}

type rowWriterFlusher interface {
	parquet.RowWriter
	Flush() error
}

type flushingWriter struct {
	rowWriterFlusher
	flush int
	cur   int
}

func newFlushingWriter(w rowWriterFlusher, flush int) parquet.RowWriter {
	return &flushingWriter{rowWriterFlusher: w, flush: flush}
}

func (fw *flushingWriter) WriteRows(buf []parquet.Row) (int, error) {
	n, err := fw.rowWriterFlusher.WriteRows(buf)
	if err != nil {
		return n, err
	}
	fw.cur += n

	if fw.cur > fw.flush {
		if err := fw.rowWriterFlusher.Flush(); err != nil {
			return n, err
		}
		fw.cur = 0
	}
	return n, err
}

type limitReader struct {
	parquet.RowReader
	limit int
	cur   int
}

func newLimitReader(r parquet.RowReader, limit int) parquet.RowReader {
	return &limitReader{RowReader: r, limit: limit}
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
