// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos-parquet-gateway/internal/encoding"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

type indexRowReader struct {
	ctx context.Context

	closers []io.Closer

	seriesSet storage.ChunkSeriesSet

	schema           *parquet.Schema
	chunksProjection schema.Projection
	labelsProjection schema.Projection
	rowBuilder       *parquet.RowBuilder

	concurrency int

	chunksColumn0    int
	chunksColumn1    int
	chunksColumn2    int
	labelIndexColumn int
	labelHashColumn  int

	countColumn   int
	sumColumn     int
	minColumn     int
	maxColumn     int
	counterColumn int

	downsampled bool
}

var _ parquet.RowReader = &indexRowReader{}

func columnIDForKnownColumn(schema *parquet.Schema, columnName string) int {
	lc, _ := schema.Lookup(columnName)
	return lc.ColumnIndex
}

func (rr *indexRowReader) Close() error {
	errs := make([]error, 0)
	for i := range rr.closers {
		if err := rr.closers[i].Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close %q-th closer: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (rr *indexRowReader) Schema() *parquet.Schema {
	return rr.schema
}

func (rr *indexRowReader) ReadRows(buf []parquet.Row) (int, error) {
	if rr.downsampled {
		return rr.readDownsampledRows(buf)
	}
	return rr.readNonDownsampledRows(buf)
}

func (rr *indexRowReader) readDownsampledRows(buf []parquet.Row) (int, error) {
	select {
	case <-rr.ctx.Done():
		return 0, rr.ctx.Err()
	default:
	}

	// Same as other but with downsampled chunk collection.
	type chunkAggregatesOrError struct {
		res aggregateBytes

		err error
	}
	type chunkSeriesPromise struct {
		s storage.ChunkSeries
		c chan chunkAggregatesOrError
	}

	chunkSeriesC := make(chan chunkSeriesPromise, rr.concurrency)

	go func() {
		defer close(chunkSeriesC)
		for j := 0; j < len(buf) && rr.seriesSet.Next(); j++ {
			s := rr.seriesSet.At()
			it := s.Iterator(nil)

			promise := chunkSeriesPromise{
				s: s,
				c: make(chan chunkAggregatesOrError, 1),
			}

			chunkSeriesC <- promise

			go func() {
				res, err := collectDownsampledChunks(it)
				promise.c <- chunkAggregatesOrError{res: res, err: err}
				close(promise.c)
			}()
		}
	}()

	colIdxSlice := make([]int, 0)
	i, j := 0, 0
	for promise := range chunkSeriesC {
		j++

		rr.rowBuilder.Reset()
		colIdxSlice = colIdxSlice[:0]

		chkAggregatesOrError := <-promise.c
		if err := chkAggregatesOrError.err; err != nil {
			return i, err
		}
		res := chkAggregatesOrError.res
		chkLbls := promise.s.Labels()

		chkLbls.Range(func(l labels.Label) {
			colName := schema.LabelNameToColumn(l.Name)
			lc, _ := rr.schema.Lookup(colName)
			rr.rowBuilder.Add(lc.ColumnIndex, parquet.ValueOf(l.Value))
			colIdxSlice = append(colIdxSlice, lc.ColumnIndex-schema.ChunkColumnsPerDay)
		})
		rr.rowBuilder.Add(rr.labelIndexColumn, parquet.ValueOf(encoding.EncodeLabelColumnIndex(colIdxSlice)))
		rr.rowBuilder.Add(rr.labelHashColumn, parquet.ValueOf(chkLbls.Hash()))

		if allAggregatesEmpty(res) {
			continue
		}

		// Use a byte array and store all the samples, with timestamps and values
		rr.rowBuilder.Add(rr.countColumn, parquet.ValueOf(res[downsample.AggrCount]))
		rr.rowBuilder.Add(rr.sumColumn, parquet.ValueOf(res[downsample.AggrSum]))
		rr.rowBuilder.Add(rr.minColumn, parquet.ValueOf(res[downsample.AggrMin]))
		rr.rowBuilder.Add(rr.maxColumn, parquet.ValueOf(res[downsample.AggrMax]))
		rr.rowBuilder.Add(rr.counterColumn, parquet.ValueOf(res[downsample.AggrCounter]))
		buf[i] = rr.rowBuilder.AppendRow(buf[i][:0])
		i++
	}
	if j < len(buf) {
		return i, io.EOF
	}
	return i, rr.seriesSet.Err()
}

func allAggregatesEmpty(aggBytes aggregateBytes) bool {
	for _, chk := range aggBytes {
		if len(chk) != 0 {
			return false
		}
	}
	return true
}

func (rr *indexRowReader) readNonDownsampledRows(buf []parquet.Row) (int, error) {
	select {
	case <-rr.ctx.Done():
		return 0, rr.ctx.Err()
	default:
	}

	type chkBytesOrError struct {
		chkBytes [schema.ChunkColumnsPerDay][]byte
		err      error
	}
	type chunkSeriesPromise struct {
		s storage.ChunkSeries
		c chan chkBytesOrError
	}

	chunkSeriesC := make(chan chunkSeriesPromise, rr.concurrency)

	go func() {
		defer close(chunkSeriesC)
		for j := 0; j < len(buf) && rr.seriesSet.Next(); j++ {
			s := rr.seriesSet.At()
			it := s.Iterator(nil)

			promise := chunkSeriesPromise{
				s: s,
				c: make(chan chkBytesOrError, 1),
			}

			chunkSeriesC <- promise

			go func() {
				chkBytes, err := collectChunks(it)
				promise.c <- chkBytesOrError{chkBytes: chkBytes, err: err}
				close(promise.c)
			}()
		}
	}()

	colIdxSlice := make([]int, 0)
	i, j := 0, 0
	for promise := range chunkSeriesC {
		j++

		rr.rowBuilder.Reset()
		colIdxSlice = colIdxSlice[:0]

		chkBytesOrError := <-promise.c
		if err := chkBytesOrError.err; err != nil {
			return i, err
		}
		chkBytes := chkBytesOrError.chkBytes
		chkLbls := promise.s.Labels()

		chkLbls.Range(func(l labels.Label) {
			colName := schema.LabelNameToColumn(l.Name)
			lc, _ := rr.schema.Lookup(colName)
			rr.rowBuilder.Add(lc.ColumnIndex, parquet.ValueOf(l.Value))
			// we need to address for projecting chunk columns away later so we need to correct for the offset here
			colIdxSlice = append(colIdxSlice, lc.ColumnIndex-schema.ChunkColumnsPerDay-1)
		})
		rr.rowBuilder.Add(rr.labelIndexColumn, parquet.ValueOf(encoding.EncodeLabelColumnIndex(colIdxSlice)))
		rr.rowBuilder.Add(rr.labelHashColumn, parquet.ValueOf(chkLbls.Hash()))

		if allChunksEmpty(chkBytes) {
			continue
		}
		for idx, chk := range chkBytes {
			if len(chk) == 0 {
				continue
			}
			switch idx {
			case 0:
				rr.rowBuilder.Add(rr.chunksColumn0, parquet.ValueOf(chk))
			case 1:
				rr.rowBuilder.Add(rr.chunksColumn1, parquet.ValueOf(chk))
			case 2:
				rr.rowBuilder.Add(rr.chunksColumn2, parquet.ValueOf(chk))
			}
		}

		buf[i] = rr.rowBuilder.AppendRow(buf[i][:0])
		i++
	}
	if j < len(buf) {
		return i, io.EOF
	}
	return i, rr.seriesSet.Err()
}

func allChunksEmpty(chkBytes [schema.ChunkColumnsPerDay][]byte) bool {
	for _, chk := range chkBytes {
		if len(chk) != 0 {
			return false
		}
	}
	return true
}

type downsampledSeries struct {
	lset  labels.Labels
	metas []chunks.Meta
}

func (d *downsampledSeries) Labels() labels.Labels {
	return d.lset
}

func (d *downsampledSeries) Iterator(it chunks.Iterator) chunks.Iterator {
	if dsit, ok := it.(*downsampledSeriesIterator); ok {
		dsit.metas = d.metas
		dsit.i = -1
		return dsit
	}
	return &downsampledSeriesIterator{metas: d.metas, i: -1}
}

type downsampledSeriesIterator struct {
	metas []chunks.Meta
	i     int
}

func (it *downsampledSeriesIterator) Next() bool {
	it.i++
	return it.i < len(it.metas)
}

func (it *downsampledSeriesIterator) At() chunks.Meta {
	return it.metas[it.i]
}

func (it *downsampledSeriesIterator) Err() error {
	return nil
}

type concatChunkSeriesSet struct {
	series []storage.ChunkSeries
	idx    int
}

func newConcatChunkSeriesSet(series ...storage.ChunkSeries) storage.ChunkSeriesSet {
	return &concatChunkSeriesSet{series: series, idx: -1}
}

func (c *concatChunkSeriesSet) Next() bool {
	c.idx++
	return c.idx < len(c.series)
}

func (c *concatChunkSeriesSet) At() storage.ChunkSeries {
	return c.series[c.idx]
}

func (c *concatChunkSeriesSet) Err() error {
	return nil
}

func (c *concatChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func buildDownsampledSeriesSet(ctx context.Context, indexr tsdb.IndexReader, chunkr tsdb.ChunkReader, tombsr tombstones.Reader, mint, maxt int64, sortFn func(a, b labels.Labels) int) (storage.ChunkSeriesSet, error) {
	postings := tsdb.AllSortedPostings(ctx, indexr)
	lb := labels.NewScratchBuilder(16)
	series := make([]storage.ChunkSeries, 0, 1024)
	for postings.Next() {
		ref := postings.At()
		lb.Reset()
		var metas []chunks.Meta
		if err := indexr.Series(ref, &lb, &metas); err != nil {
			return nil, fmt.Errorf("series expansion: %w", err)
		}
		full := make([]chunks.Meta, 0, len(metas))
		for _, m := range metas {
			if m.MaxTime < mint || m.MinTime > maxt {
				continue
			}
			// Load chunk bytes or subset via ChunkOrIterable.
			ch, iterable, cerr := chunkr.ChunkOrIterable(m)
			if cerr != nil {
				// log skip verbose
				// fmt.Printf("downsample: skip chunk ref=%d err=%v\n", m.Ref, cerr)
				continue // skip unreadable
			}
			if iterable != nil {
				// Materialize samples from iterable into a single XOR chunk.
				it := iterable.Iterator(nil)
				xc := chunkenc.NewXORChunk()
				app, _ := xc.Appender()
				var smint, smaxt int64
				first := true
				for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
					if vt != chunkenc.ValFloat { // skip non-float samples (histograms) for now
						continue
					}
					ts, v := it.At()
					if first {
						smint = ts
						first = false
					}
					smaxt = ts
					app.Append(ts, v)
				}
				if it.Err() == nil && !first {
					full = append(full, chunks.Meta{MinTime: smint, MaxTime: smaxt, Chunk: xc})
				}
				continue
			}
			// Normal chunk path.
			full = append(full, chunks.Meta{MinTime: m.MinTime, MaxTime: m.MaxTime, Chunk: ch})
		}
		// Tombstone filtering if available.
		if tombsr != nil {
			filtered := full[:0]
			ivs, terr := tombsr.Get(ref)
			if terr == nil && len(ivs) > 0 {
				for _, cm := range full {
					covered := false
					for _, iv := range ivs {
						if cm.MinTime <= iv.Maxt && iv.Mint <= cm.MaxTime {
							covered = true
							break
						}
					}
					if !covered {
						filtered = append(filtered, cm)
					}
				}
				full = filtered
			}
		}
		series = append(series, &downsampledSeries{lset: lb.Labels(), metas: full})
	}
	if err := postings.Err(); err != nil {
		return nil, fmt.Errorf("postings error: %w", err)
	}
	slices.SortFunc(series, func(a, b storage.ChunkSeries) int { return sortFn(a.Labels(), b.Labels()) })
	return newConcatChunkSeriesSet(series...), nil
}
