// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos-parquet-gateway/internal/encoding"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type indexRowReader struct {
	ctx context.Context

	closers []io.Closer

	seriesSet storage.ChunkSeriesSet

	schema     *parquet.Schema
	rowBuilder *parquet.RowBuilder

	concurrency int

	chunksColumn0    int
	chunksColumn1    int
	chunksColumn2    int
	labelIndexColumn int
	labelHashColumn  int
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
