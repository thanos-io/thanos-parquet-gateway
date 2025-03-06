// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/hashicorp/go-multierror"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type indexRowReader struct {
	ctx context.Context

	closers []io.Closer

	seriesSet storage.ChunkSeriesSet

	rowBuilder *parquet.RowBuilder
	schema     *parquet.Schema

	chunksColumn0 int
	chunksColumn1 int
	chunksColumn2 int

	m map[string]map[string]struct{}
}

var _ parquet.RowReader = &indexRowReader{}

func newIndexRowReader(ctx context.Context, mint, maxt int64, blks []Convertible) (*indexRowReader, error) {
	var (
		lbls       = make([]string, 0)
		seriesSets = make([]storage.ChunkSeriesSet, 0, len(blks))
		closers    = make([]io.Closer, 0, len(blks))
	)
	for _, blk := range blks {
		indexr, err := blk.Index()
		if err != nil {
			return nil, fmt.Errorf("unable to get index reader from block: %s", err)
		}
		closers = append(closers, indexr)

		chunkr, err := blk.Chunks()
		if err != nil {
			return nil, fmt.Errorf("unable to get chunk reader from block: %s", err)
		}
		closers = append(closers, chunkr)

		tombsr, err := blk.Tombstones()
		if err != nil {
			return nil, fmt.Errorf("unable to get tombstone reader from block: %s", err)
		}
		closers = append(closers, tombsr)

		lblns, err := indexr.LabelNames(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to get label names from block: %s", err)
		}
		lbls = append(lbls, lblns...)

		postings := tsdb.AllSortedPostings(ctx, indexr)
		seriesSet := tsdb.NewBlockChunkSeriesSet(blk.Meta().ULID, indexr, chunkr, tombsr, postings, mint, maxt, false)
		seriesSets = append(seriesSets, seriesSet)
	}
	slices.Sort(lbls)

	cseriesSet := storage.NewMergeChunkSeriesSet(seriesSets, storage.NewConcatenatingChunkSeriesMerger())
	s := schema.BuildSchemaFromLabels(slices.Compact(lbls))

	return &indexRowReader{
		ctx:       ctx,
		seriesSet: cseriesSet,
		closers:   closers,

		rowBuilder: parquet.NewRowBuilder(s),
		schema:     s,

		chunksColumn0: columnIDForKnownColumn(s, schema.ChunksColumn0),
		chunksColumn1: columnIDForKnownColumn(s, schema.ChunksColumn1),
		chunksColumn2: columnIDForKnownColumn(s, schema.ChunksColumn2),

		m: make(map[string]map[string]struct{}),
	}, nil
}

func columnIDForKnownColumn(schema *parquet.Schema, columnName string) int {
	lc, _ := schema.Lookup(columnName)
	return lc.ColumnIndex
}

func (rr *indexRowReader) Close() error {
	err := &multierror.Error{}
	for i := range rr.closers {
		err = multierror.Append(err, rr.closers[i].Close())
	}
	return err.ErrorOrNil()
}

func (rr *indexRowReader) Schema() *parquet.Schema {
	return rr.schema
}

func (rr *indexRowReader) NameLabelMapping() map[string]map[string]struct{} {
	return rr.m
}

func (rr *indexRowReader) ReadRows(buf []parquet.Row) (int, error) {
	select {
	case <-rr.ctx.Done():
		return 0, rr.ctx.Err()
	default:
	}

	var it chunks.Iterator

	i := 0
	for i < len(buf) && rr.seriesSet.Next() {
		rr.rowBuilder.Reset()
		s := rr.seriesSet.At()
		it = s.Iterator(it)

		chkBytes, err := collectChunks(it)
		if err != nil {
			return i, fmt.Errorf("unable to collect chunks: %s", err)
		}

		// skip series that have no chunks in the requested time
		if allChunksEmpty(chkBytes) {
			continue
		}

		metricName := s.Labels().Get(labels.MetricName)
		nameMap, ok := rr.m[metricName]
		if !ok {
			nameMap = make(map[string]struct{})
		}
		rr.m[metricName] = nameMap
		s.Labels().Range(func(l labels.Label) {
			colName := schema.LabelNameToColumn(l.Name)
			lc, _ := rr.schema.Lookup(colName)
			rr.rowBuilder.Add(lc.ColumnIndex, parquet.ValueOf(l.Value))
			if l.Name != labels.MetricName {
				nameMap[colName] = struct{}{}
			}
		})

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
	if i < len(buf) {
		return i, io.EOF
	}
	return i, rr.seriesSet.Err()
}
