// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

const (
	// This label gets added when we honor projections, it is used so we can still perform
	// horizontal merges of series sets even when we dont load all labels. It is also used
	// in the query engine for similar purposes, i.e. binary joins when we dont load all labels.
	SeriesHashLabel = "__cf_series_hash__"
)

const (
	LabelColumnPrefix = "___cf_meta_label_"
	LabelIndexColumn  = "___cf_meta_index"
	LabelHashColumn   = "___cf_meta_hash"
	ChunksColumn0     = "___cf_meta_chunk_0"
	ChunksColumn1     = "___cf_meta_chunk_1"
	ChunksColumn2     = "___cf_meta_chunk_2"
)

const (
	ChunkColumnLength  = 8 * time.Hour
	ChunkColumnsPerDay = 3
)

const (
	// V0 blocks contain a map[__name__] ~ columns for that series
	V0 = 0
	// V1 contains a column in the label parquet file that contains an encoded list of indexes that correspond
	// to columns that the row has populated. This will lift the constraint on needing to have
	// a matcher on the __name__ label present, as we can compute the necessary column projection
	// dynamically from the matching rows.
	V1 = 1
	// V2 contains a column in the chunks parquet file for the hash of the labels of the timeseries, this makes
	// it possible to project labels and still join series horizontally. We write this into the chunks parquet
	// file because its essentially random numbers that would bloat the labels file too much
	V2 = 2
)

func ChunkColumnName(i int) (string, bool) {
	switch i {
	case 0:
		return ChunksColumn0, true
	case 1:
		return ChunksColumn1, true
	case 2:
		return ChunksColumn2, true
	}
	return "", false
}

func LabelNameToColumn(lbl string) string {
	return fmt.Sprintf("%s%s", LabelColumnPrefix, lbl)
}

func ColumnToLabelName(col string) string {
	return strings.TrimPrefix(col, LabelColumnPrefix)
}

// ChunkColumnIndexForMint returns the chunk column index for a minimum timestamp.
// Use absolute UTC hour to determine chunk column, matching the write side in convert/chunks.go.
func ChunkColumnIndexForMint(t time.Time) (int, bool) {
	hour := t.UTC().Hour()
	colIdx := (hour / int(ChunkColumnLength.Hours())) % ChunkColumnsPerDay
	return colIdx, true
}

// ChunkColumnIndexForMaxt returns the chunk column index for a maximum timestamp.
// For max timestamps, we need special handling: if the time is exactly at a chunk boundary
// (e.g., 00:00, 08:00, 16:00), data up to that point is in the previous chunk.
// For example, query_maxt=00:00 should include chunk_2 (16:00-24:00), not chunk_0.
func ChunkColumnIndexForMaxt(t time.Time) (int, bool) {
	utc := t.UTC()
	hour := utc.Hour()
	minute := utc.Minute()
	second := utc.Second()
	nano := utc.Nanosecond()

	// If exactly at a chunk boundary, use the previous chunk
	// (since maxt is exclusive, 00:00 means "up to but not including 00:00")
	if minute == 0 && second == 0 && nano == 0 && hour%int(ChunkColumnLength.Hours()) == 0 {
		if hour == 0 {
			// Midnight: data up to 00:00 is in chunk_2 (16:00-24:00 of previous day)
			return ChunkColumnsPerDay - 1, true
		}
		// Otherwise, previous chunk
		hour = hour - 1
	}

	colIdx := (hour / int(ChunkColumnLength.Hours())) % ChunkColumnsPerDay
	return colIdx, true
}

func BuildSchemaFromLabels(lbls []string) *parquet.Schema {
	g := make(parquet.Group)

	g[LabelIndexColumn] = parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaLengthByteArray)
	g[LabelHashColumn] = parquet.Encoded(parquet.Leaf(parquet.Int64Type), &parquet.Plain)

	for _, lbl := range lbls {
		g[LabelNameToColumn(lbl)] = parquet.Optional(parquet.Encoded(parquet.String(), &parquet.RLEDictionary))
	}

	chunkNode := parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.DeltaLengthByteArray)
	g[ChunksColumn0] = chunkNode
	g[ChunksColumn1] = chunkNode
	g[ChunksColumn2] = chunkNode
	return parquet.NewSchema("tsdb", g)
}

func WithCompression(s *parquet.Schema) *parquet.Schema {
	g := make(parquet.Group)

	for _, c := range s.Columns() {
		lc, _ := s.Lookup(c...)
		g[lc.Path[0]] = parquet.Compressed(lc.Node, &zstd.Codec{Level: zstd.SpeedBetterCompression, Concurrency: 4})
	}

	return parquet.NewSchema("compressed", g)
}

var (
	ChunkColumns = []string{LabelHashColumn, ChunksColumn0, ChunksColumn1, ChunksColumn2}
)

func ChunkProjection(s *parquet.Schema) *parquet.Schema {
	g := make(parquet.Group)

	for _, c := range ChunkColumns {
		lc, ok := s.Lookup(c)
		if !ok {
			continue
		}
		g[c] = lc.Node
	}
	return parquet.NewSchema("chunk-projection", g)
}

func LabelsProjection(s *parquet.Schema) *parquet.Schema {
	g := make(parquet.Group)

	for _, c := range s.Columns() {
		if slices.Contains(ChunkColumns, c[0]) {
			continue
		}
		lc, ok := s.Lookup(c...)
		if !ok {
			continue
		}
		g[c[0]] = lc.Node
	}
	return parquet.NewSchema("labels-projection", g)
}

func RemoveNullColumns(p *parquet.File) *parquet.Schema {
	g := make(parquet.Group)

	s := p.Schema()
	cidxs := p.ColumnIndexes()
	nrg := len(p.RowGroups())

	for i, c := range s.Columns() {
		nps := make([]bool, 0)
		for j := range nrg * len(s.Columns()) {
			if j%len(s.Columns()) == i {
				nps = append(nps, cidxs[j].NullPages...)
			}
		}
		if !slices.ContainsFunc(nps, func(np bool) bool { return !np }) {
			continue
		}
		lc, ok := s.Lookup(c...)
		if !ok {
			continue
		}
		g[c[0]] = lc.Node
	}
	return parquet.NewSchema("remove-nulls-projection", g)
}
