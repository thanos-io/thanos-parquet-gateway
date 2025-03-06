// Copyright (c) 2025 Cloudflare, Inc.
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
	LabelColumnPrefix = "___cf_meta_label_"
	ChunksColumn0     = "___cf_meta_chunk_0"
	ChunksColumn1     = "___cf_meta_chunk_1"
	ChunksColumn2     = "___cf_meta_chunk_2"
)

const (
	ChunkColumnLength  = 8 * time.Hour
	ChunkColumnsPerDay = 3
)

func LabelNameToColumn(lbl string) string {
	return fmt.Sprintf("%s%s", LabelColumnPrefix, lbl)
}

func ColumnToLabelName(col string) string {
	return strings.TrimPrefix(col, LabelColumnPrefix)
}

func BuildSchemaFromLabels(lbls []string) *parquet.Schema {
	g := make(parquet.Group)

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
		g[lc.Path[0]] = parquet.Compressed(lc.Node, &zstd.Codec{Level: zstd.SpeedBetterCompression})
	}

	return parquet.NewSchema("uncompressed", g)
}

func Projection(schema *parquet.Schema, projections []string) *parquet.Schema {
	g := make(parquet.Group)

	for i := range projections {
		lc, ok := schema.Lookup(projections[i])
		if !ok {
			continue
		}
		g[projections[i]] = lc.Node
	}
	return parquet.NewSchema("projection", g)
}

var (
	ChunkColumns = []string{ChunksColumn0, ChunksColumn1, ChunksColumn2}
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

func Joined(l, r *parquet.Schema) *parquet.Schema {
	g := make(parquet.Group)

	for _, c := range l.Columns() {
		lc, ok := l.Lookup(c...)
		if !ok {
			continue
		}
		g[c[0]] = lc.Node
	}
	for _, c := range r.Columns() {
		lc, ok := r.Lookup(c...)
		if !ok {
			continue
		}
		g[c[0]] = lc.Node
	}
	return parquet.NewSchema("joined", g)
}
