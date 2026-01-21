// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"math"

	"slices"
	"sync"
	"time"

	"github.com/efficientgo/core/errcapture"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos-parquet-gateway/internal/encoding"
	"github.com/thanos-io/thanos-parquet-gateway/internal/limits"
	"github.com/thanos-io/thanos-parquet-gateway/internal/tracing"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/internal/warnings"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

// materializeSeries reconstructs the ChunkSeries that belong to the specified row ranges (rr).
// It uses the row group index (rgi) and time bounds (mint, maxt) to filter and decode the series.
func materializeSeries(
	ctx context.Context,
	m SelectReadMeta,
	rgi int,
	mint,
	maxt int64,
	hints *storage.SelectHints,
	rr []rowRange,
) ([]SeriesChunks, annotations.Annotations, error) {
	var annos annotations.Annotations

	if limit := hints.Limit; limit > 0 {
		annos.Add(warnings.ErrorTruncatedResponse)
		// Series are all different so we can actually limit the rowranges themselves
		// This would not work for Label APIs but for series its ok.
		rr = limitRowRanges(int64(limit), rr)
	}

	if err := checkRowQuota(m.RowCountQuota, rr); err != nil {
		return nil, annos, err
	}
	res := make([]SeriesChunks, totalRows(rr))

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		sLbls, err := materializeLabels(ctx, hints, m, rgi, rr)
		if err != nil {
			return fmt.Errorf("error materializing labels: %w", err)
		}

		for i, s := range sLbls {
			m.ExternalLabels.Range(func(lbl labels.Label) { s.Set(lbl.Name, lbl.Value) })
			s.Del(m.ReplicaLabelNames...)

			lbls := s.Labels()
			h := lbls.Hash()

			res[i].Lset = lbls
			res[i].LsetHash = h
		}
		return nil
	})

	g.Go(func() error {
		if hints.Func == "series" {
			return nil
		}
		chks, err := materializeChunks(ctx, m, rgi, mint, maxt, rr)
		if err != nil {
			return fmt.Errorf("unable to materialize chunks: %w", err)
		}

		for i, c := range chks {
			res[i].Chunks = append(res[i].Chunks, c...)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return res, annos, fmt.Errorf("unable to materialize series: %w", err)
	}
	return res, annos, nil
}

func materializeLabels(ctx context.Context, h *storage.SelectHints, m SelectReadMeta, rgi int, rr []rowRange) ([]labels.Builder, error) {
	switch v := m.Meta.Version; v {
	case schema.V0:
		return materializeLabelsV0(ctx, m, rgi, rr)
	case schema.V1, schema.V2:
		return materializeLabelsV1(ctx, h, m, rgi, rr)
	default:
		return nil, fmt.Errorf("unable to materialize labels for block of version %q", v)
	}
}

// v0 blocks have a map to resolve column names for a metric
func materializeLabelsV0(ctx context.Context, m SelectReadMeta, rgi int, rr []rowRange) ([]labels.Builder, error) {
	// NOTE: v0 blocks did not have a labelhash column so we dont need to worry about projections

	rowCount := totalRows(rr)

	metricNameColumn := schema.LabelNameToColumn(labels.MetricName)

	lc, ok := m.LabelPfile.Schema().Lookup(metricNameColumn)
	if !ok {
		return nil, fmt.Errorf("unable to to find column %q", metricNameColumn)
	}

	rg := m.LabelPfile.RowGroups()[rgi]

	cc := rg.ColumnChunks()[lc.ColumnIndex]
	metricNames, err := materializeLabelColumn(ctx, rg, cc, rr)
	if err != nil {
		return nil, fmt.Errorf("unable to materialize metric name column: %w", err)
	}

	seen := make(map[string]struct{})
	colsMap := make(map[int]*[]parquet.Value, 10)

	v := make([]parquet.Value, 0, rowCount)
	colsMap[lc.ColumnIndex] = &v
	for _, nm := range metricNames {
		key := yoloString(nm.ByteArray())
		if _, ok := seen[key]; !ok {
			cols := m.Meta.ColumnsForName[key]
			for _, c := range cols {
				lc, ok := m.LabelPfile.Schema().Lookup(c)
				if !ok {
					continue
				}
				colsMap[lc.ColumnIndex] = &[]parquet.Value{}
			}
			seen[key] = struct{}{}
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	for cIdx, v := range colsMap {
		g.Go(func() error {
			cc := rg.ColumnChunks()[cIdx]
			values, err := materializeLabelColumn(ctx, rg, cc, rr)
			if err != nil {
				return fmt.Errorf("unable to materialize labels values: %w", err)
			}
			*v = values
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	builders := make([]labels.Builder, rowCount)
	for cIdx, values := range colsMap {
		colName := m.LabelPfile.Schema().Columns()[cIdx][0]
		labelName := schema.ColumnToLabelName(colName)

		for i, value := range *values {
			if value.IsNull() {
				continue
			}
			builders[i].Set(labelName, yoloString(value.ByteArray()))
		}
	}
	return builders, nil
}

// v1+ blocks have a cf_meta_index column that contains an index which columns to resolve for a metric
func materializeLabelsV1(ctx context.Context, h *storage.SelectHints, m SelectReadMeta, rgi int, rr []rowRange) ([]labels.Builder, error) {
	// TODO: refactor this maybe? interleaving "useProjections" with normal materialization is a bit ugly...

	rowCount := totalRows(rr)
	s := m.LabelPfile.Schema()

	// We only use projections for inclusive cases, as in we know what labels to add, not
	// what labels to take away. We also only use them if the shards we use for the query
	// all support them.
	useProjections := m.HonorProjectionHints && h.ProjectionInclude

	lrg := m.LabelPfile.RowGroups()[rgi]
	crg := m.ChunkPfile.RowGroups()[rgi]

	colsMap := make(map[int]*[]parquet.Value, 10)
	if useProjections {
		for _, labelName := range h.ProjectionLabels {
			col, ok := s.Lookup(schema.LabelNameToColumn(labelName))
			if !ok {
				// we asked to aggregate by a label that does not exist, we can just ignore it
				continue
			}
			colsMap[col.ColumnIndex] = &[]parquet.Value{}
		}
	} else {
		lc, ok := s.Lookup(schema.LabelIndexColumn)
		if !ok {
			return nil, fmt.Errorf("unable to to find label index column %q", schema.LabelIndexColumn)
		}
		cci := lc.ColumnIndex
		cc := lrg.ColumnChunks()[cci]

		colsIdxs, err := materializeLabelColumn(ctx, lrg, cc, rr)
		if err != nil {
			return nil, fmt.Errorf("unable to materialize label index column: %w", err)
		}

		seen := make(map[string]struct{})
		for _, colsIdx := range colsIdxs {
			key := yoloString(colsIdx.ByteArray())
			if _, ok := seen[key]; !ok {
				idxs, err := encoding.DecodeLabelColumnIndex(colsIdx.ByteArray())
				if err != nil {
					return nil, fmt.Errorf("unable to decode column index: %w", err)
				}
				for _, idx := range idxs {
					colsMap[idx] = &[]parquet.Value{}
				}
				seen[key] = struct{}{}
			}
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	for cci, v := range colsMap {
		g.Go(func() error {
			cc := lrg.ColumnChunks()[cci]
			values, err := materializeLabelColumn(ctx, lrg, cc, rr)
			if err != nil {
				return fmt.Errorf("unable to materialize labels values: %w", err)
			}
			*v = values
			return nil
		})
	}

	var hashes []parquet.Value
	if useProjections {
		g.Go(func() error {
			// NOTE: label hash is persisted into the chunk file because its a bigger column
			col, ok := m.ChunkPfile.Schema().Lookup(schema.LabelHashColumn)
			if !ok {
				return fmt.Errorf("unable to to find label hash column %q", schema.LabelHashColumn)
			}
			cci := col.ColumnIndex
			cc := crg.ColumnChunks()[cci]

			h, err := materializeChunkColumn(
				ctx,
				crg,
				cc,
				rr,
				withReaderFromContext(m.ChunkFileReaderFromContext),
			)
			if err != nil {
				return fmt.Errorf("unable to materialize hash values: %w", err)
			}
			hashes = h
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	builders := make([]labels.Builder, rowCount)
	for cci, values := range colsMap {
		colName := s.Columns()[cci][0]
		labelName := schema.ColumnToLabelName(colName)

		for i, value := range *values {
			if value.IsNull() {
				continue
			}
			builders[i].Set(labelName, yoloString(value.ByteArray()))
		}
	}

	if useProjections {
		for i := range hashes {
			builders[i].Set(schema.SeriesHashLabel, yoloString(hashes[i].Bytes()))
		}
	}
	return builders, nil
}

func materializeChunks(
	ctx context.Context,
	m SelectReadMeta,
	rgi int,
	mint int64,
	maxt int64,
	rr []rowRange,
) ([][]chunks.Meta, error) {
	rowCount := totalRows(rr)

	minChunkCol, ok := schema.ChunkColumnIndex(m.Meta, time.UnixMilli(mint))
	if !ok {
		return nil, errors.New("unable to find min chunk column")
	}
	maxChunkCol, ok := schema.ChunkColumnIndex(m.Meta, time.UnixMilli(maxt))
	if !ok {
		return nil, errors.New("unable to find max chunk column")
	}
	rg := m.ChunkPfile.RowGroups()[rgi]

	numCols := maxChunkCol - minChunkCol + 1

	perColValues := make([][]parquet.Value, numCols)
	g, ctx := errgroup.WithContext(ctx)
	for i := minChunkCol; i <= maxChunkCol; i++ {
		colName, ok := schema.ChunkColumnName(i)
		if !ok {
			return nil, fmt.Errorf("unable to find chunk column for column index %d", i)
		}
		col, ok := rg.Schema().Lookup(colName)
		if !ok {
			return nil, fmt.Errorf("unable to find chunk column for column name %q", colName)
		}
		cci := col.ColumnIndex
		cc := rg.ColumnChunks()[cci]

		g.Go(func() error {
			values, err := materializeChunkColumn(
				ctx,
				rg,
				cc,
				rr,
				withByteQuota(m.ChunkBytesQuota),
				withReaderFromContext(m.ChunkFileReaderFromContext),
				withPartitionMaxRangeSize(m.ChunkPagePartitionMaxRange),
				withPartitionMaxGapSize(m.ChunkPagePartitionMaxGap),
				withPartitionMaxConcurrency(m.ChunkPagePartitionMaxConcurrency),
			)
			if err != nil {
				return fmt.Errorf("unable to materialize column: %w", err)
			}
			perColValues[i-minChunkCol] = values
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to process chunks: %w", err)
	}

	chunkCounts := make([]int, rowCount)
	totalChunks := 0
	for _, values := range perColValues {
		for j, chkVal := range values {
			for range chunkHeaders(chkVal.ByteArray(), mint, maxt) {
				chunkCounts[j]++
				totalChunks++
			}
		}
	}

	backing := make([]chunks.Meta, totalChunks)
	r := make([][]chunks.Meta, rowCount)
	offset := 0
	for i := range r {
		r[i] = backing[offset : offset : offset+chunkCounts[i]]
		offset += chunkCounts[i]
	}

	for _, values := range perColValues {
		for j, chkVal := range values {
			for ch := range chunkHeaders(chkVal.ByteArray(), mint, maxt) {
				chk, err := chunkenc.FromData(ch.Enc, ch.Data)
				if err != nil {
					return nil, fmt.Errorf("unable to create chunk: %w", err)
				}
				r[j] = append(r[j], chunks.Meta{MinTime: ch.MinTime, MaxTime: ch.MaxTime, Chunk: chk})
			}
		}
	}

	return r, nil
}

type chunkHeader struct {
	MinTime int64
	MaxTime int64
	Enc     chunkenc.Encoding
	Data    []byte
}

func chunkHeaders(bs []byte, mint, maxt int64) func(yield func(chunkHeader) bool) {
	return func(yield func(chunkHeader) bool) {
		offset := 0
		for offset+24 <= len(bs) {
			enc := chunkenc.Encoding(binary.BigEndian.Uint32(bs[offset:]))
			offset += 4
			cmint := encoding.ZigZagDecode(binary.BigEndian.Uint64(bs[offset:]))
			offset += 8
			cmaxt := encoding.ZigZagDecode(binary.BigEndian.Uint64(bs[offset:]))
			offset += 8
			l := int(binary.BigEndian.Uint32(bs[offset:]))
			offset += 4
			if offset+l > len(bs) {
				break
			}
			if util.Intersects(mint, maxt, cmint, cmaxt) {
				if !yield(chunkHeader{MinTime: cmint, MaxTime: cmaxt, Enc: enc, Data: bs[offset : offset+l]}) {
					return
				}
			}
			offset += l
		}
	}
}

func materializeLabelNames(ctx context.Context, meta LabelNamesReadMeta, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	if err := checkRowQuota(meta.RowCountQuota, rr); err != nil {
		return nil, nil, err
	}

	switch v := meta.Meta.Version; v {
	case schema.V0:
		return materializeLabelNamesV0(ctx, meta, rgi, rr)
	case schema.V1, schema.V2:
		return materializeLabelNamesV1(ctx, meta, rgi, rr)
	default:
		return nil, nil, fmt.Errorf("unable to materialize labels names for block of version %q", v)
	}
}

func materializeLabelNamesV0(ctx context.Context, meta LabelNamesReadMeta, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	metricNameColumn := schema.LabelNameToColumn(labels.MetricName)

	lc, ok := meta.LabelPfile.Schema().Lookup(metricNameColumn)
	if !ok {
		return nil, annos, fmt.Errorf("unable to to find column %q", metricNameColumn)
	}

	rg := meta.LabelPfile.RowGroups()[rgi]

	cc := rg.ColumnChunks()[lc.ColumnIndex]
	metricNames, err := materializeLabelColumn(ctx, rg, cc, rr)
	if err != nil {
		return nil, annos, fmt.Errorf("unable to materialize metric name column: %w", err)
	}

	seen := make(map[string]struct{})
	colIdxs := make(map[int]struct{})
	for _, mn := range metricNames {
		key := yoloString(mn.ByteArray())
		if _, ok := seen[key]; !ok {
			cols := meta.Meta.ColumnsForName[key]
			for _, c := range cols {
				lc, ok := meta.LabelPfile.Schema().Lookup(c)
				if !ok {
					continue
				}
				colIdxs[lc.ColumnIndex] = struct{}{}
			}
		}
		seen[key] = struct{}{}
	}

	cols := meta.LabelPfile.Schema().Columns()

	res := make([]string, 0, len(colIdxs))
	for k := range colIdxs {
		res = append(res, schema.ColumnToLabelName(cols[k][0]))
	}
	return res, annos, nil

}

func materializeLabelNamesV1(ctx context.Context, meta LabelNamesReadMeta, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	lc, ok := meta.LabelPfile.Schema().Lookup(schema.LabelIndexColumn)
	if !ok {
		return nil, annos, fmt.Errorf("unable to to find label index column %q", schema.LabelIndexColumn)
	}

	rg := meta.LabelPfile.RowGroups()[rgi]

	cc := rg.ColumnChunks()[lc.ColumnIndex]
	colsIdxs, err := materializeLabelColumn(ctx, rg, cc, rr)
	if err != nil {
		return nil, annos, fmt.Errorf("unable to materialize label index column: %w", err)
	}

	seen := make(map[string]struct{})
	colIdxs := make(map[int]struct{})
	for _, colsIdx := range colsIdxs {
		key := yoloString(colsIdx.ByteArray())
		if _, ok := seen[key]; !ok {
			idxs, err := encoding.DecodeLabelColumnIndex(colsIdx.ByteArray())
			if err != nil {
				return nil, annos, fmt.Errorf("materializer failed to decode column index: %w", err)
			}
			for _, idx := range idxs {
				colIdxs[idx] = struct{}{}
			}
			seen[key] = struct{}{}
		}
	}

	cols := meta.LabelPfile.Schema().Columns()

	res := make([]string, 0, len(colIdxs))
	for k := range colIdxs {
		res = append(res, schema.ColumnToLabelName(cols[k][0]))
	}
	return res, annos, nil
}

func materializeLabelValues(ctx context.Context, meta LabelValuesReadMeta, name string, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	if err := checkRowQuota(meta.RowCountQuota, rr); err != nil {
		return nil, annos, err
	}

	switch v := meta.Meta.Version; v {
	case schema.V0, schema.V1, schema.V2:
		return materializeLabelValuesV0V1(ctx, meta, name, rgi, rr)
	default:
		return nil, nil, fmt.Errorf("unable to materialize labels values for block of version %q", v)
	}
}

func materializeLabelValuesV0V1(ctx context.Context, meta LabelValuesReadMeta, name string, rgi int, rr []rowRange) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	lc, ok := meta.LabelPfile.Schema().Lookup(schema.LabelNameToColumn(name))
	if !ok {
		return nil, annos, nil
	}

	rg := meta.LabelPfile.RowGroups()[rgi]

	cc := rg.ColumnChunks()[lc.ColumnIndex]
	vals, err := materializeLabelColumn(ctx, rg, cc, rr)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to materialize label %q column: %w", name, err)
	}

	seen := make(map[string]struct{})
	res := make([]string, 0)
	for _, v := range vals {
		if v.IsNull() {
			continue
		}
		key := yoloString(v.ByteArray())
		if _, ok := seen[key]; !ok {
			res = append(res, v.Clone().String())
			seen[key] = struct{}{}
		}
	}
	return res, annos, nil
}

type chunkMaterializeConfig struct {
	bytesQuota *limits.Quota

	readerFromContext readerFromContext

	partitionMaxRangeSize   uint64
	partitionMaxGapSize     uint64
	partitionMaxConcurrency int
}

type chunkMaterializeOption func(*chunkMaterializeConfig)

func withByteQuota(q *limits.Quota) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.bytesQuota = q
	}
}

func withPartitionMaxRangeSize(maxRangeSize uint64) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.partitionMaxRangeSize = maxRangeSize
	}
}

func withPartitionMaxGapSize(maxGapSize uint64) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.partitionMaxGapSize = maxGapSize
	}
}

func withPartitionMaxConcurrency(maxConcurrency int) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.partitionMaxConcurrency = maxConcurrency
	}
}

type readerFromContext func(ctx context.Context) io.ReaderAt

func withReaderFromContext(rx readerFromContext) chunkMaterializeOption {
	return func(cfg *chunkMaterializeConfig) {
		cfg.readerFromContext = rx
	}
}

func materializeChunkColumn(ctx context.Context, rg parquet.RowGroup, cc parquet.ColumnChunk, rr []rowRange, opts ...chunkMaterializeOption) ([]parquet.Value, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rowCount := totalRows(rr)
	column := rg.Schema().Columns()[cc.Column()][0]

	cfg := chunkMaterializeConfig{
		bytesQuota:            limits.UnlimitedQuota(),
		partitionMaxRangeSize: math.MaxUint64,
		partitionMaxGapSize:   math.MaxUint64,
	}
	for i := range opts {
		opts[i](&cfg)
	}
	ctx, span := tracing.Tracer().Start(ctx, "Materialize Column")
	defer span.End()

	span.SetAttributes(attribute.String("column", rg.Schema().Columns()[cc.Column()][0]))

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("could not get offset index: %w", err)
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("could not get column index: %w", err)
	}

	pagesToRowsMap := make(map[int][]rowRange, len(rr))
	for i := range cidx.NumPages() {
		pr := rowRange{
			from: oidx.FirstRowIndex(i),
		}
		pr.count = rg.NumRows()

		if i < oidx.NumPages()-1 {
			pr.count = oidx.FirstRowIndex(i+1) - pr.from
		}

		for _, r := range rr {
			if intersect(pr, r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], intersection(r, pr))
			}
		}
	}

	if err := checkByteQuota(cfg.bytesQuota, maps.Keys(pagesToRowsMap), oidx); err != nil {
		return nil, err
	}

	pageRanges := partitionPageRanges(
		cfg.partitionMaxRangeSize,
		cfg.partitionMaxGapSize,
		pagesToRowsMap,
		oidx,
	)

	r := make(map[rowRange][]parquet.Value, len(pageRanges))
	for _, v := range pageRanges {
		for _, rs := range v.rows {
			r[rs] = make([]parquet.Value, 0, rs.count)
		}
	}

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	if limit := cfg.partitionMaxConcurrency; limit != 0 {
		g.SetLimit(limit)
	}

	method := methodFromContext(ctx)
	columnMaterialized.WithLabelValues(column, method).Add(1)
	rowsMaterialized.WithLabelValues(column, method).Add(float64(rowCount))

	for _, p := range pageRanges {
		g.Go(func() (rerr error) {
			ctx, span := tracing.Tracer().Start(ctx, "Materialize Page Range")
			defer span.End()

			span.SetAttributes(attribute.IntSlice("pages", p.pages))

			rdrAt := cfg.readerFromContext(ctx)

			// TODO: read pages in one big read here - then use "PagesFrom" with a bytes.NewReader
			// that reads that byte slice - this prevents AsyncPages from overfetching and coalesces
			// small reads into one big read

			minOffset := oidx.Offset(p.pages[0])
			maxOffset := oidx.Offset(p.pages[len(p.pages)-1]) + oidx.CompressedPageSize(p.pages[len(p.pages)-1])

			bufRdrAt := newBufferedReaderAt(rdrAt, minOffset, maxOffset)

			pagesRead.WithLabelValues(column, method).Add(float64(len(p.pages)))
			pagesReadSize.WithLabelValues(column, method).Add(float64(maxOffset - minOffset))

			pgs := cc.(*parquet.FileColumnChunk).PagesFrom(bufRdrAt)
			defer errcapture.Do(&rerr, pgs.Close, "column chunk pages close")

			if err := pgs.SeekToRow(p.rows[0].from); err != nil {
				return fmt.Errorf("could not seek to row: %w", err)
			}

			vi := &chunkValuesIterator{}
			remainingRr := p.rows
			currentRr := remainingRr[0]
			next := currentRr.from
			remaining := currentRr.count
			currentRow := currentRr.from

			remainingRr = remainingRr[1:]
			for len(remainingRr) > 0 || remaining > 0 {
				page, err := pgs.ReadPage()
				if err != nil {
					return fmt.Errorf("unable to read page: %w", err)
				}

				vi.Reset(page)
				for vi.Next() {
					if currentRow == next {
						mu.Lock()
						r[currentRr] = append(r[currentRr], vi.At())
						mu.Unlock()
						remaining--
						if remaining > 0 {
							next = next + 1
						} else if len(remainingRr) > 0 {
							currentRr = remainingRr[0]
							next = currentRr.from
							remaining = currentRr.count
							remainingRr = remainingRr[1:]
						}
					}
					currentRow++
				}
				parquet.Release(page)

				if err := vi.Error(); err != nil {
					return fmt.Errorf("error during page iteration: %w", err)
				}
			}
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to materialize columns: %w", err)
	}

	ranges := slices.Collect(maps.Keys(r))
	slices.SortFunc(ranges, func(a, b rowRange) int {
		return int(a.from - b.from)
	})

	res := make([]parquet.Value, 0, totalRows(rr))
	for _, v := range ranges {
		res = append(res, r[v]...)
	}
	return res, nil
}

func materializeLabelColumn(ctx context.Context, rg parquet.RowGroup, cc parquet.ColumnChunk, rr []rowRange) ([]parquet.Value, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rowCount := totalRows(rr)
	column := rg.Schema().Columns()[cc.Column()][0]

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("could not get offset index: %w", err)
	}

	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("could not get column index: %w", err)
	}

	pagesToRowsMap := make(map[int][]rowRange, len(rr))
	for i := range cidx.NumPages() {
		pr := rowRange{
			from: oidx.FirstRowIndex(i),
		}
		pr.count = rg.NumRows()

		if i < oidx.NumPages()-1 {
			pr.count = oidx.FirstRowIndex(i+1) - pr.from
		}

		for _, r := range rr {
			if intersect(pr, r) {
				pagesToRowsMap[i] = append(pagesToRowsMap[i], intersection(r, pr))
			}
		}
	}

	pageRanges := partitionPageRanges(
		math.MaxUint64,
		math.MaxUint64,
		pagesToRowsMap,
		oidx,
	)

	r := make(map[rowRange][]parquet.Value, len(pageRanges))
	for _, v := range pageRanges {
		for _, rs := range v.rows {
			r[rs] = make([]parquet.Value, 0, rs.count)
		}
	}

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)

	method := methodFromContext(ctx)
	columnMaterialized.WithLabelValues(column, method).Add(1)
	rowsMaterialized.WithLabelValues(column, method).Add(float64(rowCount))

	for _, p := range pageRanges {
		g.Go(func() (rerr error) {
			minOffset := oidx.Offset(p.pages[0])
			maxOffset := oidx.Offset(p.pages[len(p.pages)-1]) + oidx.CompressedPageSize(p.pages[len(p.pages)-1])

			pagesRead.WithLabelValues(column, method).Add(float64(len(p.pages)))
			pagesReadSize.WithLabelValues(column, method).Add(float64(maxOffset - minOffset))

			pgs := cc.Pages()
			defer errcapture.Do(&rerr, pgs.Close, "column chunk pages close")

			if err := pgs.SeekToRow(p.rows[0].from); err != nil {
				return fmt.Errorf("could not seek to row: %w", err)
			}

			vi := &chunkValuesIterator{}
			remainingRr := p.rows
			currentRr := remainingRr[0]
			next := currentRr.from
			remaining := currentRr.count
			currentRow := currentRr.from

			remainingRr = remainingRr[1:]
			for len(remainingRr) > 0 || remaining > 0 {
				page, err := pgs.ReadPage()
				if err != nil {
					return fmt.Errorf("unable to read page: %w", err)
				}

				vi.Reset(page)
				for vi.Next() {
					if currentRow == next {
						mu.Lock()
						r[currentRr] = append(r[currentRr], vi.At())
						mu.Unlock()
						remaining--
						if remaining > 0 {
							next = next + 1
						} else if len(remainingRr) > 0 {
							currentRr = remainingRr[0]
							next = currentRr.from
							remaining = currentRr.count
							remainingRr = remainingRr[1:]
						}
					}
					currentRow++
				}
				parquet.Release(page)

				if err := vi.Error(); err != nil {
					return fmt.Errorf("error during page iteration: %w", err)
				}
			}
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to materialize columns: %w", err)
	}

	ranges := slices.Collect(maps.Keys(r))
	slices.SortFunc(ranges, func(a, b rowRange) int {
		return int(a.from - b.from)
	})

	res := make([]parquet.Value, 0, totalRows(rr))
	for _, v := range ranges {
		res = append(res, r[v]...)
	}
	return res, nil
}

func totalRows(rr []rowRange) int64 {
	res := int64(0)
	for _, r := range rr {
		res += r.count
	}
	return res
}

func totalBytes(pages iter.Seq[int], oidx parquet.OffsetIndex) int64 {
	res := int64(0)
	for i := range pages {
		res += oidx.CompressedPageSize(i)
	}
	return res
}

func checkRowQuota(rowQuota *limits.Quota, rr []rowRange) error {
	if err := rowQuota.Reserve(totalRows(rr)); err != nil {
		return fmt.Errorf("would use too many rows: %w", err)
	}
	return nil
}

func checkByteQuota(byteQuota *limits.Quota, pages iter.Seq[int], oidx parquet.OffsetIndex) error {
	if err := byteQuota.Reserve(totalBytes(pages, oidx)); err != nil {
		return fmt.Errorf("would use too many bytes: %w", err)
	}
	return nil
}

type pageEntryRead struct {
	pages []int
	rows  []rowRange
}

func partitionPageRanges(
	maxRangeSize uint64,
	maxGapSize uint64,
	pageIdx map[int][]rowRange,
	offset parquet.OffsetIndex,
) []pageEntryRead {
	partitioner := newGapBasedPartitioner(maxRangeSize, maxGapSize)
	if len(pageIdx) == 0 {
		return []pageEntryRead{}
	}
	idxs := make([]int, 0, len(pageIdx))
	for idx := range pageIdx {
		idxs = append(idxs, idx)
	}

	slices.Sort(idxs)

	parts := partitioner.partition(len(idxs), func(i int) (uint64, uint64) {
		return uint64(offset.Offset(idxs[i])), uint64(offset.Offset(idxs[i]) + offset.CompressedPageSize(idxs[i]))
	})

	r := make([]pageEntryRead, 0, len(parts))
	for _, part := range parts {
		pagesToRead := pageEntryRead{}
		for i := part.elemRng[0]; i < part.elemRng[1]; i++ {
			pagesToRead.pages = append(pagesToRead.pages, idxs[i])
			pagesToRead.rows = append(pagesToRead.rows, pageIdx[idxs[i]]...)
		}
		pagesToRead.rows = simplify(pagesToRead.rows)
		r = append(r, pagesToRead)
	}
	return r
}

type chunkValuesIterator struct {
	p parquet.Page

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func (vi *chunkValuesIterator) Reset(p parquet.Page) {
	vi.p = p
	vi.vr = p.Values()
	vi.buffer = make([]parquet.Value, 0, 128)
	vi.currentBufferIndex = -1
	vi.current = -1
}

func (vi *chunkValuesIterator) Next() bool {
	if vi.err != nil {
		return false
	}

	vi.current++
	if vi.current >= int(vi.p.NumRows()) {
		return false
	}

	vi.currentBufferIndex++

	if vi.currentBufferIndex == len(vi.buffer) {
		n, err := vi.vr.ReadValues(vi.buffer[:cap(vi.buffer)])
		if err != nil && err != io.EOF {
			vi.err = err
			return false
		}
		vi.buffer = vi.buffer[:n]
		vi.currentBufferIndex = 0
	}
	return true
}

func (vi *chunkValuesIterator) Error() error {
	return vi.err
}

func (vi *chunkValuesIterator) At() parquet.Value {
	return vi.buffer[vi.currentBufferIndex].Clone()
}

type bufferedReaderAt struct {
	r      io.ReaderAt
	b      []byte
	offset int64
}

func (b bufferedReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= b.offset && off < b.offset+int64(len(b.b)) {
		diff := off - b.offset
		n := copy(p, b.b[diff:])
		return n, nil
	}
	return b.r.ReadAt(p, off)
}

func newBufferedReaderAt(r io.ReaderAt, minOffset, maxOffset int64) io.ReaderAt {
	if minOffset < maxOffset {
		b := make([]byte, maxOffset-minOffset)
		n, err := r.ReadAt(b, minOffset)
		if err == nil {
			return &bufferedReaderAt{r: r, b: b[:n], offset: minOffset}
		}
	}
	return r
}
