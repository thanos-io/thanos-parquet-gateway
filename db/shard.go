// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"

	"github.com/thanos-io/thanos-parquet-gateway/internal/limits"
	"github.com/thanos-io/thanos-parquet-gateway/internal/tracing"
	"github.com/thanos-io/thanos-parquet-gateway/internal/warnings"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
	"github.com/thanos-io/thanos-parquet-gateway/search"
)

type Shard struct {
	meta        schema.Meta
	chunkspfile *parquet.File
	labelspfile *parquet.File

	chunkFileReaderFromCtx func(ctx context.Context) io.ReaderAt
}

func NewShard(
	meta schema.Meta,
	chunkspfile *parquet.File,
	labelspfile *parquet.File,
	chunkFileReaderCtxFunc func(ctx context.Context) io.ReaderAt,
) *Shard {
	return &Shard{
		meta:                   meta,
		chunkspfile:            chunkspfile,
		labelspfile:            labelspfile,
		chunkFileReaderFromCtx: chunkFileReaderCtxFunc,
	}
}

func (shd *Shard) Queryable(
	extlabels labels.Labels,
	replicaLabelNames []string,
	selectChunkBytesQuota *limits.Quota,
	selectRowCountQuota *limits.Quota,
	selectChunkPartitionMaxRange uint64,
	selectChunkPartitionMaxGap uint64,
	selectChunkPartitionMaxConcurrency int,
	selectHonorProjectionHints bool,
) *ShardQueryable {
	return &ShardQueryable{
		extlabels:                          extlabels,
		replicaLabelNames:                  replicaLabelNames,
		selectChunkBytesQuota:              selectChunkBytesQuota,
		selectRowCountQuota:                selectRowCountQuota,
		selectChunkPartitionMaxRange:       selectChunkPartitionMaxRange,
		selectChunkPartitionMaxGap:         selectChunkPartitionMaxGap,
		selectChunkPartitionMaxConcurrency: selectChunkPartitionMaxConcurrency,
		selectHonorProjectionHints:         selectHonorProjectionHints,
		shard:                              shd,
	}
}

type ShardQueryable struct {
	extlabels                          labels.Labels
	replicaLabelNames                  []string
	selectChunkBytesQuota              *limits.Quota
	selectRowCountQuota                *limits.Quota
	selectChunkPartitionMaxRange       uint64
	selectChunkPartitionMaxGap         uint64
	selectChunkPartitionMaxConcurrency int
	selectHonorProjectionHints         bool

	shard *Shard
}

func (q *ShardQueryable) Querier(mint, maxt int64) (*ShardQuerier, error) {
	return &ShardQuerier{
		mint:                               mint,
		maxt:                               maxt,
		shard:                              q.shard,
		extlabels:                          q.extlabels,
		replicaLabelNames:                  q.replicaLabelNames,
		selectChunkBytesQuota:              q.selectChunkBytesQuota,
		selectRowCountQuota:                q.selectRowCountQuota,
		selectChunkPartitionMaxRange:       q.selectChunkPartitionMaxRange,
		selectChunkPartitionMaxGap:         q.selectChunkPartitionMaxGap,
		selectChunkPartitionMaxConcurrency: q.selectChunkPartitionMaxConcurrency,
		selectHonorProjectionHints:         q.selectHonorProjectionHints,
	}, nil
}

type ShardQuerier struct {
	mint, maxt                         int64
	extlabels                          labels.Labels
	replicaLabelNames                  []string
	selectChunkBytesQuota              *limits.Quota
	selectRowCountQuota                *limits.Quota
	selectChunkPartitionMaxRange       uint64
	selectChunkPartitionMaxGap         uint64
	selectChunkPartitionMaxConcurrency int
	selectHonorProjectionHints         bool

	shard *Shard
}

var _ storage.Querier = &ShardQuerier{}

func (ShardQuerier) Close() error { return nil }

func (q ShardQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx, span := tracing.Tracer().Start(ctx, "Label Values Shard")
	defer span.End()

	span.SetAttributes(attribute.StringSlice("matchers", matchersToStringSlice(matchers)))
	span.SetAttributes(attribute.StringSlice("shard.replica_labels", q.replicaLabelNames))
	span.SetAttributes(attribute.String("shard.external_labels", q.extlabels.String()))

	queryableOperationsTotal.WithLabelValues(typeLabelValues, whereShard).Inc()

	start := time.Now()
	defer func() {
		queryableOperationsDuration.WithLabelValues(typeLabelValues, whereShard).Observe(float64(time.Since(start).Seconds()))
	}()

	labelValues, warns, err := search.LabelValues(
		ctx,
		search.LabelValuesReadMeta{
			Meta:              q.shard.meta,
			LabelPfile:        q.shard.labelspfile,
			ExternalLabels:    q.extlabels,
			ReplicaLabelNames: q.replicaLabelNames,
		},
		name,
		hints,
		matchers...)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to query label values: %w", err)
	}

	return labelValues, warns, nil
}

func (q ShardQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx, span := tracing.Tracer().Start(ctx, "Label Names Shard")
	defer span.End()

	span.SetAttributes(attribute.StringSlice("matchers", matchersToStringSlice(matchers)))
	span.SetAttributes(attribute.StringSlice("shard.replica_labels", q.replicaLabelNames))
	span.SetAttributes(attribute.String("shard.external_labels", q.extlabels.String()))

	queryableOperationsTotal.WithLabelValues(typeLabelNames, whereShard).Inc()

	start := time.Now()
	defer func() {
		queryableOperationsDuration.WithLabelValues(typeLabelNames, whereShard).Observe(float64(time.Since(start).Seconds()))
	}()

	labelNames, warns, err := search.LabelNames(
		ctx,
		search.LabelNamesReadMeta{
			Meta:              q.shard.meta,
			LabelPfile:        q.shard.labelspfile,
			ExternalLabels:    q.extlabels,
			ReplicaLabelNames: q.replicaLabelNames,
		},
		hints,
		matchers...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to query label names: %w", err)
	}

	return labelNames, warns, nil
}

func (q ShardQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return newLazySeriesSet(ctx, q.selectFn, sorted, hints, matchers...)
}

func matchersToStringSlice(matchers []*labels.Matcher) []string {
	res := make([]string, len(matchers))
	for i := range matchers {
		res[i] = matchers[i].String()
	}
	return res
}

func (q ShardQuerier) selectCore(ctx context.Context, spanName string, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) ([]search.SeriesChunks, annotations.Annotations, error) {
	ctx, span := tracing.Tracer().Start(ctx, spanName)
	defer span.End()

	span.SetAttributes(attribute.Bool("sorted", sorted))
	span.SetAttributes(attribute.StringSlice("matchers", matchersToStringSlice(matchers)))
	span.SetAttributes(attribute.StringSlice("shard.replica_labels", q.replicaLabelNames))
	span.SetAttributes(attribute.String("shard.external_labels", q.extlabels.String()))

	queryableOperationsTotal.WithLabelValues(typeSelect, whereShard).Inc()

	start := time.Now()
	defer func() {
		queryableOperationsDuration.WithLabelValues(typeSelect, whereShard).Observe(float64(time.Since(start).Seconds()))
	}()

	seriesChunks, warns, err := search.Select(
		ctx,
		search.SelectReadMeta{
			Meta:                             q.shard.meta,
			LabelPfile:                       q.shard.labelspfile,
			ChunkPfile:                       q.shard.chunkspfile,
			RowCountQuota:                    q.selectRowCountQuota,
			ChunkBytesQuota:                  q.selectChunkBytesQuota,
			ChunkPagePartitionMaxRange:       q.selectChunkPartitionMaxRange,
			ChunkPagePartitionMaxGap:         q.selectChunkPartitionMaxGap,
			ChunkPagePartitionMaxConcurrency: q.selectChunkPartitionMaxConcurrency,
			ChunkFileReaderFromContext:       q.shard.chunkFileReaderFromCtx,
			HonorProjectionHints:             q.selectHonorProjectionHints,
			ExternalLabels:                   q.extlabels,
			ReplicaLabelNames:                q.replicaLabelNames,
		},
		q.mint,
		q.maxt,
		hints,
		matchers...,
	)
	if err != nil {
		return nil, nil, err
	}

	skipChunks := hints.Func == "series"

	// Deduplicate series by label set hash
	seen := make(map[uint64]struct{})
	result := seriesChunks[:0]
	for i := range seriesChunks {
		if len(seriesChunks[i].Chunks) == 0 && !skipChunks {
			continue
		}
		h := seriesChunks[i].LsetHash
		if _, ok := seen[h]; ok {
			// We have seen this series before, skip it for now; we could be smarter and select
			// chunks appropriately so that we fill in what might be missing but for now skipping is fine
			warns = warns.Add(warnings.ErrorDroppedSeriesAfterExternalLabelMangling)
			continue
		}
		seen[h] = struct{}{}
		result = append(result, seriesChunks[i])
	}

	if sorted {
		slices.SortFunc(result, func(l, r search.SeriesChunks) int {
			return labels.Compare(l.Lset, r.Lset)
		})
	}
	return result, warns, nil
}

func (q ShardQuerier) selectFn(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	seriesChunks, warns, err := q.selectCore(ctx, "Select Shard", sorted, hints, matchers...)
	if err != nil {
		return storage.ErrSeriesSet(fmt.Errorf("unable to select: %w", err))
	}

	series := q.seriesFromSeriesChunks(seriesChunks)
	return newWarningsSeriesSet(newConcatSeriesSet(series...), warns)
}

func (q ShardQuerier) SelectChunks(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	return newLazyChunkSeriesSet(ctx, q.selectChunksFn, sorted, hints, matchers...)
}

func (q ShardQuerier) selectChunksFn(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	seriesChunks, warns, err := q.selectCore(ctx, "ChunkSelect Shard", sorted, hints, matchers...)
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}

	series := q.chunkSeriesFromSeriesChunks(seriesChunks)
	return newChunkSeriesSet(series, warns)
}

func (q ShardQuerier) chunkSeriesFromSeriesChunks(sc []search.SeriesChunks) []storage.ChunkSeries {
	res := make([]storage.ChunkSeries, 0, len(sc))
	for i := range sc {
		res = append(res, &storageChunkSeries{
			lset:   sc[i].Lset,
			chunks: sc[i].Chunks,
		})
	}
	return res
}

func (q ShardQuerier) seriesFromSeriesChunks(sc []search.SeriesChunks) []storage.Series {
	res := make([]storage.Series, 0, len(sc))
	for i := range sc {
		ss := &chunkSeries{
			lset:   sc[i].Lset,
			mint:   q.mint,
			maxt:   q.maxt,
			chunks: sc[i].Chunks,
		}
		res = append(res, ss)
	}
	return res
}
