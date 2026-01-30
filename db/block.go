// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos-parquet-gateway/internal/limits"
	"github.com/thanos-io/thanos-parquet-gateway/internal/tracing"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/internal/warnings"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type Block struct {
	meta          schema.Meta
	shards        []*Shard
	extLabels     schema.ExternalLabels
	extPromLabels labels.Labels
}

func NewBlock(meta schema.Meta, extLabels schema.ExternalLabels, shards ...*Shard) *Block {
	return &Block{meta: meta, extLabels: extLabels, shards: shards, extPromLabels: labels.FromMap(extLabels)}
}

func (blk *Block) ExternalLabels() schema.ExternalLabels {
	return blk.extLabels
}

func (blk *Block) ExternalPromLabels() labels.Labels {
	return blk.extPromLabels
}

func (blk *Block) Meta() schema.Meta {
	return blk.meta
}

func (blk *Block) Timerange() (int64, int64) {
	return blk.meta.Mint, blk.meta.Maxt
}

func (blk *Block) Queryable(
	overrideExtLabels labels.Labels,
	replicaLabelNames []string,
	selectChunkBytesQuota *limits.Quota,
	selectRowCountQuota *limits.Quota,
	selectChunkPartitionMaxRange uint64,
	selectChunkPartitionMaxGap uint64,
	selectChunkPartitionMaxConcurrency int,
	selectHonorProjectionHints bool,
	labelValuesRowCountQuota *limits.Quota,
	labelNamesRowCountQuota *limits.Quota,
) *BlockQueryable {
	qs := make([]*ShardQueryable, 0, len(blk.shards))
	var extLabels = blk.ExternalPromLabels()
	if overrideExtLabels.Len() > 0 {
		extLabels = overrideExtLabels
	}
	for _, shard := range blk.shards {
		qs = append(qs, shard.Queryable(
			extLabels,
			replicaLabelNames,
			selectChunkBytesQuota,
			selectRowCountQuota,
			selectChunkPartitionMaxRange,
			selectChunkPartitionMaxGap,
			selectChunkPartitionMaxConcurrency,
			selectHonorProjectionHints,
			labelValuesRowCountQuota,
			labelNamesRowCountQuota,
		))
	}
	return &BlockQueryable{shards: qs}
}

type BlockQueryable struct {
	shards []*ShardQueryable
}

func (q *BlockQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	qs := make([]*ShardQuerier, 0, len(q.shards))
	for _, shard := range q.shards {
		sq, err := shard.Querier(mint, maxt)
		if err != nil {
			return nil, fmt.Errorf("unable to get shard querier: %w", err)
		}
		qs = append(qs, sq)
	}
	return &BlockQuerier{mint: mint, maxt: maxt, shards: qs}, nil
}

func (q *BlockQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	qs := make([]*ShardQuerier, 0, len(q.shards))
	for _, shard := range q.shards {
		sq, err := shard.Querier(mint, maxt)
		if err != nil {
			return nil, fmt.Errorf("unable to get shard querier: %w", err)
		}
		qs = append(qs, sq)
	}
	return &BlockChunkQuerier{BlockQuerier{mint: mint, maxt: maxt, shards: qs}}, nil
}

type BlockQuerier struct {
	mint, maxt int64
	shards     []*ShardQuerier
}

func (q BlockQuerier) Close() error {
	errs := make([]error, 0)
	for i, q := range q.shards {
		if err := q.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close shard %q: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (q BlockQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)

	res := make([]string, 0)
	for _, s := range q.shards {
		g.Go(func() error {
			lvals, lannos, err := s.LabelValues(ctx, name, hints, matchers...)
			if err != nil {
				return fmt.Errorf("unable to query label values for shard: %w", err)
			}
			annos = annos.Merge(lannos)
			mu.Lock()
			res = append(res, lvals...)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, fmt.Errorf("unable to query label values: %w", err)
	}

	limit := hints.Limit

	res = util.SortUnique(res)
	if limit > 0 && len(res) > limit {
		res = res[:limit]
		annos = annos.Add(warnings.ErrorTruncatedResponse)
	}
	return res, annos, nil
}

func (q BlockQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)

	res := make([]string, 0)
	for _, s := range q.shards {
		g.Go(func() error {
			lvals, lannos, err := s.LabelNames(ctx, hints, matchers...)
			if err != nil {
				return fmt.Errorf("unable to query label names for shard: %w", err)
			}
			annos = annos.Merge(lannos)
			mu.Lock()
			res = append(res, lvals...)
			mu.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, fmt.Errorf("unable to query label values: %w", err)
	}

	limit := hints.Limit

	res = util.SortUnique(res)
	if limit > 0 && len(res) > limit {
		res = res[:limit]
		annos = annos.Add(warnings.ErrorTruncatedResponse)
	}
	return res, annos, nil
}

func (q BlockQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return newLazySeriesSet(ctx, q.selectFn, sorted, hints, matchers...)
}

func (q BlockQuerier) selectFn(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	ctx, span := tracing.Tracer().Start(ctx, "Select Block")
	defer span.End()

	span.SetAttributes(attribute.Bool("sorted", true))
	span.SetAttributes(attribute.StringSlice("matchers", matchersToStringSlice(matchers)))
	span.SetAttributes(attribute.Int("block.shards", len(q.shards)))
	span.SetAttributes(attribute.String("block.mint", time.UnixMilli(q.mint).String()))
	span.SetAttributes(attribute.String("block.maxt", time.UnixMilli(q.maxt).String()))

	sss := make([]storage.SeriesSet, 0, len(q.shards))
	for _, s := range q.shards {
		// always sort since we need to merge later anyhow
		sss = append(sss, s.Select(ctx, true, hints, matchers...))
	}
	if len(sss) == 0 {
		return storage.EmptySeriesSet()
	}
	return storage.NewMergeSeriesSet(sss, hints.Limit, storage.ChainedSeriesMerge)
}

type BlockChunkQuerier struct {
	BlockQuerier
}

var _ storage.ChunkQuerier = &BlockChunkQuerier{}

func (q *BlockChunkQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	return newLazyChunkSeriesSet(ctx, q.selectChunksFn, sorted, hints, matchers...)
}

func (q *BlockChunkQuerier) selectChunksFn(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	ctx, span := tracing.Tracer().Start(ctx, "ChunkSelect Block")
	defer span.End()

	span.SetAttributes(attribute.Bool("sorted", true))
	span.SetAttributes(attribute.StringSlice("matchers", matchersToStringSlice(matchers)))
	span.SetAttributes(attribute.Int("block.shards", len(q.shards)))

	sss := make([]storage.ChunkSeriesSet, 0, len(q.shards))
	for _, s := range q.shards {
		// always sort since we need to merge later anyhow
		sss = append(sss, s.SelectChunks(ctx, true, hints, matchers...))
	}
	if len(sss) == 0 {
		return storage.EmptyChunkSeriesSet()
	}
	if len(sss) == 1 {
		return sss[0]
	}
	return storage.NewMergeChunkSeriesSet(sss, hints.Limit, storage.NewConcatenatingChunkSeriesMerger())
}
