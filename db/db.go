// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/alecthomas/units"
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

// DB is a horizontal partitioning of multiple non-overlapping blocks that are
// aligned to 24h and span exactly 24h.
type DB struct {
	syncer            syncer
	overrideExtLabels labels.Labels
	log               *slog.Logger
}

type syncer interface {
	Blocks() []*Block
}

type dbConfig struct {
	extLabels labels.Labels
}

type DBOption func(*dbConfig)

func ExternalLabels(extlabels labels.Labels) DBOption {
	return func(cfg *dbConfig) {
		cfg.extLabels = extlabels
	}
}

func NewDB(syncer syncer, logger *slog.Logger, opts ...DBOption) *DB {
	cfg := dbConfig{extLabels: labels.EmptyLabels()}
	for _, o := range opts {
		o(&cfg)
	}
	return &DB{syncer: syncer, overrideExtLabels: cfg.extLabels, log: logger}
}

func (db *DB) Timerange() (int64, int64) {
	blocks := db.syncer.Blocks()

	mint := int64(math.MaxInt64)
	maxt := int64(math.MinInt64)

	for _, blk := range blocks {
		bmint, bmaxt := blk.Timerange()
		mint = min(mint, bmint)
		maxt = max(maxt, bmaxt)
	}
	return mint, maxt
}

type BlockInfo struct {
	MinT, MaxT int64
	Labels     schema.ExternalLabels
}

func (db *DB) BlockStreams() map[schema.ExternalLabelsHash]BlockInfo {
	blocks := db.syncer.Blocks()

	blockStreams := make(map[schema.ExternalLabelsHash]BlockInfo)
	for _, blk := range blocks {
		blkMint, blkMaxt := blk.Timerange()

		st, ok := blockStreams[blk.ExternalLabels().Hash()]
		if !ok {
			st = BlockInfo{
				MinT:   blkMint,
				MaxT:   blkMaxt,
				Labels: blk.ExternalLabels(),
			}
		} else {
			st.MinT = min(st.MinT, blkMint)
			st.MaxT = max(st.MaxT, blkMaxt)
		}
		blockStreams[blk.ExternalLabels().Hash()] = st
	}

	return blockStreams
}

func (db *DB) OverrideExtLabels() labels.Labels {
	return db.overrideExtLabels
}

type queryableConfig struct {
	replicaLabelsNames    []string
	selectChunkBytesQuota *limits.Quota
	selectRowCountQuota   *limits.Quota

	selectChunkPartitionMaxRange       uint64
	selectChunkPartitionMaxGap         uint64
	selectChunkPartitionMaxConcurrency int

	labelValuesRowCountQuota *limits.Quota
	labelNamesRowCountQuota  *limits.Quota

	shardCountQuota *limits.Quota
}

type QueryableOption func(*queryableConfig)

func DropReplicaLabels(s ...string) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.replicaLabelsNames = append(cfg.replicaLabelsNames, s...)
	}
}

func SelectChunkBytesQuota(maxBytes units.Base2Bytes) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectChunkBytesQuota = limits.NewQuota(int64(maxBytes))
	}
}

func SelectRowCountQuota(maxRows int64) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectRowCountQuota = limits.NewQuota(int64(maxRows))
	}
}

func SelectChunkPartitionMaxRange(maxRange units.Base2Bytes) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectChunkPartitionMaxRange = uint64(maxRange)
	}
}

func SelectChunkPartitionMaxGap(maxGap units.Base2Bytes) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectChunkPartitionMaxGap = uint64(maxGap)
	}
}

func SelectChunkPartitionMaxConcurrency(n int) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.selectChunkPartitionMaxConcurrency = n
	}
}

func LabelValuesRowCountQuota(maxRows int64) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.labelValuesRowCountQuota = limits.NewQuota(maxRows)
	}
}

func LabelNamesRowCountQuota(maxRows int64) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.labelNamesRowCountQuota = limits.NewQuota(maxRows)
	}
}

func ShardCountQuota(maxShards int64) QueryableOption {
	return func(cfg *queryableConfig) {
		cfg.shardCountQuota = limits.NewQuota(maxShards)
	}
}

// Queryable returns a DBQueryable (which implements both storage.Queryable and storage.ChunkQueryable)
// that drops replica labels at runtime. Replica labels are labels that identify a replica,
// i.e. one member of an HA pair of Prometheus servers. Thanos might request at query time
// to drop those labels so that we can deduplicate results into one view.
// Common replica labels are 'prometheus', 'host', etc.
// It also enforces various quotas over its lifetime.
func (db *DB) Queryable(opts ...QueryableOption) *DBQueryable {
	cfg := queryableConfig{
		selectChunkBytesQuota:              limits.UnlimitedQuota(),
		selectRowCountQuota:                limits.UnlimitedQuota(),
		selectChunkPartitionMaxRange:       math.MaxUint64,
		selectChunkPartitionMaxGap:         math.MaxUint64,
		selectChunkPartitionMaxConcurrency: 0,
		labelValuesRowCountQuota:           limits.UnlimitedQuota(),
		labelNamesRowCountQuota:            limits.UnlimitedQuota(),
		shardCountQuota:                    limits.UnlimitedQuota(),
	}
	for i := range opts {
		opts[i](&cfg)
	}

	return &DBQueryable{
		blocks:                             db.syncer.Blocks(),
		log:                                db.log,
		extLabels:                          db.overrideExtLabels,
		replicaLabelNames:                  cfg.replicaLabelsNames,
		selectChunkBytesQuota:              cfg.selectChunkBytesQuota,
		selectRowCountQuota:                cfg.selectRowCountQuota,
		selectChunkPartitionMaxRange:       cfg.selectChunkPartitionMaxRange,
		selectChunkPartitionMaxGap:         cfg.selectChunkPartitionMaxGap,
		selectChunkPartitionMaxConcurrency: cfg.selectChunkPartitionMaxConcurrency,
		labelValuesRowCountQuota:           cfg.labelValuesRowCountQuota,
		labelNamesRowCountQuota:            cfg.labelNamesRowCountQuota,
		shardCountQuota:                    cfg.shardCountQuota,
	}
}

type DBQueryable struct {
	blocks []*Block
	log    *slog.Logger

	extLabels labels.Labels

	// replicaLabelNames are names of labels that identify replicas, they are dropped
	// after extLabels were applied.
	replicaLabelNames []string

	// selectChunkBytesQuota is the limit of bytes that "Select" calls can fetch from chunk columns.
	selectChunkBytesQuota *limits.Quota

	// selectRowCountQuota is the limit of rows that "Select" calls can touch.
	selectRowCountQuota *limits.Quota

	// selectChunkPartitionMaxRange is the maximum range of chunk pages that get coalesced into a
	// range that is concurrently scheduled to be fetched from object storage.
	selectChunkPartitionMaxRange uint64

	// selectChunkPartitionMaxGap is the maximum gap that we tolerate when coalescing nearby pages into ranges.
	selectChunkPartitionMaxGap uint64

	// selectChunkPartitionMaxConcurrency is the maximum amount of parallel object storage requests we run per select.
	selectChunkPartitionMaxConcurrency int

	// labelValuesRowCountQuota is the limit of rows that "LabelValues" calls can touch.
	labelValuesRowCountQuota *limits.Quota

	// labelNamesRowCountQuota is the limit of rows that "LabelNames" calls can touch.
	labelNamesRowCountQuota *limits.Quota

	// shardCountQuota is the limit of shards that any query can touch
	shardCountQuota *limits.Quota
}

func (db *DBQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	bs := make([]*Block, 0, len(db.blocks))

	for _, blk := range db.blocks {
		bmint, bmaxt := blk.Timerange()
		if !util.Intersects(mint, maxt, bmint, bmaxt) {
			continue
		}
		bs = append(bs, blk)
	}

	// Filter out partition blocks if a daily block exists for the same date.
	// This prevents duplicate data when both partitions and daily blocks exist
	// during the transition period (after daily conversion, before partition cleanup).
	bs = filterOverlappingPartitions(bs, db.log)

	// We can honor projections if all blocks that participate in the query
	// are at least V2. We introduced a labels-hash column in V2 which is
	// required to be able to still horizontally join series.
	honorProjections := !slices.ContainsFunc(bs, func(blk *Block) bool {
		return blk.Meta().Version < schema.V2
	})

	qs := make([]*BlockQuerier, 0, len(db.blocks))
	for _, blk := range bs {
		if err := db.shardCountQuota.Reserve(int64(len(blk.shards))); err != nil {
			return nil, fmt.Errorf("would use too many shards: %w", err)
		}
		bmint, bmaxt := blk.Timerange()

		start, end := util.Intersection(mint, maxt, bmint, bmaxt)
		q, err := blk.Queryable(
			db.extLabels,
			db.replicaLabelNames,
			db.selectChunkBytesQuota,
			db.selectRowCountQuota,
			db.selectChunkPartitionMaxRange,
			db.selectChunkPartitionMaxGap,
			db.selectChunkPartitionMaxConcurrency,
			honorProjections,
			db.labelValuesRowCountQuota,
			db.labelNamesRowCountQuota,
		).Querier(start, end)
		if err != nil {
			return nil, fmt.Errorf("unable to get block querier: %w", err)
		}
		qs = append(qs, q.(*BlockQuerier))
	}
	return &DBQuerier{mint: mint, maxt: maxt, blocks: qs}, nil
}

type DBQuerier struct {
	mint, maxt int64

	blocks []*BlockQuerier
}

var _ storage.Querier = &DBQuerier{}

func (q DBQuerier) Close() error {
	errs := make([]error, 0)
	for i, q := range q.blocks {
		if err := q.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unable to close block %q: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (q DBQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)

	res := make([]string, 0)
	for _, blk := range q.blocks {
		g.Go(func() error {
			lvals, lannos, err := blk.LabelValues(ctx, name, hints, matchers...)
			if err != nil {
				return fmt.Errorf("unable to query label values for block: %w", err)
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

func (q DBQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)

	res := make([]string, 0)
	for _, blk := range q.blocks {
		g.Go(func() error {
			lnames, lannos, err := blk.LabelNames(ctx, hints, matchers...)
			if err != nil {
				return fmt.Errorf("unable to query label names for block: %w", err)
			}
			annos = annos.Merge(lannos)
			mu.Lock()
			res = append(res, lnames...)
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, fmt.Errorf("unable to query label names: %w", err)
	}

	limit := hints.Limit

	res = util.SortUnique(res)
	if limit > 0 && len(res) > limit {
		res = res[:limit]
		annos = annos.Add(warnings.ErrorTruncatedResponse)
	}
	return res, annos, nil
}

func (q DBQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return newLazySeriesSet(ctx, q.selectFn, sorted, hints, matchers...)
}

func (q DBQuerier) selectFn(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	ctx, span := tracing.Tracer().Start(ctx, "Select DB")
	defer span.End()

	span.SetAttributes(attribute.Bool("sorted", sorted))
	span.SetAttributes(attribute.StringSlice("matchers", matchersToStringSlice(matchers)))
	span.SetAttributes(attribute.Int("block.shards", len(q.blocks)))

	// If we need to merge multiple series sets vertically we need them sorted
	sorted = sorted || len(q.blocks) > 1

	sss := make([]storage.SeriesSet, 0, len(q.blocks))
	for _, blk := range q.blocks {
		sss = append(sss, blk.Select(ctx, sorted, hints, matchers...))
	}

	if len(sss) == 0 {
		return storage.EmptySeriesSet()
	}
	return storage.NewMergeSeriesSet(sss, hints.Limit, storage.ChainedSeriesMerge)
}

func (db *DBQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := db.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &DBChunkQuerier{DBQuerier: q.(*DBQuerier)}, nil
}

type DBChunkQuerier struct {
	*DBQuerier
}

var _ storage.ChunkQuerier = &DBChunkQuerier{}

func (q *DBChunkQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	return newLazyChunkSeriesSet(ctx, q.selectChunksFn, sorted, hints, matchers...)
}

func (q *DBChunkQuerier) selectChunksFn(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	ctx, span := tracing.Tracer().Start(ctx, "ChunkSelect DB")
	defer span.End()

	span.SetAttributes(attribute.Bool("sorted", sorted))
	span.SetAttributes(attribute.StringSlice("matchers", matchersToStringSlice(matchers)))
	span.SetAttributes(attribute.Int("block.shards", len(q.blocks)))

	// If we need to merge multiple series sets vertically we need them sorted
	sorted = sorted || len(q.blocks) > 1

	sss := make([]storage.ChunkSeriesSet, 0, len(q.blocks))
	for _, blk := range q.blocks {
		bcq := &BlockChunkQuerier{*blk}
		sss = append(sss, bcq.Select(ctx, sorted, hints, matchers...))
	}

	if len(sss) == 0 {
		return storage.EmptyChunkSeriesSet()
	}
	if len(sss) == 1 {
		return sss[0]
	}
	return storage.NewMergeChunkSeriesSet(sss, hints.Limit, storage.NewConcatenatingChunkSeriesMerger())
}

// filterOverlappingPartitions removes partition blocks if a daily block exists for the same date.
// This ensures we prefer daily blocks over partitions to avoid reading duplicate data.
//
// Example: If we have blocks [2025/12/02, 2025/12/02/parts/00-02, 2025/12/02/parts/02-04],
// only 2025/12/02 is kept because it covers the full day.
func filterOverlappingPartitions(blocks []*Block, log *slog.Logger) []*Block {
	// Build set of daily block dates
	dailyBlocks := make(map[string]struct{})
	for _, blk := range blocks {
		name := blk.Meta().Name
		if !schema.IsPartition(name) {
			dailyBlocks[name] = struct{}{}
		}
	}

	// If no daily blocks, return all blocks unchanged
	if len(dailyBlocks) == 0 {
		return blocks
	}

	// Filter out partitions that have a corresponding daily block
	result := make([]*Block, 0, len(blocks))
	for _, blk := range blocks {
		name := blk.Meta().Name
		if schema.IsPartition(name) {
			dailyName := schema.DailyBlockNameForPartition(name)
			if _, hasDailyBlock := dailyBlocks[dailyName]; hasDailyBlock {
				log.Debug("Skipping partition block, daily block exists",
					slog.String("partition", name),
					slog.String("daily", dailyName),
				)
				continue
			}
		}
		result = append(result, blk)
	}

	return result
}

// blockTypeFromTimeRange determines the block type based on its time range.
// Daily blocks span 24 hours, partition blocks span less.
func blockTypeFromTimeRange(mint, maxt int64) string {
	duration := time.Duration(maxt-mint) * time.Millisecond
	switch {
	case duration >= 24*time.Hour:
		return "daily"
	case duration >= 8*time.Hour:
		return "partition"
	default:
		return "unknown"
	}
}
