// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/hashicorp/go-multierror"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
)

// DB is a horizontal partitioning of multiple non-overlapping blocks that are
// aligned to 24h and span exactely 24h.
type DB struct {
	syncer    syncer
	extLabels labels.Labels
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

func NewDB(syncer syncer, opts ...DBOption) *DB {
	cfg := dbConfig{extLabels: labels.EmptyLabels()}
	for _, o := range opts {
		o(&cfg)
	}
	return &DB{syncer: syncer, extLabels: cfg.extLabels}
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

func (db *DB) Extlabels() labels.Labels {
	return db.extLabels
}

// Queryable returns a storage.Queryable to evaluate queries with.
func (db *DB) Queryable() storage.Queryable {
	return &DBQueryable{
		blocks:    db.syncer.Blocks(),
		extLabels: db.extLabels,
	}
}

// ReplicaQueryable returns a storage.Queryable that drops replica labels at runtime. Replica labels are
// labels that identify a replica, i.e. one member of an HA pair of Prometheus servers. Thanos
// might request at query time to drop those labels so that we can deduplicate results into one view.
// Common replica labels are 'prometheus', 'host', etc.
func (db *DB) ReplicaQueryable(replicaLabelNames []string) storage.Queryable {
	return &DBQueryable{
		blocks:            db.syncer.Blocks(),
		extLabels:         db.extLabels,
		replicaLabelNames: replicaLabelNames,
	}
}

type DBQueryable struct {
	blocks []*Block

	// extLabels are added to all series in the result set overriding any internal labels.
	extLabels labels.Labels

	// replicaLabelNames are names of labels that identify replicas, they are dropped
	// after extLabels were applied.
	replicaLabelNames []string
}

func (db *DBQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	qs := make([]storage.Querier, 0, len(db.blocks))
	for _, blk := range db.blocks {
		bmint, bmaxt := blk.Timerange()
		if !util.Intersects(mint, maxt, bmint, bmaxt) {
			continue
		}
		start, end := util.Intersection(mint, maxt, bmint, bmaxt)
		q, err := blk.Queryable(db.extLabels, db.replicaLabelNames).Querier(start, end)
		if err != nil {
			return nil, fmt.Errorf("unable to get block querier: %s", err)
		}
		qs = append(qs, q)
	}
	return &DBQuerier{mint: mint, maxt: maxt, blocks: qs}, nil
}

type DBQuerier struct {
	mint, maxt int64

	blocks []storage.Querier
}

var _ storage.Querier = &DBQuerier{}

func (q DBQuerier) Close() error {
	var err *multierror.Error
	for _, q := range q.blocks {
		err = multierror.Append(err, q.Close())
	}
	return err.ErrorOrNil()
}

func (q DBQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	res := make([]string, 0)
	for _, blk := range q.blocks {
		lvals, lannos, err := blk.LabelValues(ctx, name, hints, ms...)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to query label values for block: %w", err)
		}
		annos = annos.Merge(lannos)
		res = append(res, lvals...)
	}

	slices.Sort(res)
	return slices.Compact(res), annos, nil
}

func (DBQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// TODO
	return nil, nil, nil
}

func (q DBQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return newLazySeriesSet(ctx, q.selectFn, sorted, hints, matchers...)
}

func (q DBQuerier) selectFn(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	// If we need to merge multiple series sets vertically we need them sorted
	sorted = sorted || len(q.blocks) > 1

	sss := make([]storage.SeriesSet, 0, len(q.blocks))
	for _, q := range q.blocks {
		sss = append(sss, q.Select(ctx, sorted, hints, matchers...))
	}

	if len(sss) == 0 {
		return storage.EmptySeriesSet()
	}
	return storage.NewMergeSeriesSet(sss, storage.ChainedSeriesMerge)
}
