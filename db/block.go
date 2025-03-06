// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"fmt"
	"slices"

	"github.com/hashicorp/go-multierror"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

type Block struct {
	meta   Meta
	shards []*Shard
}

type Meta struct {
	Name           string
	Mint, Maxt     int64
	Shards         int64
	ColumnsForName map[string][]string
}

func (blk *Block) Timerange() (int64, int64) {
	return blk.meta.Mint, blk.meta.Maxt
}

func (blk *Block) Queryable(extlabels labels.Labels, replicaLabelNames []string) storage.Queryable {
	qs := make([]storage.Queryable, 0, len(blk.shards))
	for _, shard := range blk.shards {
		qs = append(qs, shard.Queryable(extlabels, replicaLabelNames))
	}
	return &BlockQueryable{extlabels: extlabels, shards: qs}
}

type BlockQueryable struct {
	extlabels labels.Labels

	shards []storage.Queryable
}

func (q *BlockQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	qs := make([]storage.Querier, 0, len(q.shards))
	for _, shard := range q.shards {
		q, err := shard.Querier(mint, maxt)
		if err != nil {
			return nil, fmt.Errorf("unable to get shard querier: %w", err)
		}
		qs = append(qs, q)
	}
	return &BlockQuerier{mint: mint, maxt: maxt, shards: qs}, nil
}

type BlockQuerier struct {
	mint, maxt int64

	shards []storage.Querier
}

func (q BlockQuerier) Close() error {
	var err *multierror.Error
	for _, q := range q.shards {
		err = multierror.Append(err, q.Close())
	}
	return err.ErrorOrNil()
}

func (q BlockQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var annos annotations.Annotations

	res := make([]string, 0)
	for _, shrd := range q.shards {
		lvals, lannos, err := shrd.LabelValues(ctx, name, hints, ms...)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to query label values for shard: %w", err)
		}
		annos = annos.Merge(lannos)
		res = append(res, lvals...)
	}

	slices.Sort(res)
	return slices.Compact(res), annos, nil
}

func (BlockQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// TODO
	return nil, nil, nil
}

func (q BlockQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	sss := make([]storage.SeriesSet, 0, len(q.shards))
	for _, q := range q.shards {
		sss = append(sss, q.Select(ctx, sorted, hints, matchers...))
	}
	return newVerticalSeriesSet(sss...)
}
