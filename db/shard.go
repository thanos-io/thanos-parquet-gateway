// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/cloudflare/parquet-tsdb-poc/internal/encoding"
	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
	"github.com/cloudflare/parquet-tsdb-poc/search"
)

type Shard struct {
	meta        Meta
	chunkspfile *parquet.File
	labelspfile *parquet.File
}

func (shd *Shard) Queryable(extlabels labels.Labels, replicaLabelNames []string) storage.Queryable {
	return &ShardQueryable{extlabels: extlabels, replicaLabelNames: replicaLabelNames, shard: shd}
}

type ShardQueryable struct {
	extlabels         labels.Labels
	replicaLabelNames []string

	shard *Shard
}

func (q *ShardQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &ShardQuerier{
		mint:              mint,
		maxt:              maxt,
		shard:             q.shard,
		extlabels:         q.extlabels,
		replicaLabelNames: q.replicaLabelNames,
	}, nil
}

type ShardQuerier struct {
	mint, maxt        int64
	extlabels         labels.Labels
	replicaLabelNames []string

	shard *Shard
}

var _ storage.Querier = &ShardQuerier{}

func (ShardQuerier) Close() error { return nil }

func (q ShardQuerier) LabelValues(_ context.Context, name string, _ *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if len(ms) != 0 {
		return nil, nil, errors.New("label values with label matchers is not supported")
	}

	if name != model.MetricNameLabel {
		return nil, nil, errors.New("label values for label names other then __name__ is not supported")
	}

	res := make([]string, 0, len(q.shard.meta.ColumnsForName))
	for name := range q.shard.meta.ColumnsForName {
		res = append(res, name)
	}

	return res, nil, nil
}

func (ShardQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// TODO
	return nil, nil, nil
}

func (q ShardQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return newLazySeriesSet(ctx, q.selectFn, sorted, hints, matchers...)
}

func (q ShardQuerier) selectFn(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	metricName, err := metricNameFromMatchers(matchers)
	if err != nil {
		return storage.ErrSeriesSet(fmt.Errorf("unable to get metric name from matchers: %s", err))
	}
	columnsForName := slices.Clone(q.shard.meta.ColumnsForName[metricName])

	constraint, err := constraintForMatchers(matchers, columnsForName)
	if err != nil {
		return storage.ErrSeriesSet(fmt.Errorf("unable to compute constraint for matchers: %s", err))
	}

	// we cannot do projections in presence of replica/external labels as its not clear how to resolve conflicts
	labelProjections := labelProjections(columnsForName)
	chunkProjections := chunkProjections(q.shard.meta.Mint, q.mint, q.maxt)

	// no need to fetch chunks for /api/v1/series
	if hints.Func == "series" {
		chunkProjections = nil
	}

	labelpfile := q.shard.labelspfile
	labelReadSchema := schema.Projection(labelpfile.Schema(), labelProjections)
	chunkspfile := q.shard.chunkspfile
	chunksReadSchema := schema.Projection(chunkspfile.Schema(), chunkProjections)

	rr, err := search.Match(ctx, constraint, labelpfile, labelReadSchema, chunkspfile, chunksReadSchema)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	defer rr.Close()

	ssb := &seriesSetBuilder{
		schema:            schema.Joined(labelReadSchema, chunksReadSchema),
		mint:              q.mint,
		maxt:              q.maxt,
		m:                 make(map[uint64]struct{}),
		s:                 make([]*chunkSeries, 0),
		b:                 labels.NewBuilder(labels.EmptyLabels()),
		extlabels:         q.extlabels,
		replicaLabelNames: q.replicaLabelNames,
	}

	if _, err := parquet.CopyRows(ssb, rr); err != nil {
		return storage.ErrSeriesSet(err)
	}
	series := ssb.Series()
	if sorted {
		slices.SortFunc(series, func(l, r storage.Series) int { return labels.Compare(l.Labels(), r.Labels()) })
	}
	return newConcatSeriesSet(series...)
}

func metricNameFromMatchers(matchers []*labels.Matcher) (string, error) {
	for i := range matchers {
		if matchers[i].Name == labels.MetricName {
			return matchers[i].Value, nil
		}
	}
	return "", errors.New("metric name is required")
}

func constraintForMatchers(matchers []*labels.Matcher, columnsForName []string) (search.Constraint, error) {
	constraints := make([]search.Constraint, 0)
	for i := range matchers {
		m := matchers[i]
		col := schema.LabelNameToColumn(m.Name)
		val := parquet.ValueOf(m.Value)
		if m.Name == labels.MetricName {
			if m.Type != labels.MatchEqual {
				return nil, errors.New("only equal matches on metric name are allowed")
			}
			constraints = append(constraints, search.EqualWithPageCheck(col, val))
			continue
		}
		validColumn := slices.Contains(columnsForName, col)
		var c search.Constraint
		switch m.Type {
		case labels.MatchEqual:
			if !validColumn {
				// equal match on a column that the series does not have; return nothing
				return search.Null(), nil
			}
			c = search.Equal(col, val)
		case labels.MatchNotEqual:
			if !validColumn {
				continue
			}
			c = search.Not(search.Equal(col, val))
		case labels.MatchRegexp:
			if !validColumn {
				// equal match on a column that the series does not have; return nothing
				return search.Null(), nil
			}
			var err error
			c, err = search.Regex(col, m.GetRegexString())
			if err != nil {
				return nil, fmt.Errorf("unable to build regex constraint: %s", err)
			}
		case labels.MatchNotRegexp:
			if !validColumn {
				continue
			}
			var err error
			c, err = search.Regex(col, m.GetRegexString())
			if err != nil {
				return nil, fmt.Errorf("unable to build regex constraint: %s", err)
			}
			c = search.Not(c)
		}
		constraints = append(constraints, c)
	}
	return search.And(constraints...), nil
}

func labelProjections(columnsForName []string) []string {
	return append(columnsForName, schema.LabelNameToColumn(model.MetricNameLabel))
}

func chunkProjections(blkStart, mint, maxt int64) []string {
	res := make([]string, 0)

	// TODO: This might be buggy since a block might start at the end of 0-8 and might
	//       leak into 8-16. Right now we would not catch this. This supposes that chunks
	//       are aligned to 8h.
	cmin, cmax := blkStart, blkStart+schema.ChunkColumnLength.Milliseconds()
	for i := 0; i != int(schema.ChunkColumnsPerDay); i++ {
		if !util.Intersects(cmin, cmax, mint, maxt) {
			goto C
		}
		switch i {
		case 0:
			res = append(res, schema.ChunksColumn0)
		case 1:
			res = append(res, schema.ChunksColumn1)
		case 2:
			res = append(res, schema.ChunksColumn2)
		}
	C:
		cmin, cmax = cmax, cmax+schema.ChunkColumnLength.Milliseconds()
	}
	return res
}

type seriesSetBuilder struct {
	schema            *parquet.Schema
	mint, maxt        int64
	extlabels         labels.Labels
	replicaLabelNames []string

	s []*chunkSeries
	b *labels.Builder
	m map[uint64]struct{}
}

func (ssb *seriesSetBuilder) WriteRows(rs []parquet.Row) (int, error) {
	var (
		chksBytes [schema.ChunkColumnsPerDay][]byte
	)
	cols := ssb.schema.Columns()
	for i := range rs {
		ssb.b.Reset(labels.EmptyLabels())
		rc := rs[i].Clone()
		for j := range rc {
			key := cols[j][0]
			val := rc[j]
			switch key {
			case schema.ChunksColumn0:
				chksBytes[0] = val.ByteArray()
			case schema.ChunksColumn1:
				chksBytes[1] = val.ByteArray()
			case schema.ChunksColumn2:
				chksBytes[2] = val.ByteArray()
			default:
				lblName := schema.ColumnToLabelName(key)
				if !val.IsNull() {
					ssb.b.Set(lblName, val.String())
				}
			}
		}
		chks := make([]chunkenc.Chunk, 0, 12)
		for _, bs := range chksBytes {
			for len(bs) != 0 {
				enc := chunkenc.Encoding(binary.BigEndian.Uint32(bs[:4]))
				bs = bs[4:]
				mint := encoding.ZigZagDecode(binary.BigEndian.Uint64(bs[:8]))
				bs = bs[8:]
				maxt := encoding.ZigZagDecode(binary.BigEndian.Uint64(bs[:8]))
				bs = bs[8:]
				l := binary.BigEndian.Uint32(bs[:4])
				bs = bs[4:]
				if util.Intersects(mint, maxt, ssb.mint, ssb.maxt) {
					chk, err := chunkenc.FromData(enc, bs[:l])
					if err != nil {
						return i, fmt.Errorf("unable to create chunk from data: %s", err)
					}
					chks = append(chks, chk)
				}
				bs = bs[l:]
			}
		}

		ssb.extlabels.Range(func(lbl labels.Label) { ssb.b.Set(lbl.Name, lbl.Value) })
		for _, lbl := range ssb.replicaLabelNames {
			ssb.b.Del(lbl)
		}

		lbls := ssb.b.Labels()

		h := lbls.Hash()
		if _, ok := ssb.m[h]; ok {
			// We have seen this series before, skip it for now; we could be smarter and select
			// chunks appropriately so that we fill in what might be missing but for now skipping is fine
			continue
		}
		ssb.m[h] = struct{}{}

		ssb.s = append(ssb.s, &chunkSeries{
			lset:   lbls,
			mint:   ssb.mint,
			maxt:   ssb.maxt,
			chunks: chks,
		})
	}
	return len(rs), nil
}

func (ssb *seriesSetBuilder) Series() []storage.Series {
	res := make([]storage.Series, 0, len(ssb.s))
	for _, v := range ssb.s {
		res = append(res, v)
	}
	return res
}
