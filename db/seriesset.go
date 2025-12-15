// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

type hints struct {
	Limit    int
	By       bool
	Func     string
	Grouping []string

	ProjectionLabels  []string
	ProjectionInclude bool
}

func fromStorageHints(h *storage.SelectHints) hints {
	grouping := make([]string, len(h.Grouping))
	copy(grouping, h.Grouping)

	projections := make([]string, len(h.ProjectionLabels))
	copy(projections, h.ProjectionLabels)

	return hints{
		Limit:             h.Limit,
		Func:              h.Func,
		By:                h.By,
		Grouping:          grouping,
		ProjectionLabels:  projections,
		ProjectionInclude: h.ProjectionInclude,
	}
}

func toStorageHints(h hints) *storage.SelectHints {
	return &storage.SelectHints{
		Limit:             h.Limit,
		Func:              h.Func,
		By:                h.By,
		Grouping:          h.Grouping,
		ProjectionLabels:  h.ProjectionLabels,
		ProjectionInclude: h.ProjectionInclude,
	}

}

type selectFn func(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet
type chunkSelectFn func(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.ChunkSeriesSet

func newLazySeriesSet(ctx context.Context, selectFn selectFn, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	res := &lazySeriesSet{
		selectfn: selectFn,
		ctx:      ctx,
		sorted:   sorted,
		// SelectHints is reused in the subsequent parallel Select call
		hints:    fromStorageHints(hints),
		matchers: matchers,
		done:     make(chan struct{}),
	}
	go res.init()

	return res
}

type lazySeriesSet struct {
	selectfn selectFn
	ctx      context.Context
	sorted   bool
	hints    hints
	matchers []*labels.Matcher

	set storage.SeriesSet

	once sync.Once
	done chan struct{}
}

func (c *lazySeriesSet) init() {
	c.once.Do(func() {
		c.set = c.selectfn(c.ctx, c.sorted, toStorageHints(c.hints), c.matchers...)
		close(c.done)
	})
}

func (c *lazySeriesSet) Next() bool {
	<-c.done
	return c.set.Next()
}

func (c *lazySeriesSet) Err() error {
	<-c.done
	return c.set.Err()
}

func (c *lazySeriesSet) At() storage.Series {
	<-c.done
	return c.set.At()
}

func (c *lazySeriesSet) Warnings() annotations.Annotations {
	<-c.done
	return c.set.Warnings()
}

type concatSeriesSet struct {
	i      int
	series []storage.Series
}

func newConcatSeriesSet(series ...storage.Series) storage.SeriesSet {
	if len(series) == 0 {
		return storage.EmptySeriesSet()
	}
	return &concatSeriesSet{series: series, i: -1}
}

func (ss *concatSeriesSet) Next() bool {
	if ss.i < len(ss.series)-1 {
		ss.i++
		return true
	}
	return false
}

func (ss *concatSeriesSet) At() storage.Series                { return ss.series[ss.i] }
func (ss *concatSeriesSet) Err() error                        { return nil }
func (ss *concatSeriesSet) Warnings() annotations.Annotations { return nil }

type warningsSeriesSet struct {
	storage.SeriesSet

	warns annotations.Annotations
}

func newWarningsSeriesSet(ss storage.SeriesSet, warns annotations.Annotations) storage.SeriesSet {
	return &warningsSeriesSet{SeriesSet: ss, warns: warns}
}

func (ss *warningsSeriesSet) Warnings() annotations.Annotations {
	original := ss.SeriesSet.Warnings()
	return original.Merge(ss.warns)
}

func newLazyChunkSeriesSet(ctx context.Context, selectFn chunkSelectFn, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	res := &lazyChunkSeriesSet{
		selectfn: selectFn,
		ctx:      ctx,
		sorted:   sorted,
		hints:    fromStorageHints(hints),
		matchers: matchers,
		done:     make(chan struct{}),
	}
	go res.init()

	return res
}

type lazyChunkSeriesSet struct {
	selectfn chunkSelectFn
	ctx      context.Context
	sorted   bool
	hints    hints
	matchers []*labels.Matcher

	set storage.ChunkSeriesSet

	once sync.Once
	done chan struct{}
}

func (c *lazyChunkSeriesSet) init() {
	c.once.Do(func() {
		c.set = c.selectfn(c.ctx, c.sorted, toStorageHints(c.hints), c.matchers...)
		close(c.done)
	})
}

func (c *lazyChunkSeriesSet) Next() bool {
	<-c.done
	return c.set.Next()
}

func (c *lazyChunkSeriesSet) Err() error {
	<-c.done
	return c.set.Err()
}

func (c *lazyChunkSeriesSet) At() storage.ChunkSeries {
	<-c.done
	return c.set.At()
}

func (c *lazyChunkSeriesSet) Warnings() annotations.Annotations {
	<-c.done
	return c.set.Warnings()
}

// chunkSeriesSet implements storage.ChunkSeriesSet.
type chunkSeriesSet struct {
	series []storage.ChunkSeries
	i      int
	warns  annotations.Annotations
}

func newChunkSeriesSet(series []storage.ChunkSeries, warns annotations.Annotations) storage.ChunkSeriesSet {
	return &chunkSeriesSet{series: series, i: -1, warns: warns}
}

func (ss *chunkSeriesSet) Next() bool {
	ss.i++
	return ss.i < len(ss.series)
}

func (ss *chunkSeriesSet) At() storage.ChunkSeries {
	return ss.series[ss.i]
}

func (ss *chunkSeriesSet) Err() error {
	return nil
}

func (ss *chunkSeriesSet) Warnings() annotations.Annotations {
	return ss.warns
}
