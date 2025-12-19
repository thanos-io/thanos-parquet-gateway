// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos-parquet-gateway/internal/tracing"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type Constraint interface {
	// init initializes the constraint with respect to the file schema and projections.
	init(s *parquet.Schema) error

	// path is the path for the column that is constrained.
	path() string

	// prefilter returns a set of non-overlapping increasing row indexes that may satisfy the constraint.
	// This MUST be a superset of the real set of matching rows.
	prefilter(f *schema.FileWithReader, rgi int, rr []rowRange) ([]rowRange, error)

	// filter returns a set of non-overlapping increasing row indexes that do satisfy the constraint.
	// This MUST be the precise set of matching rows.
	filter(ctx context.Context, f *schema.FileWithReader, rgi int, rr []rowRange) ([]rowRange, error)
}

func matchersToConstraint(matchers ...*labels.Matcher) ([]Constraint, error) {
	r := make([]Constraint, 0, len(matchers))
L:
	for _, matcher := range matchers {
		var c Constraint
	S:
		switch matcher.Type {
		case labels.MatchEqual:
			c = Equal(schema.LabelNameToColumn(matcher.Name), parquet.ValueOf(matcher.Value))
		case labels.MatchNotEqual:
			c = Not(Equal(schema.LabelNameToColumn(matcher.Name), parquet.ValueOf(matcher.Value)))
		case labels.MatchRegexp:
			if matcher.GetRegexString() == ".*" {
				continue L
			}
			if matcher.GetRegexString() == ".+" {
				c = Not(Equal(schema.LabelNameToColumn(matcher.Name), parquet.ValueOf("")))
				break S
			}
			if set := matcher.SetMatches(); len(set) == 1 {
				c = Equal(schema.LabelNameToColumn(matcher.Name), parquet.ValueOf(set[0]))
				break S
			}
			rc, err := Regex(schema.LabelNameToColumn(matcher.Name), matcher)
			if err != nil {
				return nil, fmt.Errorf("unable to construct regex matcher: %w", err)
			}
			c = rc
		case labels.MatchNotRegexp:
			inverted, err := matcher.Inverse()
			if err != nil {
				return nil, fmt.Errorf("unable to invert matcher: %w", err)
			}
			if set := inverted.SetMatches(); len(set) == 1 {
				c = Not(Equal(schema.LabelNameToColumn(matcher.Name), parquet.ValueOf(set[0])))
				break S
			}
			rc, err := Regex(schema.LabelNameToColumn(matcher.Name), inverted)
			if err != nil {
				return nil, fmt.Errorf("unable to construct regex matcher: %w", err)
			}
			c = Not(rc)
		default:
			return nil, fmt.Errorf("unsupported matcher type %s", matcher.Type)
		}
		r = append(r, c)
	}
	return r, nil
}

func initialize(s *parquet.Schema, cs ...Constraint) error {
	for i := range cs {
		if err := cs[i].init(s); err != nil {
			return fmt.Errorf("unable to initialize constraint %d: %w", i, err)
		}
	}
	return nil
}

func filter(ctx context.Context, f *schema.FileWithReader, rgi int, cs ...Constraint) ([]rowRange, error) {
	if rgi >= len(f.File().RowGroups()) {
		return nil, fmt.Errorf("unable to filter row group %d: row group does not exist", rgi)
	}
	rg := f.File().RowGroups()[rgi]

	// Constraints for sorting columns are cheaper to evaluate, so we sort them first.
	sortConstraintsBySortingColumns(cs, rg.SortingColumns())

	var (
		err error
		mu  sync.Mutex
		g   errgroup.Group
	)

	// First pass prefilter with a quick index scan to find a superset of matching rows
	rr := []rowRange{{from: int64(0), count: rg.NumRows()}}
	for i := range cs {
		rr, err = cs[i].prefilter(f, rgi, rr)
		if err != nil {
			return nil, fmt.Errorf("unable to prefilter with constraint %d: %w", i, err)
		}
	}
	res := slices.Clone(rr)

	if len(res) == 0 {
		return nil, nil
	}

	// Second pass page filter find the real set of matching rows, done concurrently because it involves IO
	for i := range cs {
		g.Go(func() error {
			srr, err := cs[i].filter(ctx, f, rgi, rr)
			if err != nil {
				return fmt.Errorf("unable to filter with constraint %d: %w", i, err)
			}
			mu.Lock()
			res = intersectRowRanges(res, srr)
			mu.Unlock()

			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to do second pass filter: %w", err)
	}

	return res, nil
}

func sortConstraintsBySortingColumns(cs []Constraint, sc []parquet.SortingColumn) {
	if len(sc) == 0 {
		return
	}

	sortingPaths := make(map[string]int, len(sc))
	for i, col := range sc {
		sortingPaths[col.Path()[0]] = i
	}

	slices.SortStableFunc(cs, func(a, b Constraint) int {
		aIdx, aIsSorting := sortingPaths[a.path()]
		bIdx, bIsSorting := sortingPaths[b.path()]

		if aIsSorting && bIsSorting {
			return aIdx - bIdx
		}
		if aIsSorting {
			return -1
		}
		if bIsSorting {
			return 1
		}
		return 0
	})
}

// TODO: equal/regex prefilter and filter are VERY similar, we should refactor this eventually

type pageToRead struct {
	idx   int
	pfrom int64
	pto   int64
}

const valueBufferSize = 1024

var valueBufferPool = sync.Pool{
	New: func() any {
		buf := make([]parquet.Value, valueBufferSize)
		return &buf
	},
}

func getValueBuffer() []parquet.Value {
	return *valueBufferPool.Get().(*[]parquet.Value)
}

func putValueBuffer(buf []parquet.Value) {
	valueBufferPool.Put(&buf)
}

type equalConstraint struct {
	pth string

	val          parquet.Value
	matchesEmpty bool

	comp func(l, r parquet.Value) int
}

func Equal(path string, value parquet.Value) Constraint {
	return &equalConstraint{pth: path, val: value}
}

func (ec *equalConstraint) prefilter(f *schema.FileWithReader, rgi int, rr []rowRange) ([]rowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rg := f.File().RowGroups()[rgi]

	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	col, ok := rg.Schema().Lookup(ec.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if ec.matchesEmpty {
			return slices.Clone(rr), nil
		}
		return []rowRange{}, nil
	}
	cci := col.ColumnIndex

	cc := rg.ColumnChunks()[cci].(*parquet.FileColumnChunk)

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	res := make([]rowRange, 0)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if ec.matchesEmpty {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}

		// If we are not matching the empty string (which would be satisfied by Null too), we can
		// use page statistics to skip rows
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		// If page is constant (min == max) and has no nulls, we can decide without reading
		if ec.comp(minv, maxv) == 0 && cidx.NullCount(i) == 0 {
			if ec.comp(ec.val, minv) == 0 {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}
		if !ec.matchesEmpty && ec.comp(ec.val, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.matchesEmpty && ec.comp(ec.val, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		res = append(res, rowRange{from: pfrom, count: pto - pfrom})
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (ec *equalConstraint) filter(ctx context.Context, f *schema.FileWithReader, rgi int, rr []rowRange) ([]rowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	ctx, span := tracing.Tracer().Start(ctx, "Filter Constraint")
	defer span.End()

	span.SetAttributes(
		attribute.String("type", "equal"),
		attribute.String("path", ec.path()),
		attribute.String("value", ec.val.String()),
		attribute.Int("row_group", rgi),
	)

	rg := f.File().RowGroups()[rgi]
	method := methodFromContext(ctx)

	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	col, ok := rg.Schema().Lookup(ec.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if ec.matchesEmpty {
			return slices.Clone(rr), nil
		}
		return []rowRange{}, nil
	}
	cci := col.ColumnIndex

	cc := rg.ColumnChunks()[cci].(*parquet.FileColumnChunk)

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		res     = make([]rowRange, 0)
		readPgs = make([]pageToRead, 0)
	)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if ec.matchesEmpty {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}

		// If we are not matching the empty string (which would be satisfied by Null too), we can
		// use page statistics to skip rows
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		// If page is constant (min == max) and has no nulls, we can decide without reading
		if ec.comp(minv, maxv) == 0 && cidx.NullCount(i) == 0 {
			if ec.comp(ec.val, minv) == 0 {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}
		if !ec.matchesEmpty && ec.comp(ec.val, maxv) > 0 {
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.matchesEmpty && ec.comp(ec.val, minv) < 0 {
			if cidx.IsAscending() {
				break
			}
			continue
		}
		// We cannot discard the page through statistics but we might need to read it to see if it has the value
		readPgs = append(readPgs, pageToRead{idx: i, pfrom: pfrom, pto: pto})
	}

	if len(readPgs) == 0 {
		return intersectRowRanges(simplify(res), rr), nil
	}

	minOffset := uint64(oidx.Offset(readPgs[0].idx))
	maxOffset := uint64(oidx.Offset(readPgs[len(readPgs)-1].idx)) + uint64(oidx.CompressedPageSize(readPgs[len(readPgs)-1].idx))

	// if the dictionary page is close enough to the interesting range we coalesce the read
	// if not then we end up reading it sequentially
	if dictOffset, dictSize, ok := f.DictionaryPageBounds(rgi, col.ColumnIndex); ok {
		if minOffset-(dictOffset+dictSize) < coalesceableLabelsGap {
			minOffset = dictOffset
		}
	}

	bufRdrAt, err := newBufferedReaderAt(f.Reader(ctx), int64(minOffset), int64(maxOffset))
	if err != nil {
		return nil, fmt.Errorf("unable to create buffered reader: %w", err)
	}
	defer bufRdrAt.Close()

	pgs := cc.PagesFrom(bufRdrAt)
	defer func() { _ = pgs.Close() }()

	buf := getValueBuffer()
	defer putValueBuffer(buf)

	for _, p := range readPgs {
		pfrom := p.pfrom
		pto := p.pto

		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		pagesScanned.WithLabelValues(ec.path(), scanEqual, method).Add(1)

		// The page has the value, we need to find the matching row ranges
		bl := int(max(pfrom, from) - pfrom)
		br := int(pg.NumRows()) - int(pto-min(pto, to))

		// NOTE: shortcut to skip equality checks on constant pages
		// Only applies when dictionary has one value AND all rows are non-null
		if dict := pg.Dictionary(); dict.Len() == 1 && pg.NumNulls() == 0 {
			if ec.matches(dict.Index(0)) {
				res = append(res, rowRange{pfrom + int64(bl), int64(br - bl)})
				parquet.Release(pg)
				continue
			}
		}

		vr := pg.Values()

		off, count := bl, 0
		idx := 0
		for idx < br {
			n, err := vr.ReadValues(buf)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("unable to read page values: %w", err)
			}
			if n == 0 {
				break
			}

			for i := 0; i < n && idx < br; i++ {
				if idx < bl {
					idx++
					continue
				}

				if !ec.matches(buf[i]) {
					if count != 0 {
						res = append(res, rowRange{pfrom + int64(off), int64(count)})
					}
					off, count = idx, 0
				} else {
					if count == 0 {
						off = idx
					}
					count++
				}
				idx++
			}
		}
		if count != 0 {
			res = append(res, rowRange{pfrom + int64(off), int64(count)})
		}
		parquet.Release(pg)
	}

	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (ec *equalConstraint) init(s *parquet.Schema) error {
	ec.matchesEmpty = len(ec.val.ByteArray()) == 0

	c, ok := s.Lookup(ec.path())
	if !ok {
		return nil
	}
	stringKind := parquet.String().Type().Kind()
	if ec.val.Kind() != stringKind {
		return fmt.Errorf("schema: can only search string kind, got: %s", ec.val.Kind())
	}
	if c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	ec.comp = c.Node.Type().Compare
	return nil
}

func (ec *equalConstraint) path() string {
	return ec.pth
}

func (ec *equalConstraint) matches(v parquet.Value) bool {
	return bytes.Equal(v.ByteArray(), ec.val.ByteArray())
}

// r MUST be a matcher of type Regex
func Regex(path string, r *labels.Matcher) (Constraint, error) {
	if r.Type != labels.MatchRegexp {
		return nil, fmt.Errorf("unsupported matcher type: %s", r.Type)
	}
	return &regexConstraint{pth: path, cache: make(map[parquet.Value]bool), r: r}, nil
}

type regexConstraint struct {
	pth   string
	cache map[parquet.Value]bool

	// set members for regexes like "foo|bar|baz", sorted
	setMembers []parquet.Value

	// bounds for prefix regexes like "thanos-.*"
	minv parquet.Value
	maxv parquet.Value

	// cached result of whether regex matches empty string
	matchesEmpty bool

	r *labels.Matcher

	comp func(l, r parquet.Value) int
}

func (rc *regexConstraint) prefilter(f *schema.FileWithReader, rgi int, rr []rowRange) ([]rowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	rg := f.File().RowGroups()[rgi]

	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	col, ok := rg.Schema().Lookup(rc.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if rc.matchesEmpty {
			return slices.Clone(rr), nil
		}
		return []rowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex].(*parquet.FileColumnChunk)

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	res := make([]rowRange, 0)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if rc.matchesEmpty {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		// If page is constant (min == max) and has no nulls, we can decide without reading
		if rc.comp(minv, maxv) == 0 && cidx.NullCount(i) == 0 {
			if rc.matches(minv) {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}
		if len(rc.setMembers) > 0 {
			if !rc.anySetMemberInRange(minv, maxv) {
				continue
			}
		} else if !rc.minv.IsNull() && !rc.maxv.IsNull() {
			if !rc.matchesEmpty && rc.comp(rc.minv, maxv) > 0 {
				if cidx.IsDescending() {
					break
				}
				continue
			}
			if !rc.matchesEmpty && rc.comp(rc.maxv, minv) < 0 {
				if cidx.IsAscending() {
					break
				}
				continue
			}
		}
		res = append(res, rowRange{from: pfrom, count: pto - pfrom})
	}
	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (rc *regexConstraint) filter(ctx context.Context, f *schema.FileWithReader, rgi int, rr []rowRange) ([]rowRange, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	ctx, span := tracing.Tracer().Start(ctx, "Filter Constraint")
	defer span.End()

	span.SetAttributes(
		attribute.String("type", "regex"),
		attribute.String("path", rc.path()),
		attribute.String("value", rc.r.GetRegexString()),
		attribute.Int("row_group", rgi),
	)

	rg := f.File().RowGroups()[rgi]
	method := methodFromContext(ctx)

	from, to := rr[0].from, rr[len(rr)-1].from+rr[len(rr)-1].count

	col, ok := rg.Schema().Lookup(rc.path())
	if !ok {
		// If match empty, return rr (filter nothing)
		// otherwise return empty
		if rc.matchesEmpty {
			return slices.Clone(rr), nil
		}
		return []rowRange{}, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex].(*parquet.FileColumnChunk)

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var (
		res     = make([]rowRange, 0)
		readPgs = make([]pageToRead, 0)
	)
	for i := range cidx.NumPages() {
		// If page does not intersect from, to; we can immediately discard it
		pfrom := oidx.FirstRowIndex(i)
		pcount := rg.NumRows() - pfrom
		if i < oidx.NumPages()-1 {
			pcount = oidx.FirstRowIndex(i+1) - pfrom
		}
		pto := pfrom + pcount
		if pfrom > to {
			break
		}
		if pto < from {
			continue
		}
		// Page intersects [from, to] but we might be able to discard it with statistics
		if cidx.NullPage(i) {
			if rc.matchesEmpty {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		// If page is constant (min == max) and has no nulls, we can decide without reading
		if rc.comp(minv, maxv) == 0 && cidx.NullCount(i) == 0 {
			if rc.matches(minv) {
				res = append(res, rowRange{pfrom, pcount})
			}
			continue
		}
		if len(rc.setMembers) > 0 {
			if !rc.anySetMemberInRange(minv, maxv) {
				continue
			}
		} else if !rc.minv.IsNull() && !rc.maxv.IsNull() {
			if !rc.matchesEmpty && rc.comp(rc.minv, maxv) > 0 {
				if cidx.IsDescending() {
					break
				}
				continue
			}
			if !rc.matchesEmpty && rc.comp(rc.maxv, minv) < 0 {
				if cidx.IsAscending() {
					break
				}
				continue
			}
		}
		readPgs = append(readPgs, pageToRead{pfrom: pfrom, pto: pto, idx: i})
	}
	if len(readPgs) == 0 {
		return intersectRowRanges(simplify(res), rr), nil
	}

	minOffset := uint64(oidx.Offset(readPgs[0].idx))
	maxOffset := uint64(oidx.Offset(readPgs[len(readPgs)-1].idx)) + uint64(oidx.CompressedPageSize(readPgs[len(readPgs)-1].idx))

	// if the dictionary page is close enough to the interesting range we coalesce the read
	// if not then we end up reading it sequentially
	if dictOffset, dictSize, ok := f.DictionaryPageBounds(rgi, col.ColumnIndex); ok {
		if minOffset-(dictOffset+dictSize) < coalesceableLabelsGap {
			minOffset = dictOffset
		}
	}

	bufRdrAt, err := newBufferedReaderAt(f.Reader(ctx), int64(minOffset), int64(maxOffset))
	if err != nil {
		return nil, fmt.Errorf("unable to create buffered reader: %w", err)
	}
	defer bufRdrAt.Close()

	pgs := cc.PagesFrom(bufRdrAt)
	defer func() { _ = pgs.Close() }()

	buf := getValueBuffer()
	defer putValueBuffer(buf)

	for _, p := range readPgs {
		pfrom := p.pfrom
		pto := p.pto

		if err := pgs.SeekToRow(pfrom); err != nil {
			return nil, fmt.Errorf("unable to seek to row: %w", err)
		}
		pg, err := pgs.ReadPage()
		if err != nil {
			return nil, fmt.Errorf("unable to read page: %w", err)
		}

		pagesScanned.WithLabelValues(rc.path(), scanRegex, method).Add(1)

		// The page has the value, we need to find the matching row ranges
		bl := int(max(pfrom, from) - pfrom)
		br := int(pg.NumRows()) - int(pto-min(pto, to))

		// NOTE: shortcut to skip regex checks on constant pages
		// Only applies when dictionary has one value AND all rows are non-null
		if dict := pg.Dictionary(); dict.Len() == 1 && pg.NumNulls() == 0 {
			if rc.matches(dict.Index(0)) {
				res = append(res, rowRange{pfrom + int64(bl), int64(br - bl)})
				parquet.Release(pg)
				continue
			}
		}

		vr := pg.Values()

		off, count := bl, 0
		idx := 0
		for idx < br {
			n, err := vr.ReadValues(buf)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("unable to read page values: %w", err)
			}
			if n == 0 {
				break
			}

			for i := 0; i < n && idx < br; i++ {
				if idx < bl {
					idx++
					continue
				}

				if !rc.matches(buf[i]) {
					if count != 0 {
						res = append(res, rowRange{pfrom + int64(off), int64(count)})
					}
					off, count = idx, 0
				} else {
					if count == 0 {
						off = idx
					}
					count++
				}
				idx++
			}
		}
		if count != 0 {
			res = append(res, rowRange{pfrom + int64(off), int64(count)})
		}
		parquet.Release(pg)
	}

	if len(res) == 0 {
		return nil, nil
	}
	return intersectRowRanges(simplify(res), rr), nil
}

func (rc *regexConstraint) init(s *parquet.Schema) error {
	rc.matchesEmpty = rc.r.Matches("")
	rc.minv = parquet.NullValue()
	rc.maxv = parquet.NullValue()

	c, ok := s.Lookup(rc.path())
	if !ok {
		return nil
	}
	if stringKind := parquet.String().Type().Kind(); c.Node.Type().Kind() != stringKind {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", stringKind, c.Node.Type().Kind())
	}
	rc.cache = make(map[parquet.Value]bool)
	rc.comp = c.Node.Type().Compare

	if len(rc.r.SetMatches()) > 0 {
		rc.setMembers = make([]parquet.Value, len(rc.r.SetMatches()))
		for i, m := range rc.r.SetMatches() {
			rc.setMembers[i] = parquet.ValueOf(m)
		}
		slices.SortFunc(rc.setMembers, rc.comp)
	} else if len(rc.r.Prefix()) > 0 {
		prefix := rc.r.Prefix()
		rc.minv = parquet.ValueOf(prefix)
		rc.maxv = parquet.ValueOf(prefix + "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff")
	}

	return nil
}

func (rc *regexConstraint) path() string {
	return rc.pth
}

func (rc *regexConstraint) matches(v parquet.Value) bool {
	accept, seen := rc.cache[v]
	if !seen {
		accept = rc.r.Matches(yoloString(v.ByteArray()))
		rc.cache[v] = accept
	}
	return accept
}

func (rc *regexConstraint) anySetMemberInRange(pageMin, pageMax parquet.Value) bool {
	for _, member := range rc.setMembers {
		if rc.comp(member, pageMin) < 0 {
			continue
		}
		if rc.comp(member, pageMax) > 0 {
			break
		}
		return true
	}
	return false
}

func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

func Not(c Constraint) Constraint {
	return &notConstraint{c: c}
}

type notConstraint struct {
	c Constraint
}

func (nc *notConstraint) prefilter(_ *schema.FileWithReader, _ int, rr []rowRange) ([]rowRange, error) {
	// NOT constraints cannot be prefiltered since the child constraint returns a superset of the matching row range,
	// if we were to complement this row range the result here would be a subset and this would violate our interface.
	return slices.Clone(rr), nil
}

func (nc *notConstraint) filter(ctx context.Context, f *schema.FileWithReader, rgi int, rr []rowRange) ([]rowRange, error) {
	ctx, span := tracing.Tracer().Start(ctx, "Filter Constraint")
	defer span.End()

	span.SetAttributes(
		attribute.String("type", "not"),
		attribute.String("path", nc.path()),
		attribute.Int("row_group", rgi),
	)

	base, err := nc.c.filter(ctx, f, rgi, rr)
	if err != nil {
		return nil, fmt.Errorf("unable to filter child constraint: %w", err)
	}
	// no need to intersect since its already subset of rr
	return complementRowRanges(base, rr), nil
}

func (nc *notConstraint) init(s *parquet.Schema) error {
	return nc.c.init(s)
}

func (nc *notConstraint) path() string {
	return nc.c.path()
}
