// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"fmt"
	"io"
	"slices"
	"sort"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
)

type equalConstraint struct {
	path string

	val parquet.Value

	// set during init
	col  int
	comp func(l, r parquet.Value) int

	inspectPages bool
}

var _ Constraint = &equalConstraint{}

func Equal(path string, value parquet.Value) Constraint {
	return &equalConstraint{path: path, val: value}
}

func EqualWithPageCheck(path string, value parquet.Value) Constraint {
	return &equalConstraint{path: path, val: value, inspectPages: true}
}

func (ec *equalConstraint) rowRanges(rg parquet.RowGroup) ([]rowRange, error) {
	col, ok := rg.Schema().Lookup(ec.path)
	if !ok {
		return nil, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex]

	if skip, err := ec.skipByBloomfilter(cc); err != nil {
		return nil, fmt.Errorf("unable to skip by bloomfilter: %w", err)
	} else if skip {
		return nil, nil
	}

	var pgs parquet.Pages
	if ec.inspectPages {
		pgs = cc.Pages()
		defer pgs.Close()
	}

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}
	var buf []parquet.Value

	res := make([]rowRange, 0)
	for i := 0; i < cidx.NumPages(); i++ {
		if cidx.NullPage(i) {
			pagesDiscarded.WithLabelValues(ec.path).Inc()
			continue
		}
		minv, maxv := cidx.MinValue(i), cidx.MaxValue(i)
		if !ec.val.IsNull() && !maxv.IsNull() && ec.comp(ec.val, maxv) > 0 {
			pagesDiscarded.WithLabelValues(ec.path).Inc()
			if cidx.IsDescending() {
				break
			}
			continue
		}
		if !ec.val.IsNull() && !minv.IsNull() && ec.comp(ec.val, minv) < 0 {
			pagesDiscarded.WithLabelValues(ec.path).Inc()
			if cidx.IsAscending() {
				break
			}
			continue
		}
		from := oidx.FirstRowIndex(i)

		// TODO: this would also work for descending columns, but for now
		// this is only used on __name__ which is ascending anyway so we dont
		// bother implementing it for descending or unordered columns.
		// TODO: for unordered columns we could inspect the dictionary here
		// and for descending columns we just have to flip the inequalities
		if ec.inspectPages && cidx.IsAscending() {
			if err := pgs.SeekToRow(from); err != nil {
				return nil, fmt.Errorf("unable to seek to row: %w", err)
			}
			pg, err := pgs.ReadPage()
			if err != nil {
				return nil, fmt.Errorf("unable to read page: %w", err)
			}
			pagesRead.WithLabelValues(ec.path).Inc()
			vals, rows := pg.Values(), pg.NumRows()
			buf = slices.Grow(buf, int(rows))[:rows]
			n, err := vals.ReadValues(buf)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("unable to read page values")
			}
			l := sort.Search(n, func(i int) bool { return ec.comp(ec.val, buf[i]) <= 0 })
			r := sort.Search(n, func(i int) bool { return ec.comp(ec.val, buf[i]) < 0 })
			res = append(res, rowRange{from: from + int64(l), count: int64(r - l)})
		} else {
			count := rg.NumRows() - from
			if i < oidx.NumPages()-1 {
				count = oidx.FirstRowIndex(i+1) - from
			}
			res = append(res, rowRange{from: from, count: count})
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return simplify(res), nil
}

func (ec *equalConstraint) accept(r parquet.Row) bool {
	return ec.comp(r[ec.col], ec.val) == 0
}

func (ec *equalConstraint) init(s *parquet.Schema) error {
	c, ok := s.Lookup(ec.path)
	if !ok {
		return fmt.Errorf("schema: must contain path: %s", ec.path)
	}
	if c.Node.Type().Kind() != ec.val.Kind() {
		return fmt.Errorf("schema: cannot search value of kind %s in column of kind %s", ec.val.Kind(), c.Node.Type().Kind())
	}
	ec.comp = c.Node.Type().Compare
	ec.col = c.ColumnIndex
	return nil
}

func (ec *equalConstraint) skipByBloomfilter(cc parquet.ColumnChunk) (bool, error) {
	bf := cc.BloomFilter()
	if bf == nil {
		return false, nil
	}
	ok, err := bf.Check(ec.val)
	if err != nil {
		return false, fmt.Errorf("unable to check bloomfilter: %w", err)
	}
	return !ok, nil
}

type andConstraint struct {
	cs []Constraint
}

var _ Constraint = &andConstraint{}

func And(cs ...Constraint) Constraint {
	return &andConstraint{cs: cs}
}

func (ac *andConstraint) rowRanges(rg parquet.RowGroup) ([]rowRange, error) {
	var res []rowRange
	for i := range ac.cs {
		rrs, err := ac.cs[i].rowRanges(rg)
		if err != nil {
			return nil, fmt.Errorf("unable to get lhs row ranges: %w", err)
		}
		if i == 0 {
			res = rrs
		} else {
			res = intersectRowRanges(res, rrs)
		}
	}
	return simplify(res), nil
}

func (ac *andConstraint) accept(r parquet.Row) bool {
	for i := range ac.cs {
		if !ac.cs[i].accept(r) {
			return false
		}
	}
	return true
}

func (ac *andConstraint) init(s *parquet.Schema) error {
	for i := range ac.cs {
		if err := ac.cs[i].init(s); err != nil {
			return fmt.Errorf("unable to init constraint %d: %w", i, err)
		}
	}
	return nil
}

type notConstraint struct {
	cs Constraint
}

var _ Constraint = &notConstraint{}

func Not(cs Constraint) Constraint {
	return &notConstraint{cs: cs}
}

func (nc *notConstraint) rowRanges(rg parquet.RowGroup) ([]rowRange, error) {
	return []rowRange{{from: 0, count: int64(rg.NumRows())}}, nil
}

func (nc *notConstraint) accept(r parquet.Row) bool {
	return !nc.cs.accept(r)
}

func (nc *notConstraint) init(s *parquet.Schema) error {
	return nc.cs.init(s)
}

type nullConstraint struct {
}

var _ Constraint = &nullConstraint{}

func Null() Constraint {
	return &nullConstraint{}
}

func (null *nullConstraint) rowRanges(parquet.RowGroup) ([]rowRange, error) {
	return nil, nil
}

func (null *nullConstraint) accept(parquet.Row) bool {
	return false
}

func (null *nullConstraint) init(_ *parquet.Schema) error {
	return nil
}

type orConstraint struct {
	cs []Constraint
}

var _ Constraint = &orConstraint{}

func Or(cs ...Constraint) Constraint {
	return &orConstraint{cs: cs}
}

func Set(path string, values []parquet.Value) Constraint {
	equals := []Constraint{}
	for _, val := range values {
		equals = append(equals, Equal(path, val))
	}
	return &orConstraint{cs: equals}
}

func (oc *orConstraint) rowRanges(rg parquet.RowGroup) ([]rowRange, error) {
	var res []rowRange
	for i := range oc.cs {
		rrs, err := oc.cs[i].rowRanges(rg)
		if err != nil {
			return nil, fmt.Errorf("unable to get lhs row ranges: %w", err)
		}
		res = append(res, rrs...)
	}
	return simplify(res), nil
}

func (oc *orConstraint) accept(r parquet.Row) bool {
	for i := range oc.cs {
		if oc.cs[i].accept(r) {
			return true
		}
	}
	return false
}

func (oc *orConstraint) init(s *parquet.Schema) error {
	for i := range oc.cs {
		if err := oc.cs[i].init(s); err != nil {
			return fmt.Errorf("unable to init constraint %d: %w", i, err)
		}
	}
	return nil
}

type regexConstraint struct {
	path    string
	matcher *labels.FastRegexMatcher

	// set during init
	col   int
	cache map[string]bool
}

var _ Constraint = &regexConstraint{}

func Regex(path string, regex string) (Constraint, error) {
	matcher, err := labels.NewFastRegexMatcher(regex)
	if err != nil {
		return nil, err
	}

	if len(matcher.SetMatches()) > 0 {
		vals := []parquet.Value{}
		for _, match := range matcher.SetMatches() {
			vals = append(vals, parquet.ValueOf(match))
		}
		return Set(path, vals), nil
	}

	return &regexConstraint{
		matcher: matcher,
		path:    path,
	}, nil
}

func (ec *regexConstraint) rowRanges(rg parquet.RowGroup) ([]rowRange, error) {
	col, ok := rg.Schema().Lookup(ec.path)
	if !ok {
		return nil, nil
	}
	cc := rg.ColumnChunks()[col.ColumnIndex]

	oidx, err := cc.OffsetIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read offset index: %w", err)
	}
	cidx, err := cc.ColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("unable to read column index: %w", err)
	}

	res := []rowRange{}
	for i := 0; i < cidx.NumPages(); i++ {
		if cidx.NullPage(i) {
			pagesDiscarded.WithLabelValues(ec.path).Inc()
			continue
		}
		from := oidx.FirstRowIndex(i)
		count := rg.NumRows() - from
		if i < oidx.NumPages()-1 {
			count = oidx.FirstRowIndex(i+1) - from
		}
		res = append(res, rowRange{from: from, count: count})
	}
	return simplify(res), nil
}

func (ec *regexConstraint) accept(r parquet.Row) bool {
	val := r[ec.col].String()
	accept, seen := ec.cache[val]
	if !seen {
		accept = ec.matcher.MatchString(val)
		ec.cache[val] = accept
	}
	return accept
}

func (ec *regexConstraint) init(s *parquet.Schema) error {
	c, ok := s.Lookup(ec.path)
	if !ok {
		return fmt.Errorf("schema: must contain path: %s", ec.path)
	}

	if c.Node.Type().Kind() != parquet.ByteArray {
		return fmt.Errorf("schema: expected string, cannot assert regex of type %s", c.Node.Type().String())
	}

	ec.cache = map[string]bool{}
	ec.col = c.ColumnIndex

	return nil
}
