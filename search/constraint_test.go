// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"bytes"
	"context"
	"io"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/goleak"

	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func buildFile[T any](t testing.TB, rows []T) *schema.FileWithReader {
	buf := bytes.NewBuffer(nil)
	w := parquet.NewGenericWriter[T](buf, parquet.PageBufferSize(12), parquet.WriteBufferSize(0))
	for _, row := range rows {
		if _, err := w.Write([]T{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(buf.Bytes())
	file, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatal(err)
	}
	f, err := schema.NewFileWithReader(file, func(_ context.Context) io.ReaderAt { return reader })
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func mustMatchersToConstraint(t testing.TB, m ...*labels.Matcher) []Constraint {
	res, err := matchersToConstraint(m...)
	if err != nil {
		t.Fatalf("unable to build matchers from constraints: %s", err)
	}
	return res
}

func TestFilter(t *testing.T) {
	type expectation struct {
		constraints []Constraint
		expect      []rowRange
	}
	type testcase[T any] struct {
		rows         []T
		expectations []expectation
	}

	t.Run("", func(t *testing.T) {
		type s struct {
			A string `parquet:"___cf_meta_label_A,optional,dict"`
			B string `parquet:"___cf_meta_label_B,optional,dict"`
			C string `parquet:"___cf_meta_label_C,optional,dict"`
		}
		for _, tc := range []testcase[s]{
			{
				rows: []s{
					{A: "1", B: "2", C: "a"},
					{A: "3", B: "4", C: "b"},
					{A: "7", B: "12", C: "c"},
					{A: "9", B: "22", C: "d"},
					{A: "0", B: "1", C: "e"},
					{A: "7", B: "1", C: "f"},
					{A: "7", B: "1", C: "g"},
					{A: "0", B: "1", C: "h"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Equal("___cf_meta_label_A", parquet.ValueOf("7")),
							Equal("___cf_meta_label_C", parquet.ValueOf("g")),
						},
						expect: []rowRange{
							{from: 6, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Equal("___cf_meta_label_A", parquet.ValueOf("7")),
						},
						expect: []rowRange{
							{from: 2, count: 1},
							{from: 5, count: 2},
						},
					},
					{
						constraints: []Constraint{
							Equal("___cf_meta_label_A", parquet.ValueOf("7")), Not(Equal("___cf_meta_label_B", parquet.ValueOf("1"))),
						},
						expect: []rowRange{
							{from: 2, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Equal("___cf_meta_label_A", parquet.ValueOf("7")), Not(Equal("___cf_meta_label_C", parquet.ValueOf("c"))),
						},
						expect: []rowRange{
							{from: 5, count: 2},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("___cf_meta_label_A", parquet.ValueOf("227"))),
						},
						expect: []rowRange{
							{from: 0, count: 8},
						},
					},
					{
						constraints: mustMatchersToConstraint(t,
							labels.MustNewMatcher(labels.MatchRegexp, "C", "a|c|d"),
						),
						expect: []rowRange{
							{from: 0, count: 1},
							{from: 2, count: 2},
						},
					},
				},
			},
			{
				rows: []s{
					{A: "1", B: "2"},
					{A: "1", B: "3"},
					{A: "1", B: "4"},
					{A: "1", B: "4"},
					{A: "1", B: "5"},
					{A: "1", B: "5"},
					{A: "2", B: "5"},
					{A: "2", B: "5"},
					{A: "2", B: "5"},
					{A: "3", B: "5"},
					{A: "3", B: "6"},
					{A: "3", B: "2"},
					{A: "4", B: "8", C: "foo"},
				},
				expectations: []expectation{
					{
						constraints: mustMatchersToConstraint(t,
							labels.MustNewMatcher(labels.MatchRegexp, "C", ""),
						),
						expect: []rowRange{
							{from: 0, count: 12},
						},
					},
					{
						constraints: []Constraint{
							Equal("___cf_meta_label_C", parquet.ValueOf("")),
						},
						expect: []rowRange{
							{from: 0, count: 12},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("___cf_meta_label_A", parquet.ValueOf("3"))),
						},
						expect: []rowRange{
							{from: 0, count: 9},
							{from: 12, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("___cf_meta_label_A", parquet.ValueOf("3"))),
							Equal("___cf_meta_label_B", parquet.ValueOf("5")),
						},
						expect: []rowRange{
							{from: 4, count: 5},
						},
					},
					{
						constraints: []Constraint{
							Not(Equal("___cf_meta_label_A", parquet.ValueOf("3"))),
							Not(Equal("___cf_meta_label_A", parquet.ValueOf("1"))),
						},
						expect: []rowRange{
							{from: 6, count: 3},
							{from: 12, count: 1},
						},
					},
					{
						constraints: []Constraint{
							Equal("___cf_meta_label_A", parquet.ValueOf("2")),
							Not(Equal("___cf_meta_label_B", parquet.ValueOf("5"))),
						},
						expect: []rowRange{},
					},
					{
						constraints: []Constraint{
							Equal("___cf_meta_label_A", parquet.ValueOf("2")),
							Not(Equal("___cf_meta_label_B", parquet.ValueOf("5"))),
						},
						expect: []rowRange{},
					},
					{
						constraints: []Constraint{
							Equal("___cf_meta_label_A", parquet.ValueOf("3")),
							Not(Equal("___cf_meta_label_B", parquet.ValueOf("2"))),
						},
						expect: []rowRange{
							{from: 9, count: 2},
						},
					},
				},
			},
			{
				rows: []s{
					{A: "1", B: "1"},
					{A: "1", B: "2"},
					{A: "2", B: "1"},
					{A: "2", B: "2"},
					{A: "1", B: "1"},
					{A: "1", B: "2"},
					{A: "2", B: "1"},
					{A: "2", B: "2"},
				},
				expectations: []expectation{
					{
						constraints: []Constraint{
							Not(Equal("___cf_meta_label_A", parquet.ValueOf("1"))),
							Not(Equal("___cf_meta_label_B", parquet.ValueOf("2"))),
						},
						expect: []rowRange{
							{from: 2, count: 1},
							{from: 6, count: 1},
						},
					},
				},
			},
			{
				rows: []s{
					{C: "foo"},
					{C: "bar"},
					{C: "foo"},
					{C: "buz"},
				},
				expectations: []expectation{
					{
						constraints: mustMatchersToConstraint(t,
							labels.MustNewMatcher(labels.MatchRegexp, "C", "f.*"),
						),
						expect: []rowRange{
							{from: 0, count: 1},
							{from: 2, count: 1},
						},
					},
					{
						constraints: mustMatchersToConstraint(t,
							labels.MustNewMatcher(labels.MatchRegexp, "C", "b.*"),
						),
						expect: []rowRange{
							{from: 1, count: 1},
							{from: 3, count: 1},
						},
					},
					{
						constraints: mustMatchersToConstraint(t,
							labels.MustNewMatcher(labels.MatchRegexp, "C", "f.*|b.*"),
						),
						expect: []rowRange{
							{from: 0, count: 4},
						},
					},
				},
			},
		} {

			ctx := contextWithMethod(t.Context(), "test")
			sfile := buildFile(t, tc.rows)
			for _, expectation := range tc.expectations {
				t.Run("", func(t *testing.T) {
					if err := initialize(sfile.File().Schema(), expectation.constraints...); err != nil {
						t.Fatal(err)
					}
					for i := range sfile.File().RowGroups() {
						rr, err := filter(ctx, sfile, i, expectation.constraints...)
						if err != nil {
							t.Fatal(err)
						}
						if !slices.Equal(rr, expectation.expect) {
							t.Fatalf("expected %+v, got %+v", expectation.expect, rr)
						}
					}
				})
			}
		}
	})
}
