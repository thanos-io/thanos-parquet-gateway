// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

type mockChunkSeries struct {
	lset   labels.Labels
	chunks []chunks.Meta
}

func (s *mockChunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *mockChunkSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return &mockChunkIterator{chunks: s.chunks, idx: -1}
}

type mockChunkIterator struct {
	chunks []chunks.Meta
	idx    int
}

func (it *mockChunkIterator) At() chunks.Meta { return it.chunks[it.idx] }
func (it *mockChunkIterator) Next() bool      { it.idx++; return it.idx < len(it.chunks) }
func (it *mockChunkIterator) Err() error      { return nil }

type mockChunkSeriesSet struct {
	series []storage.ChunkSeries
	idx    int
}

func (s *mockChunkSeriesSet) Next() bool                        { s.idx++; return s.idx < len(s.series) }
func (s *mockChunkSeriesSet) At() storage.ChunkSeries           { return s.series[s.idx] }
func (s *mockChunkSeriesSet) Err() error                        { return nil }
func (s *mockChunkSeriesSet) Warnings() annotations.Annotations { return nil }

type sample struct {
	t int64
	v float64
}

func mockSeries(lset labels.Labels, chks ...chunks.Meta) *mockChunkSeries {
	return &mockChunkSeries{lset: lset, chunks: chks}
}

func mockChunk(samples ...sample) chunks.Meta {
	c := chunkenc.NewXORChunk()
	app, _ := c.Appender()
	var minT, maxT int64
	for i, s := range samples {
		app.Append(s.t, s.v)
		if i == 0 {
			minT = s.t
		}
		maxT = s.t
	}
	return chunks.Meta{MinTime: minT, MaxTime: maxT, Chunk: c}
}

func mockSet(series ...*mockChunkSeries) *mockChunkSeriesSet {
	s := make([]storage.ChunkSeries, len(series))
	for i, sr := range series {
		s[i] = sr
	}
	return &mockChunkSeriesSet{series: s, idx: -1}
}

func seriesSamples(series storage.ChunkSeries) []sample {
	var samples []sample
	chunkIt := series.Iterator(nil)
	for chunkIt.Next() {
		c := chunkIt.At()
		it := c.Chunk.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			samples = append(samples, sample{ts, v})
		}
	}
	return samples
}

func TestMergeChunkSeriesSet(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sortLabels []string
		sets       []*mockChunkSeriesSet
		expected   *mockChunkSeriesSet
	}{
		{
			name:       "sorted by __name__ with interleaved sets",
			sortLabels: []string{labels.MetricName},
			sets: []*mockChunkSeriesSet{
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_a", "job", "job1"), mockChunk(sample{0, 1}, sample{100, 2})),
					mockSeries(labels.FromStrings("__name__", "metric_c", "job", "job1"), mockChunk(sample{0, 5}, sample{100, 6})),
				),
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_b", "job", "job2"), mockChunk(sample{0, 3}, sample{100, 4})),
					mockSeries(labels.FromStrings("__name__", "metric_d", "job", "job2"), mockChunk(sample{0, 7}, sample{100, 8})),
				),
			},
			expected: mockSet(
				mockSeries(labels.FromStrings("__name__", "metric_a", "job", "job1"), mockChunk(sample{0, 1}, sample{100, 2})),
				mockSeries(labels.FromStrings("__name__", "metric_b", "job", "job2"), mockChunk(sample{0, 3}, sample{100, 4})),
				mockSeries(labels.FromStrings("__name__", "metric_c", "job", "job1"), mockChunk(sample{0, 5}, sample{100, 6})),
				mockSeries(labels.FromStrings("__name__", "metric_d", "job", "job2"), mockChunk(sample{0, 7}, sample{100, 8})),
			),
		},
		{
			name:       "sorted by job with interleaved sets",
			sortLabels: []string{"job"},
			sets: []*mockChunkSeriesSet{
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_a", "job", "job1"), mockChunk(sample{0, 1}, sample{100, 2})),
					mockSeries(labels.FromStrings("__name__", "metric_c", "job", "job3"), mockChunk(sample{50, 5}, sample{150, 6})),
				),
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_b", "job", "job2"), mockChunk(sample{100, 3}, sample{200, 4})),
					mockSeries(labels.FromStrings("__name__", "metric_d", "job", "job4"), mockChunk(sample{200, 7}, sample{300, 8})),
				),
			},
			expected: mockSet(
				mockSeries(labels.FromStrings("__name__", "metric_a", "job", "job1"), mockChunk(sample{0, 1}, sample{100, 2})),
				mockSeries(labels.FromStrings("__name__", "metric_b", "job", "job2"), mockChunk(sample{100, 3}, sample{200, 4})),
				mockSeries(labels.FromStrings("__name__", "metric_c", "job", "job3"), mockChunk(sample{50, 5}, sample{150, 6})),
				mockSeries(labels.FromStrings("__name__", "metric_d", "job", "job4"), mockChunk(sample{200, 7}, sample{300, 8})),
			),
		},
		{
			name:       "multi-key sort order (env, __name__)",
			sortLabels: []string{"env", labels.MetricName},
			sets: []*mockChunkSeriesSet{
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_a", "env", "prod"), mockChunk(sample{0, 1})),
					mockSeries(labels.FromStrings("__name__", "metric_b", "env", "prod"), mockChunk(sample{0, 2})),
				),
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_c", "env", "dev"), mockChunk(sample{0, 3})),
					mockSeries(labels.FromStrings("__name__", "metric_a", "env", "staging"), mockChunk(sample{0, 4})),
				),
			},
			expected: mockSet(
				mockSeries(labels.FromStrings("__name__", "metric_c", "env", "dev"), mockChunk(sample{0, 3})),
				mockSeries(labels.FromStrings("__name__", "metric_a", "env", "prod"), mockChunk(sample{0, 1})),
				mockSeries(labels.FromStrings("__name__", "metric_b", "env", "prod"), mockChunk(sample{0, 2})),
				mockSeries(labels.FromStrings("__name__", "metric_a", "env", "staging"), mockChunk(sample{0, 4})),
			),
		},
		{
			name:       "multi-key sort order (__name__, env)",
			sortLabels: []string{labels.MetricName, "env"},
			sets: []*mockChunkSeriesSet{
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_a", "env", "dev"), mockChunk(sample{0, 1})),
					mockSeries(labels.FromStrings("__name__", "metric_b", "env", "prod"), mockChunk(sample{0, 2})),
				),
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_a", "env", "prod"), mockChunk(sample{0, 3})),
					mockSeries(labels.FromStrings("__name__", "metric_b", "env", "dev"), mockChunk(sample{0, 4})),
				),
			},
			expected: mockSet(
				mockSeries(labels.FromStrings("__name__", "metric_a", "env", "dev"), mockChunk(sample{0, 1})),
				mockSeries(labels.FromStrings("__name__", "metric_a", "env", "prod"), mockChunk(sample{0, 3})),
				mockSeries(labels.FromStrings("__name__", "metric_b", "env", "dev"), mockChunk(sample{0, 4})),
				mockSeries(labels.FromStrings("__name__", "metric_b", "env", "prod"), mockChunk(sample{0, 2})),
			),
		},
		{
			name:       "multiple sets with collisions across sets",
			sortLabels: []string{labels.MetricName},
			sets: []*mockChunkSeriesSet{
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_a", "job", "j1"), mockChunk(sample{0, 1})),
					mockSeries(labels.FromStrings("__name__", "metric_c", "job", "j1"), mockChunk(sample{0, 5})),
				),
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_a", "job", "j1"), mockChunk(sample{100, 2})),
					mockSeries(labels.FromStrings("__name__", "metric_b", "job", "j2"), mockChunk(sample{0, 3})),
				),
				mockSet(
					mockSeries(labels.FromStrings("__name__", "metric_b", "job", "j2"), mockChunk(sample{100, 4})),
					mockSeries(labels.FromStrings("__name__", "metric_d", "job", "j3"), mockChunk(sample{0, 6})),
				),
			},
			expected: mockSet(
				mockSeries(labels.FromStrings("__name__", "metric_a", "job", "j1"), mockChunk(sample{0, 1}, sample{100, 2})),
				mockSeries(labels.FromStrings("__name__", "metric_b", "job", "j2"), mockChunk(sample{0, 3}, sample{100, 4})),
				mockSeries(labels.FromStrings("__name__", "metric_c", "job", "j1"), mockChunk(sample{0, 5})),
				mockSeries(labels.FromStrings("__name__", "metric_d", "job", "j3"), mockChunk(sample{0, 6})),
			),
		},
	} {
		t.Run(tc.name, func(tt *testing.T) {
			sets := make([]storage.ChunkSeriesSet, len(tc.sets))
			for i, s := range tc.sets {
				sets[i] = s
			}

			mergedSet := newMergeChunkSeriesSet(
				sets,
				compareBySortedLabelsFunc(tc.sortLabels),
				storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge),
			)

			for tc.expected.Next() {
				exp := tc.expected.At()
				if !mergedSet.Next() {
					tt.Fatalf("missing expected series: %v", exp.Labels())
				}
				got := mergedSet.At()

				if !labels.Equal(got.Labels(), exp.Labels()) {
					tt.Fatalf("unexpected labels: expected %v, got %v", exp.Labels(), got.Labels())
				}

				expSamples := seriesSamples(exp)
				gotSamples := seriesSamples(got)
				if len(gotSamples) != len(expSamples) {
					tt.Fatalf("unexpected number of samples for series %v: expected %d, got %d",
						exp.Labels(), len(expSamples), len(gotSamples))
				}
				for j, expSample := range expSamples {
					if gotSamples[j].t != expSample.t || gotSamples[j].v != expSample.v {
						tt.Fatalf("unexpected sample %d for series %v: expected {%d, %v}, got {%d, %v}",
							j, exp.Labels(), expSample.t, expSample.v, gotSamples[j].t, gotSamples[j].v)
					}
				}
			}
			if mergedSet.Next() {
				tt.Fatalf("unexpected extra series: %v", mergedSet.At().Labels())
			}
			if err := mergedSet.Err(); err != nil {
				tt.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
