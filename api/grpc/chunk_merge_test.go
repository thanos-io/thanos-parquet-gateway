// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package grpc

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos-parquet-gateway/search"
)

func TestLoserTreeChunkMerger_Basic(t *testing.T) {
	labels1 := labels.FromStrings("__name__", "test_metric", "job", "test1")
	labels2 := labels.FromStrings("__name__", "test_metric", "job", "test2")

	chunk1 := chunks.Meta{
		MinTime: 1000,
		MaxTime: 2000,
		Chunk:   &mockChunk{encoding: 1, samples: 10, data: []byte("chunk1data")},
	}
	chunk2 := chunks.Meta{
		MinTime: 2000,
		MaxTime: 3000,
		Chunk:   &mockChunk{encoding: 1, samples: 10, data: []byte("chunk2data")},
	}

	responses := []ChunkResponse{
		{
			Series: search.SeriesChunks{
				Lset:   labels1,
				Chunks: []chunks.Meta{chunk1},
			},
			ShardIndex: 0,
		},
		{
			Series: search.SeriesChunks{
				Lset:   labels2,
				Chunks: []chunks.Meta{chunk2},
			},
			ShardIndex: 1,
		},
	}

	merger := NewLoserTreeChunkMerger(responses)
	result, err := merger.MergeToStorePB()
	require.NoError(t, err)

	require.Len(t, result, 2)

	for _, series := range result {
		require.NotEmpty(t, series.Labels)
		require.NotEmpty(t, series.Chunks)
	}
}

func TestLoserTreeChunkMerger_EmptyInput(t *testing.T) {
	merger := NewLoserTreeChunkMerger([]ChunkResponse{})
	result, err := merger.MergeToStorePB()
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestLoserTreeChunkMerger_SameLabels(t *testing.T) {
	labels1 := labels.FromStrings("__name__", "test_metric", "job", "test")

	chunk1 := chunks.Meta{
		MinTime: 1000,
		MaxTime: 2000,
		Chunk:   &mockChunk{encoding: 1, samples: 10, data: []byte("chunk1")},
	}
	chunk2 := chunks.Meta{
		MinTime: 2000,
		MaxTime: 3000,
		Chunk:   &mockChunk{encoding: 1, samples: 10, data: []byte("chunk2")},
	}

	responses := []ChunkResponse{
		{
			Series: search.SeriesChunks{
				Lset:   labels1,
				Chunks: []chunks.Meta{chunk1},
			},
			ShardIndex: 0,
		},
		{
			Series: search.SeriesChunks{
				Lset:   labels1,
				Chunks: []chunks.Meta{chunk2},
			},
			ShardIndex: 1,
		},
	}

	merger := NewLoserTreeChunkMerger(responses)
	result, err := merger.MergeToStorePB()
	require.NoError(t, err)

	require.Len(t, result, 1)
	require.Len(t, result[0].Chunks, 2)
}

type mockChunk struct {
	encoding int
	samples  int
	data     []byte
}

func (m *mockChunk) Encoding() chunkenc.Encoding {
	return chunkenc.Encoding(m.encoding)
}

func (m *mockChunk) Bytes() []byte {
	return m.data
}

func (m *mockChunk) NumSamples() int {
	return m.samples
}

func (m *mockChunk) Compact() {}

func (m *mockChunk) Size() int {
	return len(m.data)
}

func (m *mockChunk) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	return &mockIterator{}
}

func (m *mockChunk) Appender() (chunkenc.Appender, error) {
	return nil, nil
}

func (m *mockChunk) Reset([]byte) {}

type mockIterator struct{}

func (m *mockIterator) Seek(int64) chunkenc.ValueType { return chunkenc.ValNone }
func (m *mockIterator) Next() chunkenc.ValueType      { return chunkenc.ValNone }
func (m *mockIterator) At() (int64, float64)          { return 0, 0 }
func (m *mockIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}
func (m *mockIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}
func (m *mockIterator) AtT() int64 { return 0 }
func (m *mockIterator) Err() error { return nil }
