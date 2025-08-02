// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package grpc

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/thanos-io/thanos-parquet-gateway/search"
)

func generateTestChunks() []chunks.Meta {
	now := time.Now().UnixMilli()
	chunkMetas := make([]chunks.Meta, 2)

	chk1 := chunkenc.NewXORChunk()
	appender1, _ := chk1.Appender()
	for i := 0; i < 10; i++ {
		appender1.Append(now-int64(i*60000), float64(i))
	}
	chunkMetas[0] = chunks.Meta{
		Chunk:   chk1,
		MinTime: now - 9*60000,
		MaxTime: now,
	}

	chk2 := chunkenc.NewXORChunk()
	appender2, _ := chk2.Appender()
	for i := 10; i < 20; i++ {
		appender2.Append(now-int64(i*60000), float64(i))
	}
	chunkMetas[1] = chunks.Meta{
		Chunk:   chk2,
		MinTime: now - 19*60000,
		MaxTime: now - 10*60000,
	}

	return chunkMetas
}

func TestChunkMergerIntegration(t *testing.T) {
	testChunks := generateTestChunks()

	testLabels := labels.Labels{
		{Name: "__name__", Value: "test_metric"},
		{Name: "job", Value: "test"},
	}

	responses := []ChunkResponse{
		{
			Series: search.SeriesChunks{
				Lset:   testLabels,
				Chunks: testChunks[:1],
			},
			ShardIndex: 0,
		},
		{
			Series: search.SeriesChunks{
				Lset:   testLabels,
				Chunks: testChunks[1:],
			},
			ShardIndex: 1,
		},
	}

	merger := NewLoserTreeChunkMerger(responses)
	mergedSeries, err := merger.MergeToStorePB()

	if err != nil {
		t.Fatalf("Merger failed: %v", err)
	}

	if len(mergedSeries) != 1 {
		t.Fatalf("Expected 1 merged series, got %d", len(mergedSeries))
	}

	series := mergedSeries[0]

	if len(series.Labels) == 0 {
		t.Fatal("Merged series should have labels")
	}

	if len(series.Chunks) != 2 {
		t.Fatalf("Expected 2 chunks in merged series, got %d", len(series.Chunks))
	}

	for i, chunk := range series.Chunks {
		if chunk.Raw == nil {
			t.Fatalf("Chunk %d should have raw data", i)
		}
		if len(chunk.Raw.Data) == 0 {
			t.Fatalf("Chunk %d raw data should not be empty", i)
		}
		if chunk.MinTime > chunk.MaxTime {
			t.Fatalf("Chunk %d MinTime (%d) should be <= MaxTime (%d)", i, chunk.MinTime, chunk.MaxTime)
		}
	}

	t.Logf("Successfully merged %d responses into %d series with %d total chunks",
		len(responses), len(mergedSeries), len(series.Chunks))
}

func TestChunkEncodingOptimization(t *testing.T) {
	t.Log("Testing optimized chunk encoding approach...")

	testChunks := generateTestChunks()

	merger := &LoserTreeChunkMerger{}

	storeChunks, err := merger.chunksToStorePB(testChunks)
	if err != nil {
		t.Fatalf("Optimized chunksToStorePB failed: %v", err)
	}

	if len(storeChunks) != 2 {
		t.Fatalf("Expected 2 store chunks, got %d", len(storeChunks))
	}

	for i, chunk := range storeChunks {
		originalChunk := testChunks[i]

		if chunk.Raw == nil {
			t.Fatalf("Chunk %d missing raw data", i)
		}
		if len(chunk.Raw.Data) == 0 {
			t.Fatalf("Chunk %d has empty raw data", i)
		}

		expectedBytes := originalChunk.Chunk.Bytes()
		if len(chunk.Raw.Data) != len(expectedBytes) {
			t.Errorf("Chunk %d: Expected raw data length %d, got %d",
				i, len(expectedBytes), len(chunk.Raw.Data))
		}

		expectedEncoding := originalChunk.Chunk.Encoding()
		if int(chunk.Raw.Type) != int(expectedEncoding) {
			t.Errorf("Chunk %d: Expected encoding %d, got %d",
				i, expectedEncoding, chunk.Raw.Type)
		}

		t.Logf("Chunk %d: MinTime=%d, MaxTime=%d, Encoding=%d, Data length=%d bytes (optimized)",
			i, chunk.MinTime, chunk.MaxTime, chunk.Raw.Type, len(chunk.Raw.Data))
	}

	t.Log("Optimized encoding verified: raw chunk bytes passed directly without re-encoding")
}
