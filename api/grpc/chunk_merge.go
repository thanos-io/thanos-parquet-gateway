// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package grpc

import (
	"container/heap"
	"encoding/binary"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/thanos-io/thanos-parquet-gateway/internal/encoding"
	"github.com/thanos-io/thanos-parquet-gateway/search"
)

// ChunkResponse represents a series with its raw chunk data from a single shard
type ChunkResponse struct {
	Series     search.SeriesChunks
	ShardIndex int
}

// LoserTreeChunkMerger efficiently merges chunk responses from multiple shards
// using a loser tree (min-heap) for optimal performance
type LoserTreeChunkMerger struct {
	responses []ChunkResponse
	heap      chunkResponseHeap
}

// NewLoserTreeChunkMerger creates a new merger for chunk responses
func NewLoserTreeChunkMerger(responses []ChunkResponse) *LoserTreeChunkMerger {
	merger := &LoserTreeChunkMerger{
		responses: responses,
		heap:      make(chunkResponseHeap, 0, len(responses)),
	}

	for i, resp := range responses {
		if len(resp.Series.Chunks) > 0 {
			merger.heap = append(merger.heap, &heapItem{
				response: resp,
				index:    i,
			})
		}
	}
	heap.Init(&merger.heap)

	return merger
}

// MergeToStorePB converts merged chunk responses to storepb.Series format
func (m *LoserTreeChunkMerger) MergeToStorePB() ([]*storepb.Series, error) {
	var result []*storepb.Series

	for len(m.heap) > 0 {
		current := heap.Pop(&m.heap).(*heapItem)
		currentLabels := current.response.Series.Lset

		var sameLabelsResponses []*heapItem
		sameLabelsResponses = append(sameLabelsResponses, current)

		for len(m.heap) > 0 && labels.Equal(m.heap[0].response.Series.Lset, currentLabels) {
			item := heap.Pop(&m.heap).(*heapItem)
			sameLabelsResponses = append(sameLabelsResponses, item)
		}

		storeSeries, err := m.mergeChunksToStoreSeries(currentLabels, sameLabelsResponses)
		if err != nil {
			return nil, err
		}

		result = append(result, storeSeries)
	}

	return result, nil
}

// mergeChunksToStoreSeries combines chunks from multiple shards into a single storepb.Series
func (m *LoserTreeChunkMerger) mergeChunksToStoreSeries(lset labels.Labels, items []*heapItem) (*storepb.Series, error) {
	storeLabels := labelpb.ZLabelSet{Labels: zLabelsFromMetric(lset)}

	var allChunks []chunks.Meta
	for _, item := range items {
		allChunks = append(allChunks, item.response.Series.Chunks...)
	}

	storeChunks, err := m.chunksToStorePB(allChunks)
	if err != nil {
		return nil, err
	}

	return &storepb.Series{
		Labels: storeLabels.Labels,
		Chunks: storeChunks,
	}, nil
}

// chunksToStorePB converts Prometheus chunks.Meta to storepb.AggrChunk format
func (m *LoserTreeChunkMerger) chunksToStorePB(chunkMetas []chunks.Meta) ([]storepb.AggrChunk, error) {
	result := make([]storepb.AggrChunk, 0, len(chunkMetas))

	for _, chunkMeta := range chunkMetas {
		if chunkMeta.Chunk.NumSamples() == 0 {
			continue
		}

		chkBytes := make([]byte, 0)
		enc, bs := chunkMeta.Chunk.Encoding(), chunkMeta.Chunk.Bytes()

		chkBytes = binary.BigEndian.AppendUint32(chkBytes, uint32(enc))
		chkBytes = binary.BigEndian.AppendUint64(chkBytes, encoding.ZigZagEncode(chunkMeta.MinTime))
		chkBytes = binary.BigEndian.AppendUint64(chkBytes, encoding.ZigZagEncode(chunkMeta.MaxTime))
		chkBytes = binary.BigEndian.AppendUint32(chkBytes, uint32(len(bs)))
		chkBytes = append(chkBytes, bs...)

		aggrChunk := storepb.AggrChunk{
			MinTime: chunkMeta.MinTime,
			MaxTime: chunkMeta.MaxTime,
			Raw: &storepb.Chunk{
				Type: storepb.Chunk_Encoding(enc),
				Data: chkBytes,
			},
		}

		result = append(result, aggrChunk)
	}

	return result, nil
}

// heapItem represents an item in the loser tree heap
type heapItem struct {
	response ChunkResponse
	index    int
}

// chunkResponseHeap implements heap.Interface for ChunkResponse
type chunkResponseHeap []*heapItem

func (h chunkResponseHeap) Len() int { return len(h) }

func (h chunkResponseHeap) Less(i, j int) bool {
	return labels.Compare(h[i].response.Series.Lset, h[j].response.Series.Lset) < 0
}

func (h chunkResponseHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *chunkResponseHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}

func (h *chunkResponseHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
