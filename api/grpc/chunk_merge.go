// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package grpc

import (
	"github.com/bboreham/go-loser"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/thanos-io/thanos-parquet-gateway/search"
)

// ChunkResponse represents a series with its raw chunk data from a single shard
type ChunkResponse struct {
	Series     search.SeriesChunks
	ShardIndex int
}

type ChunkSequence struct {
	responses []ChunkResponse
	index     int
}

func (cs *ChunkSequence) Next() bool {
	cs.index++
	return cs.index < len(cs.responses)
}

func (cs *ChunkSequence) At() string {
	if cs.index < 0 || cs.index >= len(cs.responses) {
		return "\uffff"
	}
	return cs.responses[cs.index].Series.Lset.String()
}

func NewChunkSequence(responses []ChunkResponse) *ChunkSequence {
	return &ChunkSequence{
		responses: responses,
		index:     -1,
	}
}

// LoserTreeChunkMerger efficiently merges chunk responses from multiple shards
type LoserTreeChunkMerger struct {
	tree *loser.Tree[string, *ChunkSequence]
}

// NewLoserTreeChunkMerger creates a new merger for chunk responses
func NewLoserTreeChunkMerger(responses []ChunkResponse) *LoserTreeChunkMerger {
	if len(responses) == 0 {
		return &LoserTreeChunkMerger{
			tree: nil,
		}
	}

	sequences := make([]*ChunkSequence, len(responses))
	for i, resp := range responses {
		sequences[i] = NewChunkSequence([]ChunkResponse{resp})
	}

	tree := loser.New(sequences, "\uffff")

	return &LoserTreeChunkMerger{
		tree: tree,
	}
}

// MergeToStorePB converts merged chunk responses to storepb.Series format
func (m *LoserTreeChunkMerger) MergeToStorePB() ([]*storepb.Series, error) {
	if m.tree == nil || m.tree.IsEmpty() {
		return []*storepb.Series{}, nil
	}

	seriesMap := make(map[string]*mergedSeries)

	for !m.tree.IsEmpty() {
		winnerSeq := m.tree.Winner()
		if winnerSeq.index >= len(winnerSeq.responses) {
			m.tree.Next()
			continue
		}

		current := winnerSeq.responses[winnerSeq.index].Series
		labelKey := current.Lset.String()

		if existing, ok := seriesMap[labelKey]; ok {
			existing.chunks = append(existing.chunks, current.Chunks...)
		} else {
			seriesMap[labelKey] = &mergedSeries{
				labels: current.Lset,
				chunks: append([]chunks.Meta{}, current.Chunks...),
			}
		}

		m.tree.Next()
	}

	result := make([]*storepb.Series, 0, len(seriesMap))
	for _, merged := range seriesMap {
		storeSeries, err := m.createStoreSeries(merged.labels, merged.chunks)
		if err != nil {
			return nil, err
		}
		result = append(result, storeSeries)
	}

	return result, nil
}

// mergedSeries holds a series with merged chunks from multiple shards
type mergedSeries struct {
	labels labels.Labels
	chunks []chunks.Meta
}

func (m *LoserTreeChunkMerger) createStoreSeries(lset labels.Labels, chunkMetas []chunks.Meta) (*storepb.Series, error) {
	storeLabels := labelpb.ZLabelSet{Labels: zLabelsFromMetric(lset)}

	storeChunks, err := m.chunksToStorePB(chunkMetas)
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

		enc := chunkMeta.Chunk.Encoding()
		rawBytes := chunkMeta.Chunk.Bytes()

		aggrChunk := storepb.AggrChunk{
			MinTime: chunkMeta.MinTime,
			MaxTime: chunkMeta.MaxTime,
			Raw: &storepb.Chunk{
				Type: storepb.Chunk_Encoding(enc),
				Data: rawBytes,
			},
		}

		result = append(result, aggrChunk)
	}

	return result, nil
}
