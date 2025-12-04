// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/thanos/pkg/compact/downsample"

	"github.com/thanos-io/thanos-parquet-gateway/internal/encoding"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

func collectChunks(it chunks.Iterator) ([schema.ChunkColumnsPerDay][]byte, error) {
	var (
		res [schema.ChunkColumnsPerDay][]byte
	)
	// NOTE: 'it' should hold chunks for one day. Chunks are usually length 2h so we should get 12 of them.
	chunks := make([]chunks.Meta, 0, 12)
	for it.Next() {
		chunks = append(chunks, it.At())
	}
	if err := it.Err(); err != nil {
		return res, fmt.Errorf("unable to iterate chunks: %w", err)
	}
	// NOTE: we need to sort chunks here as they come from different blocks that we merged.
	// Prometheus does not guarantee that they are sorted. We have to sort them either here or
	// before submitting them to the query engine.
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].MinTime < chunks[j].MinTime
	})
	for _, chk := range chunks {
		if chk.Chunk.NumSamples() == 0 {
			continue
		}
		enc, bs := chk.Chunk.Encoding(), chk.Chunk.Bytes()
		hour := time.UnixMilli(chk.MinTime).UTC().Hour()
		chkIdx := (hour / int(schema.ChunkColumnLength.Hours())) % schema.ChunkColumnsPerDay
		chkBytes := res[chkIdx]
		chkBytes = binary.BigEndian.AppendUint32(chkBytes, uint32(enc))
		chkBytes = binary.BigEndian.AppendUint64(chkBytes, encoding.ZigZagEncode(chk.MinTime))
		chkBytes = binary.BigEndian.AppendUint64(chkBytes, encoding.ZigZagEncode(chk.MaxTime))
		chkBytes = binary.BigEndian.AppendUint32(chkBytes, uint32(len(bs)))
		chkBytes = append(chkBytes, bs...)
		res[chkIdx] = chkBytes
	}
	return res, nil
}

type sample struct {
	t int64
	v float64
}

// expandXorChunkIterator reads all samples from the iterator and appends them to buf.
// Stale markers and out of order samples are skipped.
// Copied from pkg/compact/downsample/downsample.go in thanos.
func expandXorChunkIterator(it chunkenc.Iterator, buf *[]sample) error {
	// For safety reasons, we check for each sample that it does not go back in time.
	// If it does, we skip it.
	lastT := int64(0)
	for it.Next() != chunkenc.ValNone {
		t, v := it.At()
		if value.IsStaleNaN(v) {
			continue
		}
		if t >= lastT {
			*buf = append(*buf, sample{t: t, v: v})
			lastT = t
		}
	}
	return it.Err()
}

type aggregateBytes [5][]byte

// Returns collected downsampled chunks: count, sum, min, max, counter.
func collectDownsampledChunks(it chunks.Iterator) (aggregateBytes, error) {
	var (
		temp [5][]sample
	)
	res := aggregateBytes{}
	chunks := make([]chunks.Meta, 0, 12)
	for it.Next() {
		chunks = append(chunks, it.At())
	}
	if err := it.Err(); err != nil {
		return res, fmt.Errorf("unable to iterate chunks: %w", err)
	}
	// NOTE: we need to sort chunks here as they come from different blocks that we merged.
	// Prometheus does not guarantee that they are sorted. We have to sort them either here or
	// before submitting them to the query engine.
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].MinTime < chunks[j].MinTime
	})
	for _, chk := range chunks {
		if chk.Chunk.NumSamples() == 0 {
			continue
		}

		enc, bs := chk.Chunk.Encoding(), chk.Chunk.Bytes()
		if enc != downsample.ChunkEncAggr {
			return res, fmt.Errorf("expected downsample chunk encoding but got %d", enc)
		}

		ac := downsample.AggrChunk(bs)
		for _, at := range []downsample.AggrType{downsample.AggrCount, downsample.AggrSum, downsample.AggrMin, downsample.AggrMax, downsample.AggrCounter} {
			if c, err := ac.Get(at); err != downsample.ErrAggrNotExist {
				if err != nil {
					return res, err
				}
				err = expandXorChunkIterator(c.Iterator(nil), &temp[at])
				if err != nil {
					return res, fmt.Errorf("unable to expand aggregate chunk %d: %w", at, err)
				}
			}
		}
	}

	for _, aggrType := range []downsample.AggrType{downsample.AggrCount, downsample.AggrSum, downsample.AggrMin, downsample.AggrMax, downsample.AggrCounter} {
		samples := temp[aggrType]
		bts := make([]byte, 0)
		if len(samples) == 0 {
			continue
		}
		bts = binary.BigEndian.AppendUint64(bts, uint64(len(samples)))
		for _, s := range samples {
			bts = binary.BigEndian.AppendUint64(bts, uint64(s.t))
			// For the Count and Counter aggregates, we store integer values.
			if aggrType == downsample.AggrCount || aggrType == downsample.AggrCounter {
				bts = binary.BigEndian.AppendUint64(bts, uint64(s.v))
				continue
			}
			// All other aggregates are stored as float64 values.
			bts = binary.BigEndian.AppendUint64(bts, math.Float64bits(s.v))
		}
		res[aggrType] = bts
	}

	return res, nil
}
