// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolver_ResolveBlocks(t *testing.T) {
	tests := []struct {
		name       string
		files      map[string][]byte // files in bucket
		start      time.Time
		end        time.Time
		wantBlocks []string
	}{
		{
			name: "single day with data",
			files: map[string][]byte{
				"2025/12/01/meta.pb":          {},
				"2025/12/01/0.labels.parquet": {},
			},
			start:      time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:        time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			wantBlocks: []string{"2025/12/01"},
		},
		{
			name: "date range with gaps",
			files: map[string][]byte{
				"2025/12/01/meta.pb": {},
				"2025/12/03/meta.pb": {},
				// Note: 2025/12/02 is missing
			},
			start:      time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:        time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC),
			wantBlocks: []string{"2025/12/01", "2025/12/03"},
		},
		{
			name: "multiple consecutive days",
			files: map[string][]byte{
				"2025/12/01/meta.pb": {},
				"2025/12/02/meta.pb": {},
				"2025/12/03/meta.pb": {},
			},
			start:      time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:        time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC),
			wantBlocks: []string{"2025/12/01", "2025/12/02", "2025/12/03"},
		},
		{
			name:       "no data in range",
			files:      map[string][]byte{},
			start:      time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:        time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC),
			wantBlocks: nil,
		},
		{
			name: "cross-month boundary",
			files: map[string][]byte{
				"2025/11/30/meta.pb": {},
				"2025/12/01/meta.pb": {},
			},
			start:      time.Date(2025, 11, 30, 0, 0, 0, 0, time.UTC),
			end:        time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			wantBlocks: []string{"2025/11/30", "2025/12/01"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := newMockBucket()
			maps.Copy(bucket.files, tt.files)

			resolver := NewBlockResolver(bucket)
			blocks, err := resolver.ResolveBlocks(context.Background(), tt.start, tt.end)

			require.NoError(t, err)
			assert.Equal(t, tt.wantBlocks, blocks)
		})
	}
}

func TestResolver_CountShards(t *testing.T) {
	tests := []struct {
		name       string
		files      map[string][]byte
		blockName  string
		wantShards int
	}{
		{
			name: "single shard",
			files: map[string][]byte{
				"2025/12/01/0.labels.parquet": {},
			},
			blockName:  "2025/12/01",
			wantShards: 1,
		},
		{
			name: "multiple shards",
			files: map[string][]byte{
				"2025/12/01/0.labels.parquet": {},
				"2025/12/01/1.labels.parquet": {},
				"2025/12/01/2.labels.parquet": {},
			},
			blockName:  "2025/12/01",
			wantShards: 3,
		},
		{
			name: "mixed files (should only count labels.parquet)",
			files: map[string][]byte{
				"2025/12/01/0.labels.parquet": {},
				"2025/12/01/0.chunks.parquet": {},
				"2025/12/01/meta.pb":          {},
			},
			blockName:  "2025/12/01",
			wantShards: 1,
		},
		{
			name:       "empty block",
			files:      map[string][]byte{},
			blockName:  "2025/12/01",
			wantShards: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := newMockBucket()
			maps.Copy(bucket.files, tt.files)

			resolver := NewBlockResolver(bucket)
			count, err := resolver.CountShards(context.Background(), tt.blockName)

			require.NoError(t, err)
			assert.Equal(t, tt.wantShards, count)
		})
	}
}

func TestResolver_ContextCancellation(t *testing.T) {
	bucket := newMockBucket()
	// Add many files to simulate long operation
	for i := range 365 {
		day := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i)
		path := day.Format("2006/01/02") + "/meta.pb"
		bucket.files[path] = []byte{}
	}

	resolver := NewBlockResolver(bucket)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := resolver.ResolveBlocks(ctx,
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
	)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}
