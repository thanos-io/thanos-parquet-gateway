// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

func TestFilterOverlappingPartitions(t *testing.T) {
	tests := []struct {
		name     string
		blocks   []string // block names
		expected []string // expected block names after filtering
	}{
		{
			name:     "no blocks",
			blocks:   []string{},
			expected: []string{},
		},
		{
			name:     "only daily blocks",
			blocks:   []string{"2025/12/01", "2025/12/02"},
			expected: []string{"2025/12/01", "2025/12/02"},
		},
		{
			name:     "only partition blocks",
			blocks:   []string{"2025/12/02/parts/00-02", "2025/12/02/parts/02-04"},
			expected: []string{"2025/12/02/parts/00-02", "2025/12/02/parts/02-04"},
		},
		{
			name: "daily block with overlapping partitions - partitions filtered out",
			blocks: []string{
				"2025/12/02",
				"2025/12/02/parts/00-02",
				"2025/12/02/parts/02-04",
				"2025/12/02/parts/04-06",
			},
			expected: []string{"2025/12/02"},
		},
		{
			name: "mixed: daily for one date, partitions for another",
			blocks: []string{
				"2025/12/01",             // daily
				"2025/12/02/parts/00-02", // partition for different date
				"2025/12/02/parts/02-04",
			},
			expected: []string{
				"2025/12/01",
				"2025/12/02/parts/00-02",
				"2025/12/02/parts/02-04",
			},
		},
		{
			name: "complex: multiple days with mixed blocks",
			blocks: []string{
				"2025/12/01",             // daily
				"2025/12/02",             // daily - should filter its partitions
				"2025/12/02/parts/00-02", // filtered (daily exists)
				"2025/12/02/parts/02-04", // filtered (daily exists)
				"2025/12/03/parts/00-02", // kept (no daily for 12/03)
				"2025/12/03/parts/02-04", // kept (no daily for 12/03)
			},
			expected: []string{
				"2025/12/01",
				"2025/12/02",
				"2025/12/03/parts/00-02",
				"2025/12/03/parts/02-04",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create mock blocks
			blocks := make([]*Block, len(tc.blocks))
			for i, name := range tc.blocks {
				blocks[i] = &Block{
					meta: schema.Meta{Name: name},
				}
			}

			// Filter
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			result := filterOverlappingPartitions(blocks, log)

			// Extract names for comparison
			resultNames := make([]string, len(result))
			for i, blk := range result {
				resultNames[i] = blk.Meta().Name
			}

			assert.Equal(t, tc.expected, resultNames)
		})
	}
}
