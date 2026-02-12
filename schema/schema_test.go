// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"testing"
	"time"
)

func TestChunkColumnIndexForMint(t *testing.T) {
	for _, tt := range []struct {
		name      string
		queryTime time.Time
		expect    int
	}{
		// ChunkColumnIndexForMint uses absolute UTC hour to determine chunk column.
		// This matches the write side in convert/chunks.go.
		{"01:00 UTC", time.Date(2025, 12, 3, 1, 0, 0, 0, time.UTC), 0},
		{"07:00 UTC", time.Date(2025, 12, 3, 7, 0, 0, 0, time.UTC), 0},
		{"08:00 UTC", time.Date(2025, 12, 3, 8, 0, 0, 0, time.UTC), 1},
		{"08:30 UTC", time.Date(2025, 12, 3, 8, 30, 0, 0, time.UTC), 1},
		{"15:00 UTC", time.Date(2025, 12, 3, 15, 0, 0, 0, time.UTC), 1},
		{"16:00 UTC", time.Date(2025, 12, 3, 16, 0, 0, 0, time.UTC), 2},
		{"23:00 UTC", time.Date(2025, 12, 3, 23, 0, 0, 0, time.UTC), 2},
		{"00:00 UTC (midnight)", time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC), 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := ChunkColumnIndexForMint(tt.queryTime); got != tt.expect {
				t.Errorf("unexpected chunk column index %d, expected %d", got, tt.expect)
			}
		})
	}
}

func TestChunkColumnIndexForMaxt(t *testing.T) {
	for _, tt := range []struct {
		name      string
		queryTime time.Time
		expect    int
	}{
		// Normal cases (not at boundary)
		{"01:00 UTC", time.Date(2025, 12, 3, 1, 0, 0, 0, time.UTC), 0},
		{"07:59 UTC", time.Date(2025, 12, 3, 7, 59, 0, 0, time.UTC), 0},
		{"08:30 UTC", time.Date(2025, 12, 3, 8, 30, 0, 0, time.UTC), 1},
		{"15:59 UTC", time.Date(2025, 12, 3, 15, 59, 0, 0, time.UTC), 1},
		{"16:30 UTC", time.Date(2025, 12, 3, 16, 30, 0, 0, time.UTC), 2},
		{"23:59 UTC", time.Date(2025, 12, 3, 23, 59, 0, 0, time.UTC), 2},

		// Boundary cases - maxt at exact chunk boundary should use previous chunk
		// because maxt is exclusive (data up to but not including maxt)
		{"00:00 UTC (midnight boundary)", time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC), 2}, // Previous day's chunk_2
		{"08:00 UTC (boundary)", time.Date(2025, 12, 3, 8, 0, 0, 0, time.UTC), 0},          // chunk_0, not chunk_1
		{"16:00 UTC (boundary)", time.Date(2025, 12, 3, 16, 0, 0, 0, time.UTC), 1},         // chunk_1, not chunk_2

		// Not exactly at boundary (has milliseconds) - treat normally
		{"00:00:00.001 UTC", time.Date(2025, 12, 3, 0, 0, 0, 1000000, time.UTC), 0},
		{"08:00:00.001 UTC", time.Date(2025, 12, 3, 8, 0, 0, 1000000, time.UTC), 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := ChunkColumnIndexForMaxt(tt.queryTime); got != tt.expect {
				t.Errorf("unexpected chunk column index %d, expected %d", got, tt.expect)
			}
		})
	}
}

func TestChunkColumnIndex_DailyBlockFullDay(t *testing.T) {
	// Test querying a full day (00:00 to 00:00 next day)
	// This is the case that was broken - daily blocks with maxt at midnight
	mint := time.Date(2025, 12, 2, 0, 0, 0, 0, time.UTC)
	maxt := time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC) // Midnight next day

	minCol, _ := ChunkColumnIndexForMint(mint)
	maxCol, _ := ChunkColumnIndexForMaxt(maxt)

	if minCol != 0 {
		t.Errorf("minCol: expected 0, got %d", minCol)
	}
	if maxCol != 2 {
		t.Errorf("maxCol: expected 2 (full day should include all chunks), got %d", maxCol)
	}
}

func TestChunkColumnIndex_PartitionBlocks(t *testing.T) {
	tests := []struct {
		name           string
		mint           time.Time
		maxt           time.Time
		expectedMinCol int
		expectedMaxCol int
	}{
		{
			name:           "partition 08-10",
			mint:           time.Date(2025, 12, 3, 8, 0, 0, 0, time.UTC),
			maxt:           time.Date(2025, 12, 3, 10, 0, 0, 0, time.UTC),
			expectedMinCol: 1,
			expectedMaxCol: 1,
		},
		{
			name:           "partition 16-18",
			mint:           time.Date(2025, 12, 3, 16, 0, 0, 0, time.UTC),
			maxt:           time.Date(2025, 12, 3, 18, 0, 0, 0, time.UTC),
			expectedMinCol: 2,
			expectedMaxCol: 2,
		},
		{
			name:           "partition 00-02",
			mint:           time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC),
			maxt:           time.Date(2025, 12, 3, 2, 0, 0, 0, time.UTC),
			expectedMinCol: 0,
			expectedMaxCol: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			minCol, _ := ChunkColumnIndexForMint(tc.mint)
			maxCol, _ := ChunkColumnIndexForMaxt(tc.maxt)

			if minCol != tc.expectedMinCol {
				t.Errorf("minCol: expected %d, got %d", tc.expectedMinCol, minCol)
			}
			if maxCol != tc.expectedMaxCol {
				t.Errorf("maxCol: expected %d, got %d", tc.expectedMaxCol, maxCol)
			}
		})
	}
}
