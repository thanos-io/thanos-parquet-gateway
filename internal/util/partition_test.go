// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartition_String(t *testing.T) {
	tests := []struct {
		name     string
		start    time.Time
		duration time.Duration
		expected string
	}{
		{
			name:     "24h partition - daily format",
			start:    time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC),
			duration: 24 * time.Hour,
			expected: "2025/01/19",
		},
		{
			name:     "2h partition - first of day",
			start:    time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC),
			duration: 2 * time.Hour,
			expected: "2025/01/19/parts/00-02",
		},
		{
			name:     "2h partition - middle of day",
			start:    time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC),
			duration: 2 * time.Hour,
			expected: "2025/01/19/parts/10-12",
		},
		{
			name:     "2h partition - ends at midnight",
			start:    time.Date(2025, 1, 19, 22, 0, 0, 0, time.UTC),
			duration: 2 * time.Hour,
			expected: "2025/01/19/parts/22-24",
		},
		{
			name:     "4h partition",
			start:    time.Date(2025, 1, 19, 8, 0, 0, 0, time.UTC),
			duration: 4 * time.Hour,
			expected: "2025/01/19/parts/08-12",
		},
		{
			name:     "8h partition - first",
			start:    time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC),
			duration: 8 * time.Hour,
			expected: "2025/01/19/parts/00-08",
		},
		{
			name:     "8h partition - last",
			start:    time.Date(2025, 1, 19, 16, 0, 0, 0, time.UTC),
			duration: 8 * time.Hour,
			expected: "2025/01/19/parts/16-24",
		},
		{
			name:     "6h partition",
			start:    time.Date(2025, 1, 19, 6, 0, 0, 0, time.UTC),
			duration: 6 * time.Hour,
			expected: "2025/01/19/parts/06-12",
		},
		{
			name:     "1h partition",
			start:    time.Date(2025, 1, 19, 5, 0, 0, 0, time.UTC),
			duration: 1 * time.Hour,
			expected: "2025/01/19/parts/05-06",
		},
		{
			name:     "12h partition - ends at midnight",
			start:    time.Date(2025, 1, 19, 12, 0, 0, 0, time.UTC),
			duration: 12 * time.Hour,
			expected: "2025/01/19/parts/12-24",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPartition(tc.start, tc.duration)
			assert.Equal(t, tc.expected, p.String())
		})
	}
}

func TestPartitionFromString(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectedStart    time.Time
		expectedDuration time.Duration
		expectError      bool
	}{
		{
			name:             "daily partition format",
			input:            "2025/01/19",
			expectedStart:    time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC),
			expectedDuration: 24 * time.Hour,
		},
		{
			name:             "2h partition - first of day",
			input:            "2025/01/19/parts/00-02",
			expectedStart:    time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC),
			expectedDuration: 2 * time.Hour,
		},
		{
			name:             "2h partition - middle of day",
			input:            "2025/01/19/parts/10-12",
			expectedStart:    time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC),
			expectedDuration: 2 * time.Hour,
		},
		{
			name:             "2h partition - ends at midnight",
			input:            "2025/01/19/parts/22-24",
			expectedStart:    time.Date(2025, 1, 19, 22, 0, 0, 0, time.UTC),
			expectedDuration: 2 * time.Hour,
		},
		{
			name:             "8h partition",
			input:            "2025/01/19/parts/00-08",
			expectedStart:    time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC),
			expectedDuration: 8 * time.Hour,
		},
		{
			name:             "4h partition",
			input:            "2025/01/19/parts/08-12",
			expectedStart:    time.Date(2025, 1, 19, 8, 0, 0, 0, time.UTC),
			expectedDuration: 4 * time.Hour,
		},
		{
			name:        "invalid format - too short",
			input:       "2025/01",
			expectError: true,
		},
		{
			name:        "invalid format - bad hours",
			input:       "2025/01/19/parts/xx-yy",
			expectError: true,
		},
		{
			name:        "invalid format - negative duration",
			input:       "2025/01/19/parts/12-08",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := PartitionFromString(tc.input)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedStart.UTC(), p.Start().UTC())
			assert.Equal(t, tc.expectedDuration, p.Duration())
		})
	}
}

func TestPartition_RoundTrip(t *testing.T) {
	// Test that String() and PartitionFromString() are inverse operations
	tests := []struct {
		name     string
		start    time.Time
		duration time.Duration
	}{
		{
			name:     "24h partition",
			start:    time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC),
			duration: 24 * time.Hour,
		},
		{
			name:     "2h partition",
			start:    time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC),
			duration: 2 * time.Hour,
		},
		{
			name:     "8h partition ending at midnight",
			start:    time.Date(2025, 1, 19, 16, 0, 0, 0, time.UTC),
			duration: 8 * time.Hour,
		},
		{
			name:     "1h partition",
			start:    time.Date(2025, 1, 19, 23, 0, 0, 0, time.UTC),
			duration: 1 * time.Hour,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			original := NewPartition(tc.start, tc.duration)
			str := original.String()

			parsed, err := PartitionFromString(str)
			require.NoError(t, err)

			assert.Equal(t, original.Start().UTC(), parsed.Start().UTC())
			assert.Equal(t, original.Duration(), parsed.Duration())
			assert.Equal(t, original.MinT(), parsed.MinT())
			assert.Equal(t, original.MaxT(), parsed.MaxT())
		})
	}
}

func TestNewPartition(t *testing.T) {
	// Verify that NewPartition truncates start time to partition boundary
	start := time.Date(2025, 1, 19, 3, 30, 45, 123, time.UTC)

	p2h := NewPartition(start, 2*time.Hour)
	assert.Equal(t, time.Date(2025, 1, 19, 2, 0, 0, 0, time.UTC), p2h.Start())

	p8h := NewPartition(start, 8*time.Hour)
	assert.Equal(t, time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC), p8h.Start())

	p24h := NewPartition(start, 24*time.Hour)
	assert.Equal(t, time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC), p24h.Start())
}

func TestPartition_Contains(t *testing.T) {
	p := NewPartition(time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC), 2*time.Hour)

	// Exactly at start - should be included
	assert.True(t, p.Contains(time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC).UnixMilli()))

	// In the middle
	assert.True(t, p.Contains(time.Date(2025, 1, 19, 11, 0, 0, 0, time.UTC).UnixMilli()))

	// Just before end
	assert.True(t, p.Contains(time.Date(2025, 1, 19, 11, 59, 59, 999000000, time.UTC).UnixMilli()))

	// Exactly at end - should NOT be included (half-open interval)
	assert.False(t, p.Contains(time.Date(2025, 1, 19, 12, 0, 0, 0, time.UTC).UnixMilli()))

	// Before start
	assert.False(t, p.Contains(time.Date(2025, 1, 19, 9, 59, 59, 999000000, time.UTC).UnixMilli()))

	// After end
	assert.False(t, p.Contains(time.Date(2025, 1, 19, 12, 0, 0, 1000000, time.UTC).UnixMilli()))
}

func TestSplitIntoPartitions(t *testing.T) {
	tests := []struct {
		name             string
		mint             int64
		maxt             int64
		duration         time.Duration
		expectedCount    int
		expectedFirstStr string
		expectedLastStr  string
	}{
		{
			name:             "one day into 2h partitions",
			mint:             time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC).UnixMilli(),
			maxt:             time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC).UnixMilli(),
			duration:         2 * time.Hour,
			expectedCount:    12,
			expectedFirstStr: "2025/01/19/parts/00-02",
			expectedLastStr:  "2025/01/19/parts/22-24",
		},
		{
			name:             "one day into 8h partitions",
			mint:             time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC).UnixMilli(),
			maxt:             time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC).UnixMilli(),
			duration:         8 * time.Hour,
			expectedCount:    3,
			expectedFirstStr: "2025/01/19/parts/00-08",
			expectedLastStr:  "2025/01/19/parts/16-24",
		},
		{
			name:             "partial day - 6 hours into 2h partitions",
			mint:             time.Date(2025, 1, 19, 4, 0, 0, 0, time.UTC).UnixMilli(),
			maxt:             time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC).UnixMilli(),
			duration:         2 * time.Hour,
			expectedCount:    3,
			expectedFirstStr: "2025/01/19/parts/04-06",
			expectedLastStr:  "2025/01/19/parts/08-10",
		},
		{
			name:             "multi-day into daily partitions",
			mint:             time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC).UnixMilli(),
			maxt:             time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC).UnixMilli(),
			duration:         24 * time.Hour,
			expectedCount:    3,
			expectedFirstStr: "2025/01/17",
			expectedLastStr:  "2025/01/19",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			partitions := SplitIntoPartitions(tc.mint, tc.maxt, tc.duration)

			assert.Len(t, partitions, tc.expectedCount)
			if tc.expectedCount > 0 {
				assert.Equal(t, tc.expectedFirstStr, partitions[0].String())
				assert.Equal(t, tc.expectedLastStr, partitions[len(partitions)-1].String())
			}
		})
	}
}

func TestPartition_TimeRange(t *testing.T) {
	p := NewPartition(time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC), 2*time.Hour)

	assert.Equal(t, time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC), p.Start())
	assert.Equal(t, time.Date(2025, 1, 19, 12, 0, 0, 0, time.UTC), p.End())
	assert.Equal(t, time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC).UnixMilli(), p.MinT())
	assert.Equal(t, time.Date(2025, 1, 19, 12, 0, 0, 0, time.UTC).UnixMilli(), p.MaxT())
}

func TestDateFromString(t *testing.T) {
	tests := []struct {
		input     string
		expectErr bool
		expected  Date
	}{
		{"2025/12/02", false, NewDate(2025, time.December, 2)},
		{"2025/01/15", false, NewDate(2025, time.January, 15)},
		{"1970/01/01", false, NewDate(1970, time.January, 1)},
		{"invalid", true, Date{}},
		{"2025-12-02", true, Date{}}, // Wrong format
		{"", true, Date{}},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			d, err := DateFromString(tc.input)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected.String(), d.String())
			}
		})
	}
}
