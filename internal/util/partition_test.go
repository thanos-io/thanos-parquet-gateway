// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import (
	"testing"
	"time"

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
			name:     "daily partition",
			start:    time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC),
			duration: 24 * time.Hour,
			expected: "2025/01/19",
		},
		{
			name:     "2h partition start of day",
			start:    time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC),
			duration: 2 * time.Hour,
			expected: "2025/01/19/parts/00-02",
		},
		{
			name:     "2h partition end of day",
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
			name:     "8h partition",
			start:    time.Date(2025, 1, 19, 16, 0, 0, 0, time.UTC),
			duration: 8 * time.Hour,
			expected: "2025/01/19/parts/16-24",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPartition(tt.start, tt.duration)
			require.Equal(t, tt.expected, p.String())
		})
	}
}

func TestPartitionFromString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantErr  bool
		expected Partition
	}{
		{
			name:     "daily format",
			input:    "2025/01/19",
			expected: NewPartition(time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC), 24*time.Hour),
		},
		{
			name:     "2h partition start of day",
			input:    "2025/01/19/parts/00-02",
			expected: NewPartition(time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC), 2*time.Hour),
		},
		{
			name:     "2h partition end of day",
			input:    "2025/01/19/parts/22-24",
			expected: NewPartition(time.Date(2025, 1, 19, 22, 0, 0, 0, time.UTC), 2*time.Hour),
		},
		{
			name:     "4h partition",
			input:    "2025/01/19/parts/08-12",
			expected: NewPartition(time.Date(2025, 1, 19, 8, 0, 0, 0, time.UTC), 4*time.Hour),
		},
		{
			name:    "invalid format",
			input:   "invalid",
			wantErr: true,
		},
		{
			name:    "invalid hour range",
			input:   "2025/01/19/parts/12-08",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := PartitionFromString(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected.String(), p.String())
			require.Equal(t, tt.expected.MinT(), p.MinT())
			require.Equal(t, tt.expected.MaxT(), p.MaxT())
		})
	}
}

func TestSplitIntoPartitions(t *testing.T) {
	day := time.Date(2025, 1, 19, 0, 0, 0, 0, time.UTC)
	mint := day.UnixMilli()
	maxt := day.Add(24 * time.Hour).UnixMilli()

	t.Run("2h partitions", func(t *testing.T) {
		partitions := SplitIntoPartitions(mint, maxt, 2*time.Hour)
		require.Len(t, partitions, 12) // 24h / 2h = 12 partitions

		require.Equal(t, "2025/01/19/parts/00-02", partitions[0].String())
		require.Equal(t, "2025/01/19/parts/02-04", partitions[1].String())
		require.Equal(t, "2025/01/19/parts/22-24", partitions[11].String())
	})

	t.Run("4h partitions", func(t *testing.T) {
		partitions := SplitIntoPartitions(mint, maxt, 4*time.Hour)
		require.Len(t, partitions, 6) // 24h / 4h = 6 partitions

		require.Equal(t, "2025/01/19/parts/00-04", partitions[0].String())
		require.Equal(t, "2025/01/19/parts/20-24", partitions[5].String())
	})

	t.Run("24h partitions", func(t *testing.T) {
		partitions := SplitIntoPartitions(mint, maxt, 24*time.Hour)
		require.Len(t, partitions, 1)
		require.Equal(t, "2025/01/19", partitions[0].String())
	})

	t.Run("partial day coverage", func(t *testing.T) {
		// Start at 10:00, end at 18:00
		partialMint := day.Add(10 * time.Hour).UnixMilli()
		partialMaxt := day.Add(18 * time.Hour).UnixMilli()
		partitions := SplitIntoPartitions(partialMint, partialMaxt, 2*time.Hour)
		require.Len(t, partitions, 4) // 10-12, 12-14, 14-16, 16-18

		require.Equal(t, "2025/01/19/parts/10-12", partitions[0].String())
		require.Equal(t, "2025/01/19/parts/16-18", partitions[3].String())
	})
}

func TestPartition_Contains(t *testing.T) {
	p := NewPartition(time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC), 2*time.Hour)

	// Test timestamps within the partition
	require.True(t, p.Contains(time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC).UnixMilli()))
	require.True(t, p.Contains(time.Date(2025, 1, 19, 11, 30, 0, 0, time.UTC).UnixMilli()))

	// Test boundary - start is inclusive
	require.True(t, p.Contains(time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC).UnixMilli()))

	// Test boundary - end is exclusive
	require.False(t, p.Contains(time.Date(2025, 1, 19, 12, 0, 0, 0, time.UTC).UnixMilli()))

	// Test timestamps outside the partition
	require.False(t, p.Contains(time.Date(2025, 1, 19, 9, 59, 59, 0, time.UTC).UnixMilli()))
	require.False(t, p.Contains(time.Date(2025, 1, 19, 12, 0, 1, 0, time.UTC).UnixMilli()))
}

func TestNewPartitionFromTimestamp(t *testing.T) {
	ts := time.Date(2025, 1, 19, 10, 30, 0, 0, time.UTC).UnixMilli()
	p := NewPartitionFromTimestamp(ts, 2*time.Hour)

	// Should be truncated to 10:00
	require.Equal(t, "2025/01/19/parts/10-12", p.String())
	require.Equal(t, time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC), p.Start())
}

func TestPartition_MinTMaxT(t *testing.T) {
	p := NewPartition(time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC), 2*time.Hour)

	require.Equal(t, time.Date(2025, 1, 19, 10, 0, 0, 0, time.UTC).UnixMilli(), p.MinT())
	require.Equal(t, time.Date(2025, 1, 19, 12, 0, 0, 0, time.UTC).UnixMilli(), p.MaxT())
}
