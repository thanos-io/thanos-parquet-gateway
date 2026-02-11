// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
)

func TestBlockNameForDay(t *testing.T) {
	t.Run("1970/01/01", func(t *testing.T) {
		b := BlockNameForDay(util.NewDate(1970, time.January, 1))
		want := "1970/01/01"
		got := b
		if want != got {
			t.Fatalf("wanted %q, got %q", want, got)
		}
	})
	t.Run("2024/11/23", func(t *testing.T) {
		b := BlockNameForDay(util.NewDate(2024, time.November, 23))
		want := "2024/11/23"
		got := b
		if want != got {
			t.Fatalf("wanted %q, got %q", want, got)
		}
	})
}

func TestSplitBlockPath(t *testing.T) {
	tests := []struct {
		name             string
		path             string
		expectedDate     util.Date
		expectedFile     string
		expectedHash     ExternalLabelsHash
		expectedOK       bool
	}{
		// Daily formats without hash
		{
			name:         "plain daily - meta.pb",
			path:         "2025/01/19/meta.pb",
			expectedDate: util.NewDate(2025, time.January, 19),
			expectedFile: "meta.pb",
			expectedOK:   true,
		},
		{
			name:         "plain daily - labels parquet",
			path:         "2025/01/19/0.labels.parquet",
			expectedDate: util.NewDate(2025, time.January, 19),
			expectedFile: "0.labels.parquet",
			expectedOK:   true,
		},
		// Daily formats with hash
		{
			name:         "daily with hash - meta.pb",
			path:         "12345/2025/01/19/meta.pb",
			expectedDate: util.NewDate(2025, time.January, 19),
			expectedFile: "meta.pb",
			expectedHash: 12345,
			expectedOK:   true,
		},
		// Partition formats (parts/HH-HH) without hash
		{
			name:         "partition - meta.pb",
			path:         "2025/01/19/parts/00-02/meta.pb",
			expectedDate: util.NewDate(2025, time.January, 19),
			expectedFile: "meta.pb",
			expectedOK:   true,
		},
		{
			name:         "partition - labels parquet",
			path:         "2025/01/19/parts/08-16/0.labels.parquet",
			expectedDate: util.NewDate(2025, time.January, 19),
			expectedFile: "0.labels.parquet",
			expectedOK:   true,
		},
		{
			name:         "partition - ends at midnight",
			path:         "2025/01/19/parts/22-24/meta.pb",
			expectedDate: util.NewDate(2025, time.January, 19),
			expectedFile: "meta.pb",
			expectedOK:   true,
		},
		// Partition formats with hash
		{
			name:         "partition with hash",
			path:         "12345/2025/01/19/parts/08-16/meta.pb",
			expectedDate: util.NewDate(2025, time.January, 19),
			expectedFile: "meta.pb",
			expectedHash: 12345,
			expectedOK:   true,
		},
		// Invalid formats
		{
			name:       "too short path",
			path:       "2025/01/meta.pb",
			expectedOK: false,
		},
		{
			name:       "invalid year",
			path:       "xxxx/01/19/meta.pb",
			expectedOK: false,
		},
		{
			name:       "invalid month",
			path:       "2025/xx/19/meta.pb",
			expectedOK: false,
		},
		{
			name:       "invalid day",
			path:       "2025/01/xx/meta.pb",
			expectedOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			date, file, hash, ok := SplitBlockPath(tc.path)
			assert.Equal(t, tc.expectedOK, ok, "ok mismatch")
			if tc.expectedOK {
				assert.Equal(t, tc.expectedDate, date, "date mismatch")
				assert.Equal(t, tc.expectedFile, file, "file mismatch")
				assert.Equal(t, tc.expectedHash, hash, "hash mismatch")
			}
		})
	}
}

func TestIsHourRangeFormat(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		// Valid formats
		{"00-02", true},
		{"00-08", true},
		{"08-16", true},
		{"16-24", true},
		{"22-24", true},
		{"00-24", true},
		{"01-02", true},

		// Invalid formats
		{"0-02", false},   // Too short
		{"00-2", false},   // Too short
		{"00002", false},  // No dash
		{"00_02", false},  // Wrong separator
		{"24-26", false},  // Start > 23
		{"00-25", false},  // End > 24
		{"00-00", false},  // End must be > start
		{"08-04", false},  // End must be > start
		{"abc123", false}, // Not a number
		{"", false},       // Empty
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := isHourRangeFormat(tc.input)
			assert.Equal(t, tc.expected, got, "input: %s", tc.input)
		})
	}
}

func TestIsPartition(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
	}{
		// Partition blocks without hash
		{"2025/12/02/parts/00-02", true},
		{"2025/12/02/parts/08-10", true},
		{"2025/12/02/parts/22-24", true},
		{"2025/01/01/parts/00-08", true},

		// Partition blocks with hash
		{"12345/2025/12/02/parts/00-02", true},

		// Daily blocks
		{"2025/12/02", false},
		{"2025/01/01", false},
		{"1970/01/01", false},
		{"12345/2025/12/02", false}, // daily with hash

		// Invalid formats
		{"2025/12/02/parts", false},         // Missing hour range
		{"2025/12/02/parts/invalid", false}, // Invalid hour range
		{"2025/12", false},                  // Too short
		{"", false},                         // Empty
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsPartition(tc.name)
			assert.Equal(t, tc.expected, got, "input: %s", tc.name)
		})
	}
}

func TestDailyBlockNameForPartition(t *testing.T) {
	tests := []struct {
		partition string
		expected  string
	}{
		// Without hash
		{"2025/12/02/parts/00-02", "2025/12/02"},
		{"2025/12/02/parts/08-10", "2025/12/02"},
		{"2025/12/02/parts/22-24", "2025/12/02"},
		{"2025/01/15/parts/00-08", "2025/01/15"},
		// With hash
		{"12345/2025/12/02/parts/00-02", "12345/2025/12/02"},
		// Daily blocks return first 3 parts
		{"2025/12/02", "2025/12/02"},
	}

	for _, tc := range tests {
		t.Run(tc.partition, func(t *testing.T) {
			got := DailyBlockNameForPartition(tc.partition)
			assert.Equal(t, tc.expected, got)
		})
	}
}
