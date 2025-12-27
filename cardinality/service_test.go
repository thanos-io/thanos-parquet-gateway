// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos-parquet-gateway/cardinality/testutil"
)

// mockBlockResolver is a test double for BlockResolver.
type mockBlockResolver struct {
	blocks []string
}

func (m *mockBlockResolver) ResolveBlocks(_ context.Context, _, _ time.Time) ([]string, error) {
	return m.blocks, nil
}

func (m *mockBlockResolver) CountShards(_ context.Context, _ string) (int, error) {
	return 1, nil
}

func (m *mockBlockResolver) ListShardPaths(_ context.Context, blockName string) ([]string, error) {
	return []string{blockName + "/0.labels.parquet"}, nil
}

func TestService_GetCardinality(t *testing.T) {
	tests := []struct {
		name                   string
		setupBlocks            map[string]map[string]int // blockPath -> metrics
		start                  time.Time
		end                    time.Time
		limit                  int
		wantDaysLen            int    // number of days in result
		wantTotalMetrics       int    // total metrics across all days
		wantFirstDayMetricsLen int    // number of metrics in first day
		wantFirstName          string // first metric name (top metric of first day)
		wantFirstCount         int64
		wantBlocksCount        int
		wantErr                error
	}{
		{
			name: "single day single shard",
			setupBlocks: map[string]map[string]int{
				"2025/12/01": {"metric_a": 100, "metric_b": 50},
			},
			start:                  time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:                    time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			limit:                  10,
			wantDaysLen:            1,
			wantTotalMetrics:       2,
			wantFirstDayMetricsLen: 2,
			wantFirstName:          "metric_a",
			wantFirstCount:         100,
			wantBlocksCount:        1,
		},
		{
			name: "multiple days returns per-day metrics",
			setupBlocks: map[string]map[string]int{
				"2025/12/01": {"metric_a": 100},
				"2025/12/02": {"metric_a": 150, "metric_b": 50},
			},
			start:                  time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:                    time.Date(2025, 12, 2, 0, 0, 0, 0, time.UTC),
			limit:                  10,
			wantDaysLen:            2,
			wantTotalMetrics:       3, // 1 metric for day1, 2 metrics for day2
			wantFirstDayMetricsLen: 1,
			wantBlocksCount:        2,
		},
		{
			name: "limit applied per day",
			setupBlocks: map[string]map[string]int{
				"2025/12/01": {
					"metric_a": 100,
					"metric_b": 90,
					"metric_c": 80,
					"metric_d": 70,
					"metric_e": 60,
				},
			},
			start:                  time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:                    time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			limit:                  3,
			wantDaysLen:            1,
			wantTotalMetrics:       3, // limit=3, so only top 3 metrics per day
			wantFirstDayMetricsLen: 3,
			wantFirstName:          "metric_a",
			wantFirstCount:         100,
			wantBlocksCount:        1,
		},
		{
			name:             "no data in range returns empty result",
			setupBlocks:      map[string]map[string]int{},
			start:            time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:              time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC),
			limit:            10,
			wantDaysLen:      0, // no blocks available, empty result
			wantTotalMetrics: 0,
			wantBlocksCount:  0,
		},
		{
			name: "missing days in range are filled with empty metrics",
			setupBlocks: map[string]map[string]int{
				"2025/12/01": {"metric_a": 100},
				"2025/12/03": {"metric_b": 200}, // 12/02 is missing
			},
			start:                  time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:                    time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC),
			limit:                  10,
			wantDaysLen:            3, // all 3 days should be present
			wantTotalMetrics:       2, // only 2 metrics total
			wantFirstDayMetricsLen: 1,
			wantFirstName:          "metric_a",
			wantFirstCount:         100,
			wantBlocksCount:        2,
		},
		{
			name: "results sorted by count descending within each day",
			setupBlocks: map[string]map[string]int{
				"2025/12/01": {
					"lowest":  10,
					"middle":  50,
					"highest": 100,
				},
			},
			start:                  time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:                    time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			limit:                  10,
			wantDaysLen:            1,
			wantTotalMetrics:       3,
			wantFirstDayMetricsLen: 3,
			wantFirstName:          "highest",
			wantFirstCount:         100,
			wantBlocksCount:        1,
		},
		{
			name: "days are sorted chronologically",
			setupBlocks: map[string]map[string]int{
				"2025/12/03": {"metric_c": 100},
				"2025/12/01": {"metric_a": 100},
				"2025/12/02": {"metric_b": 100},
			},
			start:                  time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			end:                    time.Date(2025, 12, 3, 0, 0, 0, 0, time.UTC),
			limit:                  10,
			wantDaysLen:            3,
			wantTotalMetrics:       3,
			wantFirstDayMetricsLen: 1,
			wantFirstName:          "metric_a", // first day (12/01) should have metric_a
			wantFirstCount:         100,
			wantBlocksCount:        3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test directory
			tmpDir := t.TempDir()
			bucket := newMockBucket()

			// Create test blocks
			for blockName, metrics := range tt.setupBlocks {
				testutil.SetupTestBlock(t, tmpDir, blockName, 1, metrics)

				// Add to mock bucket
				metaPath := blockName + "/meta.pb"
				bucket.files[metaPath] = []byte{}

				shardPath := blockName + "/0.labels.parquet"
				content, err := os.ReadFile(filepath.Join(tmpDir, shardPath))
				require.NoError(t, err)
				bucket.files[shardPath] = content
			}

			// Create service
			cacheDir := filepath.Join(tmpDir, "cache")
			svc, err := NewService(bucket, cacheDir)
			require.NoError(t, err)
			defer svc.Close()

			// Execute
			result, err := svc.GetMetricsCardinality(context.Background(), tt.start, tt.end, tt.limit, nil)

			// Assert
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Len(t, result.Days, tt.wantDaysLen)
			assert.Equal(t, tt.wantBlocksCount, result.BlocksAnalyzed)

			// Count total metrics across all days
			var totalMetrics int
			for _, day := range result.Days {
				totalMetrics += len(day.Metrics)
			}
			assert.Equal(t, tt.wantTotalMetrics, totalMetrics)

			if tt.wantDaysLen > 0 {
				// Check first day has expected number of metrics
				assert.Len(t, result.Days[0].Metrics, tt.wantFirstDayMetricsLen)

				// Check first metric of first day
				if tt.wantFirstName != "" && tt.wantFirstDayMetricsLen > 0 {
					assert.Equal(t, tt.wantFirstName, result.Days[0].Metrics[0].MetricName)
					assert.Equal(t, tt.wantFirstCount, result.Days[0].Metrics[0].SeriesCount)
				}

				// Verify each metric has percentage > 0
				for _, day := range result.Days {
					assert.NotEmpty(t, day.Date, "day should have a date")
					for _, m := range day.Metrics {
						assert.Greater(t, m.Percentage, float64(0), "percentage should be > 0")
					}
				}

				// Verify days are sorted chronologically
				for i := 1; i < len(result.Days); i++ {
					assert.True(t, result.Days[i-1].Date < result.Days[i].Date,
						"days should be sorted chronologically: %s should be before %s",
						result.Days[i-1].Date, result.Days[i].Date)
				}
			}
		})
	}
}

func TestStartOfDay(t *testing.T) {
	tests := []struct {
		name  string
		input time.Time
		want  time.Time
	}{
		{
			name:  "middle of day",
			input: time.Date(2025, 12, 1, 14, 30, 45, 123456789, time.UTC),
			want:  time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "already start of day",
			input: time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := startOfDay(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPreloaderConfig(t *testing.T) {
	t.Parallel()

	// Test default values through WithPreloader option
	svc := &serviceImpl{}
	opt := WithPreloader(PreloaderConfig{})
	opt(svc)

	if !svc.preloaderEnabled {
		t.Error("expected preloader to be enabled")
	}
	if svc.preloaderDays != DefaultPreloadDays {
		t.Errorf("expected default days=%d, got %d", DefaultPreloadDays, svc.preloaderDays)
	}
	if svc.preloaderConcurrency != DefaultPreloadConcurrency {
		t.Errorf("expected default concurrency=%d, got %d", DefaultPreloadConcurrency, svc.preloaderConcurrency)
	}
	if svc.preloaderInterval != DefaultPreloadInterval {
		t.Errorf("expected default interval=%v, got %v", DefaultPreloadInterval, svc.preloaderInterval)
	}

	// Test with custom values
	svc = &serviceImpl{}
	opt = WithPreloader(PreloaderConfig{
		Days:        14,
		Concurrency: 8,
		Interval:    30 * time.Minute,
	})
	opt(svc)

	if svc.preloaderDays != 14 {
		t.Errorf("expected days=14, got %d", svc.preloaderDays)
	}
	if svc.preloaderConcurrency != 8 {
		t.Errorf("expected concurrency=8, got %d", svc.preloaderConcurrency)
	}
	if svc.preloaderInterval != 30*time.Minute {
		t.Errorf("expected interval=30m, got %v", svc.preloaderInterval)
	}
}

func TestPreloaderStartStop(t *testing.T) {
	t.Parallel()

	// Create a serviceImpl with preloader enabled but no dependencies
	// This tests the start/stop lifecycle without actual preloading
	svc := &serviceImpl{
		preloaderEnabled:     true,
		preloaderDays:        1,
		preloaderConcurrency: 1,
		preloaderInterval:    time.Hour,
		blockResolver:        &mockBlockResolver{blocks: []string{}},
	}

	// stopPreloader should handle nil cancel/done gracefully
	svc.stopPreloader()

	// After stop, should be safe to call again (idempotent)
	svc.stopPreloader()
}
