// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

var testLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestCleanupStalePartitions(t *testing.T) {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.Add(-24 * time.Hour)
	twoDaysAgo := today.Add(-48 * time.Hour)
	threeDaysAgo := today.Add(-72 * time.Hour)

	tests := []struct {
		name             string
		streams          map[schema.ExternalLabelsHash]schema.ParquetBlocksStream
		partitionFiles   []string // files to create in bucket
		expectDeleted    []string // partition dirs that should be empty after cleanup
		expectExists     []string // files that should still exist after cleanup
		cancelContext    bool
		skipDeletedCheck bool // for context cancellation test
	}{
		{
			name: "no daily blocks - no cleanup",
			streams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				0: {Metas: []schema.Meta{
					makePartitionMeta("2025/01/15/parts/00-02", threeDaysAgo),
					makePartitionMeta("2025/01/15/parts/02-04", threeDaysAgo),
				}},
			},
			partitionFiles: []string{
				"2025/01/15/parts/00-02/0.labels.parquet",
				"2025/01/15/parts/02-04/0.labels.parquet",
			},
			expectExists: []string{
				"2025/01/15/parts/00-02/0.labels.parquet",
				"2025/01/15/parts/02-04/0.labels.parquet",
			},
		},
		{
			name: "daily block for today - no cleanup",
			streams: func() map[schema.ExternalLabelsHash]schema.ParquetBlocksStream {
				todayStr := today.Format("2006/01/02")
				return map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
					0: {Metas: []schema.Meta{
						makeDailyMeta(todayStr, today),
						makePartitionMeta(todayStr+"/parts/00-02", today),
						makePartitionMeta(todayStr+"/parts/02-04", today),
					}},
				}
			}(),
			partitionFiles: func() []string {
				todayStr := today.Format("2006/01/02")
				return []string{
					todayStr + "/parts/00-02/0.labels.parquet",
					todayStr + "/parts/02-04/0.labels.parquet",
				}
			}(),
			expectExists: func() []string {
				todayStr := today.Format("2006/01/02")
				return []string{
					todayStr + "/parts/00-02/0.labels.parquet",
					todayStr + "/parts/02-04/0.labels.parquet",
				}
			}(),
		},
		{
			name: "daily block for yesterday - no cleanup",
			streams: func() map[schema.ExternalLabelsHash]schema.ParquetBlocksStream {
				yesterdayStr := yesterday.Format("2006/01/02")
				return map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
					0: {Metas: []schema.Meta{
						makeDailyMeta(yesterdayStr, yesterday),
						makePartitionMeta(yesterdayStr+"/parts/00-02", yesterday),
						makePartitionMeta(yesterdayStr+"/parts/02-04", yesterday),
					}},
				}
			}(),
			partitionFiles: func() []string {
				yesterdayStr := yesterday.Format("2006/01/02")
				return []string{
					yesterdayStr + "/parts/00-02/0.labels.parquet",
					yesterdayStr + "/parts/02-04/0.labels.parquet",
				}
			}(),
			expectExists: func() []string {
				yesterdayStr := yesterday.Format("2006/01/02")
				return []string{
					yesterdayStr + "/parts/00-02/0.labels.parquet",
					yesterdayStr + "/parts/02-04/0.labels.parquet",
				}
			}(),
		},
		{
			name: "daily block for two days ago - cleanup happens",
			streams: func() map[schema.ExternalLabelsHash]schema.ParquetBlocksStream {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				return map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
					0: {Metas: []schema.Meta{
						makeDailyMeta(twoDaysAgoStr, twoDaysAgo),
						makePartitionMeta(twoDaysAgoStr+"/parts/00-02", twoDaysAgo),
						makePartitionMeta(twoDaysAgoStr+"/parts/02-04", twoDaysAgo),
					}},
				}
			}(),
			partitionFiles: func() []string {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				return []string{
					twoDaysAgoStr + "/parts/00-02/0.labels.parquet",
					twoDaysAgoStr + "/parts/02-04/0.labels.parquet",
				}
			}(),
			expectDeleted: func() []string {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				return []string{twoDaysAgoStr + "/parts/"}
			}(),
		},
		{
			name: "multiple days with stale partitions - all get cleaned",
			streams: func() map[schema.ExternalLabelsHash]schema.ParquetBlocksStream {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				threeDaysAgoStr := threeDaysAgo.Format("2006/01/02")
				return map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
					0: {Metas: []schema.Meta{
						makeDailyMeta(twoDaysAgoStr, twoDaysAgo),
						makePartitionMeta(twoDaysAgoStr+"/parts/00-02", twoDaysAgo),
						makeDailyMeta(threeDaysAgoStr, threeDaysAgo),
						makePartitionMeta(threeDaysAgoStr+"/parts/00-02", threeDaysAgo),
						makePartitionMeta(threeDaysAgoStr+"/parts/02-04", threeDaysAgo),
					}},
				}
			}(),
			partitionFiles: func() []string {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				threeDaysAgoStr := threeDaysAgo.Format("2006/01/02")
				return []string{
					twoDaysAgoStr + "/parts/00-02/0.labels.parquet",
					threeDaysAgoStr + "/parts/00-02/0.labels.parquet",
					threeDaysAgoStr + "/parts/02-04/0.labels.parquet",
				}
			}(),
			expectDeleted: func() []string {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				threeDaysAgoStr := threeDaysAgo.Format("2006/01/02")
				return []string{
					twoDaysAgoStr + "/parts/",
					threeDaysAgoStr + "/parts/",
				}
			}(),
		},
		{
			name: "partitions without corresponding daily block - no cleanup",
			streams: func() map[schema.ExternalLabelsHash]schema.ParquetBlocksStream {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				threeDaysAgoStr := threeDaysAgo.Format("2006/01/02")
				return map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
					0: {Metas: []schema.Meta{
						makeDailyMeta(twoDaysAgoStr, twoDaysAgo),
						makePartitionMeta(threeDaysAgoStr+"/parts/00-02", threeDaysAgo),
						makePartitionMeta(threeDaysAgoStr+"/parts/02-04", threeDaysAgo),
					}},
				}
			}(),
			partitionFiles: func() []string {
				threeDaysAgoStr := threeDaysAgo.Format("2006/01/02")
				return []string{
					threeDaysAgoStr + "/parts/00-02/0.labels.parquet",
					threeDaysAgoStr + "/parts/02-04/0.labels.parquet",
				}
			}(),
			expectExists: func() []string {
				threeDaysAgoStr := threeDaysAgo.Format("2006/01/02")
				return []string{
					threeDaysAgoStr + "/parts/00-02/0.labels.parquet",
					threeDaysAgoStr + "/parts/02-04/0.labels.parquet",
				}
			}(),
		},
		{
			name: "context cancellation stops cleanup",
			streams: func() map[schema.ExternalLabelsHash]schema.ParquetBlocksStream {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				threeDaysAgoStr := threeDaysAgo.Format("2006/01/02")
				return map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
					0: {Metas: []schema.Meta{
						makeDailyMeta(twoDaysAgoStr, twoDaysAgo),
						makePartitionMeta(twoDaysAgoStr+"/parts/00-02", twoDaysAgo),
						makeDailyMeta(threeDaysAgoStr, threeDaysAgo),
						makePartitionMeta(threeDaysAgoStr+"/parts/00-02", threeDaysAgo),
					}},
				}
			}(),
			partitionFiles: func() []string {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				threeDaysAgoStr := threeDaysAgo.Format("2006/01/02")
				return []string{
					twoDaysAgoStr + "/parts/00-02/0.labels.parquet",
					threeDaysAgoStr + "/parts/00-02/0.labels.parquet",
				}
			}(),
			cancelContext:    true,
			skipDeletedCheck: true,
		},
		{
			name: "mixed recent and stale days - only stale cleaned",
			streams: func() map[schema.ExternalLabelsHash]schema.ParquetBlocksStream {
				yesterdayStr := yesterday.Format("2006/01/02")
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				return map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
					0: {Metas: []schema.Meta{
						makeDailyMeta(yesterdayStr, yesterday),
						makePartitionMeta(yesterdayStr+"/parts/00-02", yesterday),
						makeDailyMeta(twoDaysAgoStr, twoDaysAgo),
						makePartitionMeta(twoDaysAgoStr+"/parts/00-02", twoDaysAgo),
					}},
				}
			}(),
			partitionFiles: func() []string {
				yesterdayStr := yesterday.Format("2006/01/02")
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				return []string{
					yesterdayStr + "/parts/00-02/0.labels.parquet",
					twoDaysAgoStr + "/parts/00-02/0.labels.parquet",
				}
			}(),
			expectExists: func() []string {
				yesterdayStr := yesterday.Format("2006/01/02")
				return []string{yesterdayStr + "/parts/00-02/0.labels.parquet"}
			}(),
			expectDeleted: func() []string {
				twoDaysAgoStr := twoDaysAgo.Format("2006/01/02")
				return []string{twoDaysAgoStr + "/parts/"}
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bkt, err := filesystem.NewBucket(t.TempDir())
			if err != nil {
				t.Fatalf("unable to create bucket: %s", err)
			}
			defer bkt.Close()

			// Create partition files
			for _, f := range tc.partitionFiles {
				if err := bkt.Upload(t.Context(), f, bytes.NewReader([]byte("test"))); err != nil {
					t.Fatalf("unable to upload %s: %s", f, err)
				}
			}

			// Run cleanup
			ctx := t.Context()
			if tc.cancelContext {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			cleanupStalePartitions(ctx, testLogger, bkt, tc.streams)

			// Check files that should exist
			for _, f := range tc.expectExists {
				exists, err := bkt.Exists(t.Context(), f)
				if err != nil {
					t.Fatalf("unable to check existence of %s: %s", f, err)
				}
				if !exists {
					t.Errorf("file %s should not have been deleted", f)
				}
			}

			// Check directories that should be empty (deleted)
			if !tc.skipDeletedCheck {
				for _, dir := range tc.expectDeleted {
					var remaining []string
					err := bkt.Iter(t.Context(), dir, func(name string) error {
						remaining = append(remaining, name)
						return nil
					})
					if err != nil {
						t.Fatalf("unable to iter %s: %s", dir, err)
					}
					if len(remaining) > 0 {
						t.Errorf("partition files in %s should have been deleted, remaining: %v", dir, remaining)
					}
				}
			}
		})
	}
}

func TestCleanupPartitionsForDay(t *testing.T) {
	tests := []struct {
		name          string
		day           util.Date
		files         []string // files to create
		expectDeleted []string // files that should be deleted
		expectExists  []string // files that should remain
	}{
		{
			name: "deletes all files under parts directory",
			day:  util.NewDate(2025, time.January, 15),
			files: []string{
				"2025/01/15/parts/00-02/0.labels.parquet",
				"2025/01/15/parts/00-02/0.chunks.parquet",
				"2025/01/15/parts/00-02/meta.pb",
				"2025/01/15/parts/02-04/0.labels.parquet",
				"2025/01/15/parts/02-04/0.chunks.parquet",
				"2025/01/15/parts/02-04/meta.pb",
			},
			expectDeleted: []string{
				"2025/01/15/parts/00-02/0.labels.parquet",
				"2025/01/15/parts/00-02/0.chunks.parquet",
				"2025/01/15/parts/00-02/meta.pb",
				"2025/01/15/parts/02-04/0.labels.parquet",
				"2025/01/15/parts/02-04/0.chunks.parquet",
				"2025/01/15/parts/02-04/meta.pb",
			},
		},
		{
			name: "does not affect daily block files",
			day:  util.NewDate(2025, time.January, 15),
			files: []string{
				"2025/01/15/0.labels.parquet",
				"2025/01/15/0.chunks.parquet",
				"2025/01/15/meta.pb",
				"2025/01/15/parts/00-02/0.labels.parquet",
				"2025/01/15/parts/00-02/meta.pb",
			},
			expectExists: []string{
				"2025/01/15/0.labels.parquet",
				"2025/01/15/0.chunks.parquet",
				"2025/01/15/meta.pb",
			},
			expectDeleted: []string{
				"2025/01/15/parts/00-02/0.labels.parquet",
				"2025/01/15/parts/00-02/meta.pb",
			},
		},
		{
			name:  "handles empty parts directory",
			day:   util.NewDate(2025, time.January, 15),
			files: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bkt, err := filesystem.NewBucket(t.TempDir())
			if err != nil {
				t.Fatalf("unable to create bucket: %s", err)
			}
			defer bkt.Close()

			// Create files
			for _, f := range tc.files {
				if err := bkt.Upload(t.Context(), f, bytes.NewReader([]byte("test"))); err != nil {
					t.Fatalf("unable to upload %s: %s", f, err)
				}
			}

			cleanupPartitionsForDay(t.Context(), testLogger, bkt, tc.day, 0)

			// Check files that should exist
			for _, f := range tc.expectExists {
				exists, err := bkt.Exists(t.Context(), f)
				if err != nil {
					t.Fatalf("unable to check existence of %s: %s", f, err)
				}
				if !exists {
					t.Errorf("file %s should not have been deleted", f)
				}
			}

			// Check files that should be deleted
			for _, f := range tc.expectDeleted {
				exists, err := bkt.Exists(t.Context(), f)
				if err != nil {
					t.Fatalf("unable to check existence of %s: %s", f, err)
				}
				if exists {
					t.Errorf("file %s should have been deleted", f)
				}
			}
		})
	}
}

// Helper to create a daily block meta
func makeDailyMeta(name string, day time.Time) schema.Meta {
	return schema.Meta{
		Name: name,
		Mint: day.UnixMilli(),
		Maxt: day.Add(24 * time.Hour).UnixMilli(),
	}
}

// Helper to create a partition meta
func makePartitionMeta(name string, day time.Time) schema.Meta {
	return schema.Meta{
		Name: name,
		Mint: day.UnixMilli(),
		Maxt: day.Add(2 * time.Hour).UnixMilli(),
	}
}
