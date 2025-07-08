// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

func TestBlockLineageChecker_IsRedundantConversion(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	checker := NewBlockLineageChecker(logger)

	// Create some test ULIDs
	block1ULID := ulid.MustNew(ulid.Timestamp(time.Now()), nil)
	block2ULID := ulid.MustNew(ulid.Timestamp(time.Now().Add(time.Minute)), nil)
	compactedULID := ulid.MustNew(ulid.Timestamp(time.Now().Add(2*time.Minute)), nil)

	testDay := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Test case 1: Direct conversion - block has already been converted
	t.Run("direct_conversion_already_exists", func(t *testing.T) {
		tsdbBlock := metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: block1ULID,
			},
		}

		parquetMetas := map[string]schema.Meta{
			"2024/01/01": {
				Name:         "2024/01/01",
				Mint:         testDay.UnixMilli(),
				Maxt:         testDay.Add(24 * time.Hour).UnixMilli(),
				SourceBlocks: []string{block1ULID.String()},
			},
		}

		if !checker.IsRedundantConversion(tsdbBlock, testDay, parquetMetas) {
			t.Error("Expected redundant conversion detection for already converted block")
		}
	})

	// Test case 2: Compacted block with all parents converted
	t.Run("compacted_block_all_parents_converted", func(t *testing.T) {
		compactedBlock := metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: compactedULID,
				Compaction: tsdb.BlockMetaCompaction{
					Parents: []tsdb.BlockDesc{
						{ULID: block1ULID},
						{ULID: block2ULID},
					},
				},
			},
		}

		parquetMetas := map[string]schema.Meta{
			"2024/01/01": {
				Name:         "2024/01/01",
				Mint:         testDay.UnixMilli(),
				Maxt:         testDay.Add(24 * time.Hour).UnixMilli(),
				SourceBlocks: []string{block1ULID.String(), block2ULID.String()},
			},
		}

		if !checker.IsRedundantConversion(compactedBlock, testDay, parquetMetas) {
			t.Error("Expected redundant conversion detection for compacted block with all parents converted")
		}
	})

	// Test case 3: New block that hasn't been converted
	t.Run("new_block_not_converted", func(t *testing.T) {
		newULID := ulid.MustNew(ulid.Timestamp(time.Now().Add(5*time.Minute)), nil)
		tsdbBlock := metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: newULID,
			},
		}

		parquetMetas := map[string]schema.Meta{
			"2024/01/01": {
				Name:         "2024/01/01",
				Mint:         testDay.UnixMilli(),
				Maxt:         testDay.Add(24 * time.Hour).UnixMilli(),
				SourceBlocks: []string{block1ULID.String()},
			},
		}

		if checker.IsRedundantConversion(tsdbBlock, testDay, parquetMetas) {
			t.Error("Expected no redundancy detection for new unconverted block")
		}
	})
}

func TestPlannerWithLineageChecking(t *testing.T) {
	// Test that planner correctly filters out redundant blocks
	testDay := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	nextDay := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	block1ULID := ulid.MustNew(ulid.Timestamp(time.Now()), nil)
	compactedULID := ulid.MustNew(ulid.Timestamp(time.Now().Add(time.Minute)), nil)

	tsdbMetas := map[string]metadata.Meta{
		compactedULID.String(): {
			BlockMeta: tsdb.BlockMeta{
				ULID:    compactedULID,
				MinTime: nextDay.UnixMilli(),
				MaxTime: nextDay.Add(24 * time.Hour).UnixMilli(),
				Compaction: tsdb.BlockMetaCompaction{
					Parents: []tsdb.BlockDesc{
						{ULID: block1ULID},
					},
				},
			},
		},
	}

	parquetMetas := map[string]schema.Meta{
		"2024/01/01": {
			Name:         "2024/01/01",
			Mint:         testDay.UnixMilli(),
			Maxt:         testDay.Add(24 * time.Hour).UnixMilli(),
			SourceBlocks: []string{block1ULID.String()},
		},
	}

	// Test with lineage checking enabled
	plannerWithLineage := NewPlannerWithOptions(time.Now().Add(time.Hour), true)
	plan, ok := plannerWithLineage.Plan(tsdbMetas, parquetMetas)
	if ok {
		t.Error("Expected no conversion plan when compacted block's parents are already converted")
	}

	// Test with lineage checking disabled
	plannerWithoutLineage := NewPlannerWithOptions(time.Now().Add(time.Hour), false)
	plan, ok = plannerWithoutLineage.Plan(tsdbMetas, parquetMetas)
	if !ok || len(plan.Download) == 0 {
		t.Error("Expected conversion plan when lineage checking is disabled")
	}
}
