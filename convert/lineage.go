// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"log/slog"
	"time"

	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

// BlockLineageChecker analyzes block relationships to prevent redundant conversions
type BlockLineageChecker struct {
	logger *slog.Logger
}

func NewBlockLineageChecker(logger *slog.Logger) *BlockLineageChecker {
	return &BlockLineageChecker{logger: logger}
}

// IsRedundantConversion checks if a TSDB block's data has already been converted
// by analyzing the lineage of existing parquet blocks
func (blc *BlockLineageChecker) IsRedundantConversion(
	tsdbBlock metadata.Meta,
	day time.Time,
	parquetMetas map[string]schema.Meta,
) bool {
	blockULID := tsdbBlock.ULID.String()
	
	// Check if this exact TSDB block has already been used as source for any parquet block
	for _, parquetMeta := range parquetMetas {
		for _, sourceBlock := range parquetMeta.SourceBlocks {
			if sourceBlock == blockULID {
				blc.logger.Info("Block already converted",
					"tsdb_block", blockULID,
					"parquet_block", parquetMeta.Name,
					"day", day.Format("2006-01-02"))
				return true
			}
		}
	}
	
	// Check if this is a compacted block and its constituent blocks have been converted
	if len(tsdbBlock.BlockMeta.Compaction.Parents) > 0 {
		return blc.isCompactedBlockRedundant(tsdbBlock, day, parquetMetas)
	}
	
	return false
}

// isCompactedBlockRedundant checks if a compacted block's constituent blocks
// have already been converted to parquet
func (blc *BlockLineageChecker) isCompactedBlockRedundant(
	compactedBlock metadata.Meta,
	day time.Time,
	parquetMetas map[string]schema.Meta,
) bool {
	parentULIDs := make(map[string]bool)
	for _, parent := range compactedBlock.BlockMeta.Compaction.Parents {
		parentULIDs[parent.ULID.String()] = false // false = not found in parquet blocks
	}
	
	// Check if all parent blocks have been used as sources for parquet blocks
	// covering the same day
	dayStart := util.BeginOfDay(day)
	dayEnd := util.EndOfDay(day)
	
	for _, parquetMeta := range parquetMetas {
		// Only consider parquet blocks that overlap with the day we're checking
		parquetStart := time.UnixMilli(parquetMeta.Mint)
		parquetEnd := time.UnixMilli(parquetMeta.Maxt)
		
		if parquetEnd.Before(dayStart) || parquetStart.After(dayEnd) {
			continue // No overlap with the day
		}
		
		// Mark any parent blocks found in this parquet block's sources
		for _, sourceBlock := range parquetMeta.SourceBlocks {
			if _, exists := parentULIDs[sourceBlock]; exists {
				parentULIDs[sourceBlock] = true
			}
		}
	}
	
	// Check if all parent blocks covering this day have been converted
	allParentsConverted := true
	for parentULID, found := range parentULIDs {
		if !found {
			// This parent block hasn't been converted yet for this day
			blc.logger.Debug("Parent block not yet converted",
				"parent_block", parentULID,
				"compacted_block", compactedBlock.ULID.String(),
				"day", day.Format("2006-01-02"))
			allParentsConverted = false
		}
	}
	
	if allParentsConverted && len(parentULIDs) > 0 {
		blc.logger.Info("Compacted block is redundant - all parent blocks already converted",
			"compacted_block", compactedBlock.ULID.String(),
			"parent_count", len(parentULIDs),
			"day", day.Format("2006-01-02"))
		return true
	}
	
	return false
}

// GetConflictingParquetBlocks returns parquet blocks that would conflict with
// converting the given TSDB block for the specified day
func (blc *BlockLineageChecker) GetConflictingParquetBlocks(
	tsdbBlock metadata.Meta,
	day time.Time,
	parquetMetas map[string]schema.Meta,
) []string {
	var conflicts []string
	blockULID := tsdbBlock.ULID.String()
	dayStart := util.BeginOfDay(day)
	dayEnd := util.EndOfDay(day)
	
	for parquetName, parquetMeta := range parquetMetas {
		// Check if parquet block overlaps with the day
		parquetStart := time.UnixMilli(parquetMeta.Mint)
		parquetEnd := time.UnixMilli(parquetMeta.Maxt)
		
		if parquetEnd.Before(dayStart) || parquetStart.After(dayEnd) {
			continue // No overlap
		}
		
		// Check if this parquet block was created from the same TSDB block
		for _, sourceBlock := range parquetMeta.SourceBlocks {
			if sourceBlock == blockULID {
				conflicts = append(conflicts, parquetName)
				break
			}
		}
	}
	
	return conflicts
}
