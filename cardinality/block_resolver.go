// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos-parquet-gateway/internal/log"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type BlockResolver interface {
	ResolveBlocks(ctx context.Context, start, end time.Time) ([]string, error)
	CountShards(ctx context.Context, blockName string) (int, error)
	ListShardPaths(ctx context.Context, blockName string) ([]string, error)
}

type blockResolver struct {
	bucket objstore.Bucket
}

func NewBlockResolver(bucket objstore.Bucket) BlockResolver {
	return &blockResolver{bucket: bucket}
}

func (r *blockResolver) ResolveBlocks(ctx context.Context, start, end time.Time) ([]string, error) {
	l := log.Ctx(ctx).With(
		slog.String("method", "ResolveBlocks"),
		slog.Time("start", start),
		slog.Time("end", end),
	)
	l.Debug("Starting")

	var blocks []string

	for day := start; !day.After(end); day = day.AddDate(0, 0, 1) {
		if ctx.Err() != nil {
			l.Error("Context cancelled", "err", ctx.Err())
			return nil, ctx.Err()
		}

		blockName := fmt.Sprintf("%04d/%02d/%02d", day.Year(), int(day.Month()), day.Day())
		metaPath := schema.MetaFileNameForBlock(blockName)
		exists, err := r.bucket.Exists(ctx, metaPath)
		if err != nil {
			continue // Skip on error
		}
		if exists {
			blocks = append(blocks, blockName)
		}
	}

	l.Debug("Completed",
		slog.Int("blocks_found", len(blocks)),
		slog.Any("blocks", blocks))
	return blocks, nil
}

func (r *blockResolver) CountShards(ctx context.Context, blockName string) (int, error) {
	l := log.Ctx(ctx).With(
		slog.String("method", "CountShards"),
		slog.String("block", blockName),
	)
	l.Debug("Starting")

	var count int

	err := r.bucket.Iter(ctx, blockName+"/", func(name string) error {
		base := filepath.Base(name)
		if strings.HasSuffix(base, ".labels.parquet") {
			count++
		}
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		l.Error("Failed", "err", err)
		return 0, fmt.Errorf("iterate block: %w", err)
	}

	l.Debug("Completed", slog.Int("shard_count", count))
	return count, nil
}

func (r *blockResolver) ListShardPaths(ctx context.Context, blockName string) ([]string, error) {
	l := log.Ctx(ctx).With(
		slog.String("method", "ListShardPaths"),
		slog.String("block", blockName),
	)
	l.Debug("Starting")

	var paths []string

	err := r.bucket.Iter(ctx, blockName+"/", func(name string) error {
		base := filepath.Base(name)
		if strings.HasSuffix(base, ".labels.parquet") {
			paths = append(paths, name)
		}
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		l.Error("Failed", "err", err)
		return nil, fmt.Errorf("iterate block: %w", err)
	}

	l.Debug("Completed",
		slog.Int("path_count", len(paths)),
		slog.Any("paths", paths))
	return paths, nil
}
