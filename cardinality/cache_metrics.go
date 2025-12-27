// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/thanos-io/thanos-parquet-gateway/internal/log"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type MetricsBuilder struct {
	sf       singleflight.Group
	db       *sql.DB
	writer   *ParquetIndexWriter
	cacheDir string
}

func NewMetricsBuilder(db *sql.DB, cacheDir string) *MetricsBuilder {
	return &MetricsBuilder{
		db:       db,
		cacheDir: cacheDir,
		writer:   NewParquetIndexWriter(cacheDir),
	}
}

func (idx *MetricsBuilder) EnsureBuilt(ctx context.Context, block string) error {
	dateStr := DateFromBlock(block)

	// Check if parquet index already exists
	path := filepath.Join(idx.cacheDir, metricsParquetDir, dateStr+".parquet")
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	_, err, shared := idx.sf.Do(block, func() (any, error) {
		return nil, idx.build(ctx, block)
	})

	if shared {
		log.Ctx(ctx).Debug("MetricsBuilder build deduplicated", slog.String("block", block))
	}

	return err
}

// build creates the parquet index for a block from source parquet files.
func (idx *MetricsBuilder) build(ctx context.Context, block string) error {
	l := log.Ctx(ctx)
	l.Debug("MetricsBuilder building", slog.String("block", block))
	startTime := time.Now()

	dateStr := DateFromBlock(block)
	glob := fmt.Sprintf("'%s'", filepath.Join(idx.cacheDir, block, "**", "*.labels.parquet"))

	metrics, totalSeries, err := idx.queryMetrics(ctx, glob)
	if err != nil {
		return fmt.Errorf("query metrics: %w", err)
	}

	entry := &MetricsEntry{
		Date:         dateStr,
		Block:        block,
		Metrics:      metrics,
		TotalSeries:  totalSeries,
		TotalMetrics: len(metrics),
		CachedAt:     time.Now(),
	}

	// Write to parquet synchronously to ensure it's available for queries
	if err := idx.writer.WriteMetricsIndex(entry); err != nil {
		return fmt.Errorf("write parquet index: %w", err)
	}

	l.Debug("MetricsBuilder built",
		slog.String("block", block),
		slog.Int("metrics", len(metrics)),
		slog.Duration("duration", time.Since(startTime)),
	)

	return nil
}

func (idx *MetricsBuilder) queryMetrics(ctx context.Context, glob string) ([]MetricCardinality, int64, error) {
	nameColumn := schema.LabelNameToColumn("__name__")

	query := fmt.Sprintf(`
		WITH daily_counts AS (
			SELECT "%s" AS metric_name, COUNT(*) AS series_count
			FROM read_parquet([%s], union_by_name = true)
			WHERE "%s" IS NOT NULL
			GROUP BY 1
		),
		totals AS (
			SELECT SUM(series_count) AS total
			FROM daily_counts
		)
		SELECT
			dc.metric_name,
			dc.series_count,
			100.0 * dc.series_count / NULLIF(t.total, 0) AS percentage
		FROM daily_counts dc, totals t
		ORDER BY dc.series_count DESC
	`, nameColumn, glob, nameColumn)

	rows, err := idx.db.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("query parquet: %w", err)
	}
	defer rows.Close()

	var metrics []MetricCardinality
	var totalSeries int64

	for rows.Next() {
		var name string
		var count int64
		var percentage float64
		if err := rows.Scan(&name, &count, &percentage); err != nil {
			return nil, 0, fmt.Errorf("scan row: %w", err)
		}

		metrics = append(metrics, MetricCardinality{
			MetricName:  name,
			SeriesCount: count,
			Percentage:  percentage,
		})
		totalSeries += count
	}

	return metrics, totalSeries, rows.Err()
}
