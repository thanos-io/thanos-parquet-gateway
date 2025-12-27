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
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/thanos-io/thanos-parquet-gateway/internal/log"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type LabelsBuilder struct {
	sf            singleflight.Group
	db            *sql.DB
	cacheDir      string
	parquetWriter *ParquetIndexWriter
}

func NewLabelsBuilder(db *sql.DB, cacheDir string) *LabelsBuilder {
	return &LabelsBuilder{
		db:            db,
		cacheDir:      cacheDir,
		parquetWriter: NewParquetIndexWriter(cacheDir),
	}
}

func (idx *LabelsBuilder) EnsureBuilt(ctx context.Context, block string) error {
	dateStr := DateFromBlock(block)

	path := filepath.Join(idx.cacheDir, labelsParquetDir, dateStr+".parquet")
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	_, err, shared := idx.sf.Do(block, func() (any, error) {
		return nil, idx.build(ctx, block)
	})

	if shared {
		log.Ctx(ctx).Debug("LabelsBuilder build deduplicated", slog.String("block", block))
	}

	return err
}

func (idx *LabelsBuilder) build(ctx context.Context, block string) error {
	l := log.Ctx(ctx)
	l.Debug("LabelsBuilder building", slog.String("block", block))
	startTime := time.Now()

	dateStr := DateFromBlock(block)
	glob := fmt.Sprintf("'%s'", filepath.Join(idx.cacheDir, block, "**", "*.labels.parquet"))

	labels, totalUnique, err := idx.queryLabels(ctx, glob)
	if err != nil {
		return fmt.Errorf("query labels: %w", err)
	}

	entry := &LabelsEntry{
		Date:              dateStr,
		Block:             block,
		Labels:            labels,
		TotalLabels:       len(labels),
		TotalUniqueValues: totalUnique,
		CachedAt:          time.Now(),
	}

	if err := idx.parquetWriter.WriteLabelsIndex(entry); err != nil {
		return fmt.Errorf("write parquet index: %w", err)
	}

	l.Debug("LabelsBuilder built",
		slog.String("block", block),
		slog.Int("labels", len(labels)),
		slog.Duration("duration", time.Since(startTime)),
	)

	return nil
}

func (idx *LabelsBuilder) queryLabels(ctx context.Context, glob string) ([]LabelCardinality, int64, error) {
	nameColumn := schema.LabelNameToColumn("__name__")

	schemaQuery := fmt.Sprintf(`
		SELECT column_name
		FROM (DESCRIBE SELECT * FROM read_parquet([%s], union_by_name = true))
		WHERE column_name LIKE '%s%%'
		  AND column_name != '%s'
	`, glob, schema.LabelColumnPrefix, nameColumn)

	schemaRows, err := idx.db.QueryContext(ctx, schemaQuery)
	if err != nil {
		return nil, 0, fmt.Errorf("query schema: %w", err)
	}

	var columns []string
	for schemaRows.Next() {
		var col string
		if err := schemaRows.Scan(&col); err != nil {
			schemaRows.Close()
			return nil, 0, fmt.Errorf("scan column name: %w", err)
		}
		columns = append(columns, col)
	}
	schemaRows.Close()

	if len(columns) == 0 {
		return nil, 0, nil
	}

	var selectParts []string
	for _, col := range columns {
		selectParts = append(selectParts,
			fmt.Sprintf(`APPROX_COUNT_DISTINCT("%s") AS "%s"`, col, col))
	}

	dataQuery := fmt.Sprintf(`
		SELECT %s
		FROM read_parquet([%s], union_by_name = true)
		WHERE "%s" IS NOT NULL
	`, strings.Join(selectParts, ", "), glob, nameColumn)

	row := idx.db.QueryRowContext(ctx, dataQuery)

	values := make([]int64, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}

	if err := row.Scan(ptrs...); err != nil {
		if err == sql.ErrNoRows {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("scan counts: %w", err)
	}

	var total int64
	for _, v := range values {
		total += v
	}

	result := make([]LabelCardinality, 0, len(columns))
	for i, col := range columns {
		if values[i] == 0 {
			continue
		}
		labelName := schema.ColumnToLabelName(col)
		pct := 0.0
		if total > 0 {
			pct = 100.0 * float64(values[i]) / float64(total)
		}
		result = append(result, LabelCardinality{
			LabelName:    labelName,
			UniqueValues: values[i],
			Percentage:   pct,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].UniqueValues > result[j].UniqueValues
	})

	return result, total, nil
}
