// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package testutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

// LabelRow represents a row in the labels parquet file for testing.
type LabelRow struct {
	Name  string `parquet:"___cf_meta_label___name__,optional"`
	Index []byte `parquet:"___cf_meta_index"`
	Hash  int64  `parquet:"___cf_meta_hash"`
}

// GenerateTestLabelsParquet creates a minimal labels.parquet file
// with the specified metric name → count mapping.
func GenerateTestLabelsParquet(t testing.TB, dir string, metrics map[string]int) string {
	t.Helper()

	path := filepath.Join(dir, "test.labels.parquet")

	// Build rows
	var rows []LabelRow
	for name, count := range metrics {
		for i := range count {
			rows = append(rows, LabelRow{
				Name:  name,
				Index: []byte{},
				Hash:  int64(i),
			})
		}
	}

	// Write parquet file
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test parquet file: %v", err)
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[LabelRow](f)
	if len(rows) > 0 {
		if _, err := writer.Write(rows); err != nil {
			t.Fatalf("failed to write rows: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	return path
}

// GenerateTestLabelsParquetWithSchema creates a labels.parquet file
// using the actual schema from the project.
func GenerateTestLabelsParquetWithSchema(t testing.TB, dir string, metrics map[string]int) string {
	t.Helper()

	path := filepath.Join(dir, "test.labels.parquet")

	// Build schema with __name__ label
	s := schema.BuildSchemaFromLabels([]string{"__name__"})

	// Create file
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test parquet file: %v", err)
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[map[string]any](f, s)

	// Write rows
	for name, count := range metrics {
		for i := range count {
			row := map[string]any{
				schema.LabelIndexColumn:              []byte{},
				schema.LabelHashColumn:               int64(i),
				schema.LabelNameToColumn("__name__"): name,
				schema.ChunksColumn0:                 []byte{},
				schema.ChunksColumn1:                 []byte{},
				schema.ChunksColumn2:                 []byte{},
			}
			if _, err := writer.Write([]map[string]any{row}); err != nil {
				t.Fatalf("failed to write row: %v", err)
			}
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	return path
}

// SetupTestBlock creates a block directory structure with test parquet files.
func SetupTestBlock(t testing.TB, baseDir, blockName string, shards int, metrics map[string]int) {
	t.Helper()

	blockDir := filepath.Join(baseDir, blockName)
	if err := os.MkdirAll(blockDir, 0755); err != nil {
		t.Fatalf("failed to create block dir: %v", err)
	}

	// Create meta.pb file (empty for testing)
	metaPath := filepath.Join(blockDir, "meta.pb")
	if err := os.WriteFile(metaPath, []byte{}, 0644); err != nil {
		t.Fatalf("failed to create meta.pb: %v", err)
	}

	// Create shards
	for shard := range shards {
		shardPath := filepath.Join(blockDir, shardFileName(shard))
		generateLabelsParquetAt(t, shardPath, metrics)
	}
}

func shardFileName(shard int) string {
	return filepath.Base(schema.LabelsPfileNameForShard("", shard))
}

func generateLabelsParquetAt(t testing.TB, path string, metrics map[string]int) {
	t.Helper()

	// Build rows
	var rows []LabelRow
	for name, count := range metrics {
		for i := range count {
			rows = append(rows, LabelRow{
				Name:  name,
				Index: []byte{},
				Hash:  int64(i),
			})
		}
	}

	// Write parquet file
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test parquet file: %v", err)
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[LabelRow](f)
	if len(rows) > 0 {
		if _, err := writer.Write(rows); err != nil {
			t.Fatalf("failed to write rows: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
}

// TestSeries represents a series with metric name and labels for testing.
type TestSeries struct {
	Name   string
	Labels map[string]string
}

// CreateTestLabelsParquet creates a labels.parquet file in the specified block directory
// with the given series data, including all labels as columns.
func CreateTestLabelsParquet(t testing.TB, baseDir, block string, series []TestSeries) string {
	t.Helper()

	// Collect all unique label names
	labelNames := make(map[string]struct{})
	labelNames["__name__"] = struct{}{}
	for _, s := range series {
		for k := range s.Labels {
			labelNames[k] = struct{}{}
		}
	}

	// Convert to sorted slice
	var labels []string
	for k := range labelNames {
		labels = append(labels, k)
	}

	// Build schema
	s := schema.BuildSchemaFromLabels(labels)

	// Create directory
	blockDir := filepath.Join(baseDir, block)
	if err := os.MkdirAll(blockDir, 0755); err != nil {
		t.Fatalf("failed to create block dir: %v", err)
	}

	// Create file
	path := filepath.Join(blockDir, "0.labels.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test parquet file: %v", err)
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[map[string]any](f, s)

	// Write rows
	for i, ser := range series {
		row := map[string]any{
			schema.LabelIndexColumn:              []byte{},
			schema.LabelHashColumn:               int64(i),
			schema.LabelNameToColumn("__name__"): ser.Name,
			schema.ChunksColumn0:                 []byte{},
			schema.ChunksColumn1:                 []byte{},
			schema.ChunksColumn2:                 []byte{},
		}
		for k, v := range ser.Labels {
			row[schema.LabelNameToColumn(k)] = v
		}
		if _, err := writer.Write([]map[string]any{row}); err != nil {
			t.Fatalf("failed to write row: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	return path
}
