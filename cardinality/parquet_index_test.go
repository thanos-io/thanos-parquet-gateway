// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stretchr/testify/require"
)

func TestParquetIndexWriter_WriteMetricsIndex(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	entry := &MetricsEntry{
		Date:  "2024-12-01",
		Block: "2024/12/01",
		Metrics: []MetricCardinality{
			{MetricName: "http_requests_total", SeriesCount: 15000, Percentage: 60.0},
			{MetricName: "process_cpu_seconds", SeriesCount: 10000, Percentage: 40.0},
		},
		TotalSeries:  25000,
		TotalMetrics: 2,

		CachedAt: time.Now(),
	}

	err := writer.WriteMetricsIndex(entry)
	require.NoError(t, err)

	// Verify file was created
	path := filepath.Join(tmpDir, metricsParquetDir, "2024-12-01.parquet")
	_, err = os.Stat(path)
	require.NoError(t, err)

	// Verify we can read it back with DuckDB
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT date, block, metric_name, series_count FROM read_parquet('" + path + "') ORDER BY series_count DESC")
	require.NoError(t, err)
	defer rows.Close()

	var results []MetricsIndexRow
	for rows.Next() {
		var row MetricsIndexRow
		err := rows.Scan(&row.Date, &row.Block, &row.MetricName, &row.SeriesCount)
		require.NoError(t, err)
		results = append(results, row)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	require.Equal(t, "http_requests_total", results[0].MetricName)
	require.Equal(t, int64(15000), results[0].SeriesCount)
	require.Equal(t, "2024-12-01", results[0].Date)
	require.Equal(t, "2024/12/01", results[0].Block)

	require.Equal(t, "process_cpu_seconds", results[1].MetricName)
	require.Equal(t, int64(10000), results[1].SeriesCount)
}

func TestParquetIndexWriter_WriteLabelsIndex(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	entry := &LabelsEntry{
		Date:  "2024-12-01",
		Block: "2024/12/01",
		Labels: []LabelCardinality{
			{LabelName: "pod", UniqueValues: 5000, Percentage: 50.0},
			{LabelName: "namespace", UniqueValues: 100, Percentage: 1.0},
		},
		TotalLabels:       2,
		TotalUniqueValues: 5100,

		CachedAt: time.Now(),
	}

	err := writer.WriteLabelsIndex(entry)
	require.NoError(t, err)

	// Verify file was created
	path := filepath.Join(tmpDir, labelsParquetDir, "2024-12-01.parquet")
	_, err = os.Stat(path)
	require.NoError(t, err)

	// Verify we can read it back with DuckDB
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT date, block, label_name, unique_values FROM read_parquet('" + path + "') ORDER BY unique_values DESC")
	require.NoError(t, err)
	defer rows.Close()

	var results []LabelsIndexRow
	for rows.Next() {
		var row LabelsIndexRow
		err := rows.Scan(&row.Date, &row.Block, &row.LabelName, &row.UniqueValues)
		require.NoError(t, err)
		results = append(results, row)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	require.Equal(t, "pod", results[0].LabelName)
	require.Equal(t, int64(5000), results[0].UniqueValues)
	require.Equal(t, "namespace", results[1].LabelName)
	require.Equal(t, int64(100), results[1].UniqueValues)
}

func TestParquetIndexReader_QueryMetricsCardinality(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	// Write test data for multiple days
	days := []struct {
		date    string
		block   string
		metrics []MetricCardinality
	}{
		{
			date:  "2024-12-01",
			block: "2024/12/01",
			metrics: []MetricCardinality{
				{MetricName: "http_requests_total", SeriesCount: 15000},
				{MetricName: "process_cpu_seconds", SeriesCount: 10000},
			},
		},
		{
			date:  "2024-12-02",
			block: "2024/12/02",
			metrics: []MetricCardinality{
				{MetricName: "http_requests_total", SeriesCount: 16000},
				{MetricName: "process_cpu_seconds", SeriesCount: 9000},
			},
		},
	}

	for _, day := range days {
		entry := &MetricsEntry{
			Date:    day.date,
			Block:   day.block,
			Metrics: day.metrics,

			CachedAt: time.Now(),
		}
		err := writer.WriteMetricsIndex(entry)
		require.NoError(t, err)
	}

	// Create reader
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	reader := NewParquetIndexReader(db, tmpDir)

	// Query for the date range
	start, _ := time.Parse("2006-01-02", "2024-12-01")
	end, _ := time.Parse("2006-01-02", "2024-12-02")

	result, err := reader.QueryMetricsCardinality(context.Background(), start, end, 10, nil)
	require.NoError(t, err)

	require.Equal(t, 2, len(result.Days))
	require.Equal(t, int64(50000), result.TotalSeries)
	require.Equal(t, int64(2), result.TotalMetrics) // 2 unique metrics

	// Check first day
	require.Equal(t, "2024-12-01", result.Days[0].Date)
	require.Len(t, result.Days[0].Metrics, 2)
	require.Equal(t, "http_requests_total", result.Days[0].Metrics[0].MetricName)
	require.Equal(t, int64(15000), result.Days[0].Metrics[0].SeriesCount)

	// Check second day
	require.Equal(t, "2024-12-02", result.Days[1].Date)
	require.Len(t, result.Days[1].Metrics, 2)
	require.Equal(t, "http_requests_total", result.Days[1].Metrics[0].MetricName)
	require.Equal(t, int64(16000), result.Days[1].Metrics[0].SeriesCount)
}

func TestParquetIndexReader_QueryMetricsCardinality_EmptyRange(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	// Write data for 2024-12-01
	entry := &MetricsEntry{
		Date:  "2024-12-01",
		Block: "2024/12/01",
		Metrics: []MetricCardinality{
			{MetricName: "http_requests_total", SeriesCount: 15000},
		},
		CachedAt: time.Now(),
	}
	err := writer.WriteMetricsIndex(entry)
	require.NoError(t, err)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	reader := NewParquetIndexReader(db, tmpDir)

	// Query for a different date range (no data)
	start, _ := time.Parse("2006-01-02", "2024-11-01")
	end, _ := time.Parse("2006-01-02", "2024-11-30")

	result, err := reader.QueryMetricsCardinality(context.Background(), start, end, 10, nil)
	require.NoError(t, err)

	// Days are filled with empty entries for the range, but no actual data
	require.Len(t, result.Days, 30) // Nov 1-30
	require.Equal(t, int64(0), result.TotalSeries)
	require.Equal(t, int64(0), result.TotalMetrics)
	require.Equal(t, 0, result.BlocksAnalyzed) // no blocks analyzed

	// All days should have empty metrics
	for _, day := range result.Days {
		require.Empty(t, day.Metrics)
	}
}

func TestParquetIndexReader_QueryMetricsCardinality_Limit(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	// Write data with many metrics
	entry := &MetricsEntry{
		Date:  "2024-12-01",
		Block: "2024/12/01",
		Metrics: []MetricCardinality{
			{MetricName: "metric_a", SeriesCount: 1000},
			{MetricName: "metric_b", SeriesCount: 900},
			{MetricName: "metric_c", SeriesCount: 800},
			{MetricName: "metric_d", SeriesCount: 700},
			{MetricName: "metric_e", SeriesCount: 600},
		},
		CachedAt: time.Now(),
	}
	err := writer.WriteMetricsIndex(entry)
	require.NoError(t, err)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	reader := NewParquetIndexReader(db, tmpDir)

	start, _ := time.Parse("2006-01-02", "2024-12-01")
	end, _ := time.Parse("2006-01-02", "2024-12-01")

	// Query with limit=3
	result, err := reader.QueryMetricsCardinality(context.Background(), start, end, 3, nil)
	require.NoError(t, err)

	require.Len(t, result.Days, 1)
	require.Len(t, result.Days[0].Metrics, 3) // limited to 3
	require.Equal(t, "metric_a", result.Days[0].Metrics[0].MetricName)
	require.Equal(t, "metric_b", result.Days[0].Metrics[1].MetricName)
	require.Equal(t, "metric_c", result.Days[0].Metrics[2].MetricName)
}

func TestParquetIndexReader_QueryMetricsCardinality_NonExistentFilter(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	entry := &MetricsEntry{
		Date:  "2024-12-01",
		Block: "2024/12/01",
		Metrics: []MetricCardinality{
			{MetricName: "http_requests_total", SeriesCount: 15000},
		},
		CachedAt: time.Now(),
	}
	err := writer.WriteMetricsIndex(entry)
	require.NoError(t, err)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	reader := NewParquetIndexReader(db, tmpDir)

	start, _ := time.Parse("2006-01-02", "2024-12-01")
	end, _ := time.Parse("2006-01-02", "2024-12-01")

	// Filter by non-existent metric
	result, err := reader.QueryMetricsCardinality(context.Background(), start, end, 10, MetricNameToMatchers("nonexistent_metric"))
	require.NoError(t, err)

	// Day is filled but with empty metrics
	require.Len(t, result.Days, 1)
	require.Empty(t, result.Days[0].Metrics)
	require.Equal(t, int64(0), result.TotalSeries)
	require.Equal(t, int64(0), result.TotalMetrics)
}

func TestParquetIndexReader_QueryMetricsCardinality_WithFilter(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	entry := &MetricsEntry{
		Date:  "2024-12-01",
		Block: "2024/12/01",
		Metrics: []MetricCardinality{
			{MetricName: "http_requests_total", SeriesCount: 15000},
			{MetricName: "process_cpu_seconds", SeriesCount: 10000},
		},

		CachedAt: time.Now(),
	}
	err := writer.WriteMetricsIndex(entry)
	require.NoError(t, err)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	reader := NewParquetIndexReader(db, tmpDir)

	start, _ := time.Parse("2006-01-02", "2024-12-01")
	end, _ := time.Parse("2006-01-02", "2024-12-01")

	// Filter by specific metric
	result, err := reader.QueryMetricsCardinality(context.Background(), start, end, 10, MetricNameToMatchers("http_requests_total"))
	require.NoError(t, err)

	require.Len(t, result.Days, 1)
	require.Len(t, result.Days[0].Metrics, 1)
	require.Equal(t, "http_requests_total", result.Days[0].Metrics[0].MetricName)
	require.Equal(t, int64(15000), result.Days[0].Metrics[0].SeriesCount)
}

func TestParquetIndexReader_QueryLabelsCardinality(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	// Write test data
	entry := &LabelsEntry{
		Date:  "2024-12-01",
		Block: "2024/12/01",
		Labels: []LabelCardinality{
			{LabelName: "pod", UniqueValues: 5000},
			{LabelName: "namespace", UniqueValues: 100},
		},
		CachedAt: time.Now(),
	}
	err := writer.WriteLabelsIndex(entry)
	require.NoError(t, err)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	reader := NewParquetIndexReader(db, tmpDir)

	start, _ := time.Parse("2006-01-02", "2024-12-01")
	end, _ := time.Parse("2006-01-02", "2024-12-01")

	result, err := reader.QueryLabelsCardinality(context.Background(), start, end, 10, nil)
	require.NoError(t, err)

	require.Len(t, result.Days, 1)
	require.Len(t, result.Days[0].Labels, 2)
	require.Equal(t, "pod", result.Days[0].Labels[0].LabelName)
	require.Equal(t, int64(5000), result.Days[0].Labels[0].UniqueValues)
}

func TestParquetIndexReader_QueryLabelsCardinality_MultiDay(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	// Write test data for multiple days
	days := []struct {
		date   string
		block  string
		labels []LabelCardinality
	}{
		{
			date:  "2024-12-01",
			block: "2024/12/01",
			labels: []LabelCardinality{
				{LabelName: "pod", UniqueValues: 5000},
				{LabelName: "namespace", UniqueValues: 100},
			},
		},
		{
			date:  "2024-12-02",
			block: "2024/12/02",
			labels: []LabelCardinality{
				{LabelName: "pod", UniqueValues: 5500},
				{LabelName: "namespace", UniqueValues: 110},
			},
		},
	}

	for _, day := range days {
		entry := &LabelsEntry{
			Date:     day.date,
			Block:    day.block,
			Labels:   day.labels,
			CachedAt: time.Now(),
		}
		err := writer.WriteLabelsIndex(entry)
		require.NoError(t, err)
	}

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	reader := NewParquetIndexReader(db, tmpDir)

	start, _ := time.Parse("2006-01-02", "2024-12-01")
	end, _ := time.Parse("2006-01-02", "2024-12-02")

	result, err := reader.QueryLabelsCardinality(context.Background(), start, end, 10, nil)
	require.NoError(t, err)

	require.Len(t, result.Days, 2)
	require.Equal(t, int64(10710), result.TotalUniqueValues) // 5000+100+5500+110
	require.Equal(t, 2, result.TotalLabels)                  // 2 unique labels

	// Check first day
	require.Equal(t, "2024-12-01", result.Days[0].Date)
	require.Len(t, result.Days[0].Labels, 2)
	require.Equal(t, "pod", result.Days[0].Labels[0].LabelName)
	require.Equal(t, int64(5000), result.Days[0].Labels[0].UniqueValues)

	// Check second day
	require.Equal(t, "2024-12-02", result.Days[1].Date)
	require.Len(t, result.Days[1].Labels, 2)
	require.Equal(t, "pod", result.Days[1].Labels[0].LabelName)
	require.Equal(t, int64(5500), result.Days[1].Labels[0].UniqueValues)
}

func TestParquetIndexReader_QueryLabelsCardinality_Limit(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	entry := &LabelsEntry{
		Date:  "2024-12-01",
		Block: "2024/12/01",
		Labels: []LabelCardinality{
			{LabelName: "label_a", UniqueValues: 1000},
			{LabelName: "label_b", UniqueValues: 900},
			{LabelName: "label_c", UniqueValues: 800},
			{LabelName: "label_d", UniqueValues: 700},
			{LabelName: "label_e", UniqueValues: 600},
		},
		CachedAt: time.Now(),
	}
	err := writer.WriteLabelsIndex(entry)
	require.NoError(t, err)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	reader := NewParquetIndexReader(db, tmpDir)

	start, _ := time.Parse("2006-01-02", "2024-12-01")
	end, _ := time.Parse("2006-01-02", "2024-12-01")

	// Query with limit=3
	result, err := reader.QueryLabelsCardinality(context.Background(), start, end, 3, nil)
	require.NoError(t, err)

	require.Len(t, result.Days, 1)
	require.Len(t, result.Days[0].Labels, 3) // limited to 3
	require.Equal(t, "label_a", result.Days[0].Labels[0].LabelName)
	require.Equal(t, "label_b", result.Days[0].Labels[1].LabelName)
	require.Equal(t, "label_c", result.Days[0].Labels[2].LabelName)
}

func TestHasMetricsParquet(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	start, _ := time.Parse("2006-01-02", "2024-12-01")
	end, _ := time.Parse("2006-01-02", "2024-12-02")

	// Should return false when no files exist
	require.False(t, hasMetricsParquet(tmpDir, start, end))

	// Write a file in range
	entry := &MetricsEntry{
		Date:     "2024-12-01",
		Block:    "2024/12/01",
		Metrics:  []MetricCardinality{{MetricName: "test", SeriesCount: 100}},
		CachedAt: time.Now(),
	}
	err := writer.WriteMetricsIndex(entry)
	require.NoError(t, err)

	// Should return true now
	require.True(t, hasMetricsParquet(tmpDir, start, end))

	// Out of range should return false
	outStart, _ := time.Parse("2006-01-02", "2024-11-01")
	outEnd, _ := time.Parse("2006-01-02", "2024-11-30")
	require.False(t, hasMetricsParquet(tmpDir, outStart, outEnd))
}

func TestParquetIndexWriter_EmptyMetrics(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	entry := &MetricsEntry{
		Date:     "2024-12-01",
		Block:    "2024/12/01",
		Metrics:  []MetricCardinality{}, // empty
		CachedAt: time.Now(),
	}

	err := writer.WriteMetricsIndex(entry)
	require.NoError(t, err)

	// File should exist but be empty when read
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	path := filepath.Join(tmpDir, metricsParquetDir, "2024-12-01.parquet")
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM read_parquet('" + path + "')").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestParquetIndexWriter_EmptyLabels(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	entry := &LabelsEntry{
		Date:     "2024-12-01",
		Block:    "2024/12/01",
		Labels:   []LabelCardinality{}, // empty
		CachedAt: time.Now(),
	}

	err := writer.WriteLabelsIndex(entry)
	require.NoError(t, err)

	// File should exist but be empty when read
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	path := filepath.Join(tmpDir, labelsParquetDir, "2024-12-01.parquet")
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM read_parquet('" + path + "')").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestParquetIndexWriter_AtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	writer := NewParquetIndexWriter(tmpDir)

	entry := &MetricsEntry{
		Date:    "2024-12-01",
		Block:   "2024/12/01",
		Metrics: []MetricCardinality{{MetricName: "test", SeriesCount: 100}},

		CachedAt: time.Now(),
	}

	// Write first version
	err := writer.WriteMetricsIndex(entry)
	require.NoError(t, err)

	// Write second version (should overwrite atomically)
	entry.Metrics[0].SeriesCount = 200
	err = writer.WriteMetricsIndex(entry)
	require.NoError(t, err)

	// Verify the new value
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	path := filepath.Join(tmpDir, metricsParquetDir, "2024-12-01.parquet")
	var count int64
	err = db.QueryRow("SELECT series_count FROM read_parquet('" + path + "')").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, int64(200), count)

	// Verify no temp files left behind
	files, err := os.ReadDir(filepath.Join(tmpDir, metricsParquetDir))
	require.NoError(t, err)
	for _, f := range files {
		require.False(t, filepath.Ext(f.Name()) == ".tmp", "temp file should not exist: %s", f.Name())
	}
}

// hasMetricsParquet checks if metrics parquet files exist for the given date range.
// Test-only helper function.
func hasMetricsParquet(cacheDir string, start, end time.Time) bool {
	dir := filepath.Join(cacheDir, metricsParquetDir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return false
	}

	startDate := start.Format("2006-01-02")
	endDate := end.Format("2006-01-02")

	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".parquet") {
			continue
		}
		date := strings.TrimSuffix(name, ".parquet")
		if date >= startDate && date <= endDate {
			return true
		}
	}

	return false
}
