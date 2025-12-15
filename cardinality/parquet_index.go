// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

const (
	metricsParquetDir = "metrics"
	labelsParquetDir  = "labels"
)

type MetricsIndexRow struct {
	Date        string `parquet:"date,dict"`
	Block       string `parquet:"block,dict"`
	MetricName  string `parquet:"metric_name,dict"`
	SeriesCount int64  `parquet:"series_count"`
	CachedAt    int64  `parquet:"cached_at,timestamp(millisecond)"`
}

type LabelsIndexRow struct {
	Date         string `parquet:"date,dict"`
	Block        string `parquet:"block,dict"`
	LabelName    string `parquet:"label_name,dict"`
	UniqueValues int64  `parquet:"unique_values"`
	CachedAt     int64  `parquet:"cached_at,timestamp(millisecond)"`
}

type ParquetIndexWriter struct {
	cacheDir string
}

func NewParquetIndexWriter(cacheDir string) *ParquetIndexWriter {
	return &ParquetIndexWriter{cacheDir: cacheDir}
}

func (w *ParquetIndexWriter) WriteMetricsIndex(entry *MetricsEntry) error {
	dir := filepath.Join(w.cacheDir, metricsParquetDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create metrics dir: %w", err)
	}

	rows := make([]MetricsIndexRow, len(entry.Metrics))
	cachedAtMs := entry.CachedAt.UnixMilli()

	for i, mc := range entry.Metrics {
		rows[i] = MetricsIndexRow{
			Date:        entry.Date,
			Block:       entry.Block,
			MetricName:  mc.MetricName,
			SeriesCount: mc.SeriesCount,
			CachedAt:    cachedAtMs,
		}
	}

	path := filepath.Join(dir, entry.Date+".parquet")
	tmpPath := path + ".tmp"

	if err := writeParquetFile(tmpPath, rows); err != nil {
		return fmt.Errorf("write metrics parquet: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename metrics parquet: %w", err)
	}

	return nil
}

func (w *ParquetIndexWriter) WriteLabelsIndex(entry *LabelsEntry) error {
	dir := filepath.Join(w.cacheDir, labelsParquetDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create labels dir: %w", err)
	}

	rows := make([]LabelsIndexRow, len(entry.Labels))
	cachedAtMs := entry.CachedAt.UnixMilli()

	for i, lc := range entry.Labels {
		rows[i] = LabelsIndexRow{
			Date:         entry.Date,
			Block:        entry.Block,
			LabelName:    lc.LabelName,
			UniqueValues: lc.UniqueValues,
			CachedAt:     cachedAtMs,
		}
	}

	path := filepath.Join(dir, entry.Date+".parquet")
	tmpPath := path + ".tmp"

	if err := writeParquetFile(tmpPath, rows); err != nil {
		return fmt.Errorf("write labels parquet: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename labels parquet: %w", err)
	}

	return nil
}

func writeParquetFile[T any](path string, rows []T) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[T](f,
		parquet.Compression(&zstd.Codec{Level: zstd.DefaultLevel}),
	)

	if len(rows) > 0 {
		if _, err := writer.Write(rows); err != nil {
			return fmt.Errorf("write rows: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	return nil
}

type ParquetIndexReader struct {
	db       *sql.DB
	cacheDir string
}

func NewParquetIndexReader(db *sql.DB, cacheDir string) *ParquetIndexReader {
	return &ParquetIndexReader{db: db, cacheDir: cacheDir}
}

func (r *ParquetIndexReader) QueryMetricsCardinality(
	ctx context.Context,
	start, end time.Time,
	limit int,
	matchers []*Matcher,
) (*CardinalityResult, error) {
	startDate := start.Format("2006-01-02")
	endDate := end.Format("2006-01-02")

	nameMatchers, labelMatchers := SplitMatchers(matchers)

	if len(labelMatchers) > 0 {
		return r.queryMetricsFromRawParquet(ctx, start, end, limit, matchers)
	}

	return r.queryMetricsFromIndex(ctx, startDate, endDate, start, end, limit, nameMatchers)
}

func (r *ParquetIndexReader) queryMetricsFromIndex(
	ctx context.Context,
	startDate, endDate string,
	start, end time.Time,
	limit int,
	nameMatchers []*Matcher,
) (*CardinalityResult, error) {
	glob := filepath.Join(r.cacheDir, metricsParquetDir, "*.parquet")

	var queryBuilder strings.Builder
	queryBuilder.WriteString(`
		WITH filtered AS (
			SELECT date, metric_name, series_count
			FROM read_parquet('` + glob + `')
			WHERE date >= ? AND date <= ?
	`)

	args := []any{startDate, endDate}

	for _, m := range nameMatchers {
		clause, matcherArgs := m.toSQLColumn("metric_name")
		queryBuilder.WriteString(` AND ` + clause)
		args = append(args, matcherArgs...)
	}

	queryBuilder.WriteString(`
		),
		daily_totals AS (
			SELECT date, SUM(series_count) as day_total
			FROM filtered
			GROUP BY date
		),
		aggregated AS (
			SELECT
				f.date,
				f.metric_name,
				SUM(f.series_count) as series_count
			FROM filtered f
			GROUP BY f.date, f.metric_name
		)
		SELECT
			a.date,
			a.metric_name,
			a.series_count,
			CASE WHEN dt.day_total > 0
				THEN 100.0 * a.series_count / dt.day_total
				ELSE 0
			END as percentage
		FROM aggregated a
		JOIN daily_totals dt ON a.date = dt.date
		ORDER BY a.date, a.series_count DESC
	`)

	rows, err := r.db.QueryContext(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query metrics cardinality: %w", err)
	}
	defer rows.Close()

	var days []DailyMetrics
	var currentDay *DailyMetrics
	var totalSeries int64
	uniqueMetrics := make(map[string]struct{})

	for rows.Next() {
		var date, metricNameVal string
		var seriesCount int64
		var percentage float64

		if err := rows.Scan(&date, &metricNameVal, &seriesCount, &percentage); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		totalSeries += seriesCount
		uniqueMetrics[metricNameVal] = struct{}{}

		if currentDay == nil || currentDay.Date != date {
			days = append(days, DailyMetrics{Date: date})
			currentDay = &days[len(days)-1]
		}

		if limit > 0 && len(currentDay.Metrics) >= limit {
			continue
		}

		currentDay.Metrics = append(currentDay.Metrics, MetricCardinality{
			MetricName:  metricNameVal,
			SeriesCount: seriesCount,
			Percentage:  percentage,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	blocksAnalyzed := len(days)

	days = fillMissingDays(days, start, end)

	return &CardinalityResult{
		Start:          start,
		End:            end,
		Days:           days,
		TotalSeries:    totalSeries,
		TotalMetrics:   int64(len(uniqueMetrics)),
		BlocksAnalyzed: blocksAnalyzed,
	}, nil
}

func (r *ParquetIndexReader) queryMetricsFromRawParquet(
	ctx context.Context,
	start, end time.Time,
	limit int,
	matchers []*Matcher,
) (*CardinalityResult, error) {
	glob := filepath.Join(r.cacheDir, "**", "*.labels.parquet")
	startDate := start.Format("2006-01-02")
	endDate := end.Format("2006-01-02")
	nameColumn := schema.LabelNameToColumn("__name__")

	whereClause, matcherArgs := MatchersToSQL(matchers)

	query := fmt.Sprintf(`
		WITH source AS (
			SELECT
				regexp_extract(filename, '(\d{4})/(\d{2})/(\d{2})/', 0) AS date_path,
				"%s" AS metric_name
			FROM read_parquet('%s', union_by_name = true, filename = true)
			WHERE %s
		),
		with_date AS (
			SELECT
				substr(date_path, 1, 4) || '-' || substr(date_path, 6, 2) || '-' || substr(date_path, 9, 2) AS date,
				metric_name
			FROM source
			WHERE date_path IS NOT NULL AND date_path != ''
		),
		filtered AS (
			SELECT date, metric_name, COUNT(*) as series_count
			FROM with_date
			WHERE date >= ? AND date <= ?
			GROUP BY date, metric_name
		),
		daily_totals AS (
			SELECT date, SUM(series_count) as day_total
			FROM filtered
			GROUP BY date
		)
		SELECT
			f.date,
			f.metric_name,
			f.series_count,
			CASE WHEN dt.day_total > 0
				THEN 100.0 * f.series_count / dt.day_total
				ELSE 0
			END as percentage
		FROM filtered f
		JOIN daily_totals dt ON f.date = dt.date
		ORDER BY f.date, f.series_count DESC
	`, nameColumn, glob, whereClause)

	args := append(matcherArgs, startDate, endDate)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query metrics from raw parquet: %w", err)
	}
	defer rows.Close()

	var days []DailyMetrics
	var currentDay *DailyMetrics
	var totalSeries int64
	uniqueMetrics := make(map[string]struct{})

	for rows.Next() {
		var date, metricNameVal string
		var seriesCount int64
		var percentage float64

		if err := rows.Scan(&date, &metricNameVal, &seriesCount, &percentage); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		totalSeries += seriesCount
		uniqueMetrics[metricNameVal] = struct{}{}

		if currentDay == nil || currentDay.Date != date {
			days = append(days, DailyMetrics{Date: date})
			currentDay = &days[len(days)-1]
		}

		if limit > 0 && len(currentDay.Metrics) >= limit {
			continue
		}

		currentDay.Metrics = append(currentDay.Metrics, MetricCardinality{
			MetricName:  metricNameVal,
			SeriesCount: seriesCount,
			Percentage:  percentage,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	blocksAnalyzed := len(days)

	days = fillMissingDays(days, start, end)

	return &CardinalityResult{
		Start:          start,
		End:            end,
		Days:           days,
		TotalSeries:    totalSeries,
		TotalMetrics:   int64(len(uniqueMetrics)),
		BlocksAnalyzed: blocksAnalyzed,
	}, nil
}

func (r *ParquetIndexReader) QueryLabelsCardinality(
	ctx context.Context,
	start, end time.Time,
	limit int,
	matchers []*Matcher,
) (*LabelsCardinalityPerDayResult, error) {
	startDate := start.Format("2006-01-02")
	endDate := end.Format("2006-01-02")

	if len(matchers) == 0 {
		glob := filepath.Join(r.cacheDir, labelsParquetDir, "*.parquet")
		return r.queryLabelsFromIndex(ctx, glob, startDate, endDate, start, end, limit)
	}

	return r.queryLabelsFromRawParquetWithMatchers(ctx, startDate, endDate, start, end, limit, matchers)
}

func (r *ParquetIndexReader) queryLabelsFromIndex(
	ctx context.Context,
	glob string,
	startDate, endDate string,
	start, end time.Time,
	limit int,
) (*LabelsCardinalityPerDayResult, error) {
	query := `
		WITH filtered AS (
			SELECT date, label_name, unique_values
			FROM read_parquet('` + glob + `')
			WHERE date >= ? AND date <= ?
		),
		daily_totals AS (
			SELECT date, SUM(unique_values) as day_total
			FROM filtered
			GROUP BY date
		),
		aggregated AS (
			SELECT
				f.date,
				f.label_name,
				SUM(f.unique_values) as unique_values
			FROM filtered f
			GROUP BY f.date, f.label_name
		)
		SELECT
			a.date,
			a.label_name,
			a.unique_values,
			CASE WHEN dt.day_total > 0
				THEN 100.0 * a.unique_values / dt.day_total
				ELSE 0
			END as percentage
		FROM aggregated a
		JOIN daily_totals dt ON a.date = dt.date
		ORDER BY a.date, a.unique_values DESC
	`

	rows, err := r.db.QueryContext(ctx, query, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("query labels cardinality: %w", err)
	}
	defer rows.Close()

	return r.scanLabelsResult(rows, start, end, limit)
}

func (r *ParquetIndexReader) queryLabelsFromRawParquet(
	ctx context.Context,
	startDate, endDate string,
	start, end time.Time,
	limit int,
	metricName string,
) (*LabelsCardinalityPerDayResult, error) {
	glob := filepath.Join(r.cacheDir, "**", "*.labels.parquet")
	nameColumn := schema.LabelNameToColumn("__name__")

	columns, err := r.queryLabelColumns(ctx, glob, nameColumn)
	if err != nil {
		return nil, fmt.Errorf("query label columns: %w", err)
	}

	if len(columns) == 0 {
		days := fillMissingLabelDays(nil, start, end)
		return &LabelsCardinalityPerDayResult{
			Start:          start,
			End:            end,
			Days:           days,
			BlocksAnalyzed: 0,
		}, nil
	}

	labels, totalUniqueValues, err := r.queryFilteredLabelsCardinality(ctx, glob, columns, nameColumn, metricName, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("query filtered labels: %w", err)
	}

	return r.buildLabelsResult(labels, totalUniqueValues, start, end, limit)
}

func (r *ParquetIndexReader) queryLabelsFromRawParquetWithMatchers(
	ctx context.Context,
	startDate, endDate string,
	start, end time.Time,
	limit int,
	matchers []*Matcher,
) (*LabelsCardinalityPerDayResult, error) {
	glob := filepath.Join(r.cacheDir, "**", "*.labels.parquet")
	nameColumn := schema.LabelNameToColumn("__name__")

	columns, err := r.queryLabelColumns(ctx, glob, nameColumn)
	if err != nil {
		return nil, fmt.Errorf("query label columns: %w", err)
	}

	if len(columns) == 0 {
		days := fillMissingLabelDays(nil, start, end)
		return &LabelsCardinalityPerDayResult{
			Start:          start,
			End:            end,
			Days:           days,
			BlocksAnalyzed: 0,
		}, nil
	}

	whereClause, matcherArgs := MatchersToSQL(matchers)

	labels, totalUniqueValues, err := r.queryFilteredLabelsCardinalityWithMatchers(ctx, glob, columns, whereClause, matcherArgs, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("query filtered labels: %w", err)
	}

	return r.buildLabelsResult(labels, totalUniqueValues, start, end, limit)
}

func (r *ParquetIndexReader) queryFilteredLabelsCardinalityWithMatchers(
	ctx context.Context,
	glob string,
	columns []string,
	whereClause string,
	matcherArgs []any,
	startDate, endDate string,
) ([]labelCardinalityRow, int64, error) {
	var unionParts []string
	for _, col := range columns {
		labelName := schema.ColumnToLabelName(col)
		unionParts = append(unionParts, fmt.Sprintf(`
			SELECT 
				regexp_extract(filename, '(\d{4})/(\d{2})/(\d{2})/', 0) AS date_path,
				'%s' AS label_name,
				APPROX_COUNT_DISTINCT("%s") AS unique_values
			FROM read_parquet('%s', union_by_name = true, filename = true)
			WHERE %s
			GROUP BY date_path
		`, labelName, col, glob, whereClause))
	}

	query := fmt.Sprintf(`
		WITH raw_data AS (
			%s
		),
		with_date AS (
			SELECT 
				substr(date_path, 1, 4) || '-' || substr(date_path, 6, 2) || '-' || substr(date_path, 9, 2) AS date,
				label_name,
				unique_values
			FROM raw_data
			WHERE date_path IS NOT NULL AND date_path != ''
		),
		filtered AS (
			SELECT date, label_name, SUM(unique_values) as unique_values
			FROM with_date
			WHERE date >= ? AND date <= ?
			GROUP BY date, label_name
		)
		SELECT date, label_name, unique_values
		FROM filtered
		ORDER BY date, unique_values DESC
	`, strings.Join(unionParts, " UNION ALL "))

	var args []any
	for range columns {
		args = append(args, matcherArgs...)
	}
	args = append(args, startDate, endDate)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query filtered labels cardinality with matchers: %w", err)
	}
	defer rows.Close()

	var results []labelCardinalityRow
	var totalUniqueValues int64

	for rows.Next() {
		var row labelCardinalityRow
		if err := rows.Scan(&row.date, &row.labelName, &row.uniqueValues); err != nil {
			return nil, 0, fmt.Errorf("scan row: %w", err)
		}
		results = append(results, row)
		totalUniqueValues += row.uniqueValues
	}

	return results, totalUniqueValues, rows.Err()
}

func (r *ParquetIndexReader) queryLabelColumns(ctx context.Context, glob, nameColumn string) ([]string, error) {
	schemaQuery := fmt.Sprintf(`
		SELECT column_name
		FROM (DESCRIBE SELECT * FROM read_parquet('%s', union_by_name = true))
		WHERE column_name LIKE '%s%%'
		  AND column_name != '%s'
	`, glob, schema.LabelColumnPrefix, nameColumn)

	rows, err := r.db.QueryContext(ctx, schemaQuery)
	if err != nil {
		return nil, fmt.Errorf("query schema: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan column name: %w", err)
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

type labelCardinalityRow struct {
	date         string
	labelName    string
	uniqueValues int64
}

func (r *ParquetIndexReader) queryFilteredLabelsCardinality(
	ctx context.Context,
	glob string,
	columns []string,
	nameColumn string,
	metricName string,
	startDate, endDate string,
) ([]labelCardinalityRow, int64, error) {
	var unionParts []string
	for _, col := range columns {
		labelName := schema.ColumnToLabelName(col)
		unionParts = append(unionParts, fmt.Sprintf(`
			SELECT
				regexp_extract(filename, '(\d{4})/(\d{2})/(\d{2})/', 0) AS date_path,
				'%s' AS label_name,
				APPROX_COUNT_DISTINCT("%s") AS unique_values
			FROM read_parquet('%s', union_by_name = true, filename = true)
			WHERE "%s" = '%s'
			GROUP BY date_path
		`, labelName, col, glob, nameColumn, metricName))
	}

	query := fmt.Sprintf(`
		WITH raw_data AS (
			%s
		),
		with_date AS (
			SELECT 
				substr(date_path, 1, 4) || '-' || substr(date_path, 6, 2) || '-' || substr(date_path, 9, 2) AS date,
				label_name,
				unique_values
			FROM raw_data
			WHERE date_path IS NOT NULL AND date_path != ''
		),
		filtered AS (
			SELECT date, label_name, SUM(unique_values) as unique_values
			FROM with_date
			WHERE date >= '%s' AND date <= '%s'
			GROUP BY date, label_name
		)
		SELECT date, label_name, unique_values
		FROM filtered
		ORDER BY date, unique_values DESC
	`, strings.Join(unionParts, " UNION ALL "), startDate, endDate)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("query filtered labels cardinality: %w", err)
	}
	defer rows.Close()

	var results []labelCardinalityRow
	var totalUniqueValues int64

	for rows.Next() {
		var row labelCardinalityRow
		if err := rows.Scan(&row.date, &row.labelName, &row.uniqueValues); err != nil {
			return nil, 0, fmt.Errorf("scan row: %w", err)
		}
		results = append(results, row)
		totalUniqueValues += row.uniqueValues
	}

	return results, totalUniqueValues, rows.Err()
}

func (r *ParquetIndexReader) buildLabelsResult(
	labels []labelCardinalityRow,
	totalUniqueValues int64,
	start, end time.Time,
	limit int,
) (*LabelsCardinalityPerDayResult, error) {
	dayMap := make(map[string][]LabelCardinality)
	dayTotals := make(map[string]int64)
	uniqueLabels := make(map[string]struct{})

	for _, row := range labels {
		dayTotals[row.date] += row.uniqueValues
		uniqueLabels[row.labelName] = struct{}{}
	}

	for _, row := range labels {
		pct := 0.0
		if dayTotals[row.date] > 0 {
			pct = 100.0 * float64(row.uniqueValues) / float64(dayTotals[row.date])
		}
		dayMap[row.date] = append(dayMap[row.date], LabelCardinality{
			LabelName:    row.labelName,
			UniqueValues: row.uniqueValues,
			Percentage:   pct,
		})
	}

	var dates []string
	for date := range dayMap {
		dates = append(dates, date)
	}
	sort.Strings(dates)

	var days []DailyLabels
	for _, date := range dates {
		labels := dayMap[date]
		sort.Slice(labels, func(i, j int) bool {
			return labels[i].UniqueValues > labels[j].UniqueValues
		})
		if limit > 0 && len(labels) > limit {
			labels = labels[:limit]
		}
		days = append(days, DailyLabels{
			Date:   date,
			Labels: labels,
		})
	}

	blocksAnalyzed := len(days)

	days = fillMissingLabelDays(days, start, end)

	return &LabelsCardinalityPerDayResult{
		Start:             start,
		End:               end,
		Days:              days,
		TotalLabels:       len(uniqueLabels),
		TotalUniqueValues: totalUniqueValues,
		BlocksAnalyzed:    blocksAnalyzed,
	}, nil
}

func (r *ParquetIndexReader) scanLabelsResult(
	rows *sql.Rows,
	start, end time.Time,
	limit int,
) (*LabelsCardinalityPerDayResult, error) {
	var days []DailyLabels
	var currentDay *DailyLabels
	var totalUniqueValues int64
	uniqueLabels := make(map[string]struct{})

	for rows.Next() {
		var date, labelName string
		var uniqueValues int64
		var percentage float64

		if err := rows.Scan(&date, &labelName, &uniqueValues, &percentage); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		totalUniqueValues += uniqueValues
		uniqueLabels[labelName] = struct{}{}

		if currentDay == nil || currentDay.Date != date {
			days = append(days, DailyLabels{Date: date})
			currentDay = &days[len(days)-1]
		}

		// Apply per-day limit
		if limit > 0 && len(currentDay.Labels) >= limit {
			continue
		}

		currentDay.Labels = append(currentDay.Labels, LabelCardinality{
			LabelName:    labelName,
			UniqueValues: uniqueValues,
			Percentage:   percentage,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	blocksAnalyzed := len(days)

	days = fillMissingLabelDays(days, start, end)

	return &LabelsCardinalityPerDayResult{
		Start:             start,
		End:               end,
		Days:              days,
		TotalLabels:       len(uniqueLabels),
		TotalUniqueValues: totalUniqueValues,
		BlocksAnalyzed:    blocksAnalyzed,
	}, nil
}
