// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"

	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/locate"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

// createDownsampledBlock is a helper function that creates a raw TSDB block and downsamples it.
// It returns the downsampled block ready for testing.
func createDownsampledBlock(t *testing.T, seriesCount, samplesPerSeries int) (*tsdb.Block, func()) {
	t.Helper()
	ctx := context.Background()
	tempDir := t.TempDir()

	dbDir := filepath.Join(tempDir, "raw-db")
	db, err := tsdb.Open(dbDir, nil, nil, tsdb.DefaultOptions(), nil)
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}

	app := db.Appender(ctx)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

	for i := range seriesCount {
		lbls := labels.FromStrings(
			"__name__", fmt.Sprintf("test_metric_%d", i),
			"label1", fmt.Sprintf("value_%d", i),
			"label2", fmt.Sprintf("value_%d", i),
		)

		// Add samples at 1-minute intervals
		for j := range samplesPerSeries {
			timestamp := baseTime + int64(j*60*1000) // 1 minute intervals
			value := float64(100 + (i % 2 * 100))    // First series 100, second 200, then 100, 200 etc.

			if _, err := app.Append(0, lbls, timestamp, value); err != nil {
				t.Fatalf("failed to append sample: %v", err)
			}
		}
	}

	if err := app.Commit(); err != nil {
		t.Fatalf("failed to commit samples: %v", err)
	}

	if err := db.Compact(ctx); err != nil {
		t.Fatalf("failed to compact: %v", err)
	}

	blocks := db.Blocks()
	if len(blocks) == 0 {
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close db: %v", err)
		}

		db, err = tsdb.Open(dbDir, nil, nil, tsdb.DefaultOptions(), nil)
		if err != nil {
			t.Fatalf("failed to reopen TSDB: %v", err)
		}

		blocks = db.Blocks()
		if len(blocks) == 0 {
			t.Fatal("no blocks created after compaction and reopen")
		}
	}

	rawBlock := blocks[0]
	t.Logf("Created raw block with %d series, time range: [%d, %d]",
		rawBlock.Meta().Stats.NumSeries, rawBlock.MinTime(), rawBlock.MaxTime())

	rawMeta := &metadata.Meta{
		BlockMeta: rawBlock.Meta(),
		Thanos: metadata.Thanos{
			Downsample: metadata.ThanosDownsample{
				Resolution: downsample.ResLevel0, // Raw resolution
			},
		},
	}

	downsampledDir := filepath.Join(tempDir, "downsampled")
	if err := os.MkdirAll(downsampledDir, 0755); err != nil {
		t.Fatalf("failed to create downsampled directory: %v", err)
	}

	logger := log.NewNopLogger()
	downsampledID, err := downsample.Downsample(
		ctx,
		logger,
		rawMeta,
		rawBlock,
		downsampledDir,
		downsample.ResLevel1, // 5m resolution
	)
	if err != nil {
		t.Fatalf("failed to downsample block: %v", err)
	}

	downsampledBlockPath := filepath.Join(downsampledDir, downsampledID.String())
	downsampledBlock, err := tsdb.OpenBlock(nil, downsampledBlockPath, downsample.NewPool(), nil)
	if err != nil {
		t.Fatalf("failed to open downsampled block: %v", err)
	}

	downsampledMeta, err := metadata.ReadFromDir(downsampledBlockPath)
	if err != nil {
		t.Fatalf("failed to read downsampled metadata: %v", err)
	}
	if downsampledMeta.Thanos.Downsample.Resolution != downsample.ResLevel1 {
		t.Fatalf("expected downsampled resolution %d, got %d",
			downsample.ResLevel1, downsampledMeta.Thanos.Downsample.Resolution)
	}
	t.Logf("Successfully created downsampled block with resolution: %d", downsampledMeta.Thanos.Downsample.Resolution)

	cleanup := func() {
		// Close the block first, then close the DB
		// Note: The caller should close any readers before calling cleanup
		if err := downsampledBlock.Close(); err != nil {
			t.Logf("error closing downsampled block: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Logf("error closing db: %v", err)
		}
	}

	return downsampledBlock, cleanup
}

func TestReadingDownsampledWithShardedIndexRowReader(t *testing.T) {
	ctx := t.Context()
	seriesCount := 2
	samplesPerSeries := 1000

	downsampledBlock, cleanup := createDownsampledBlock(t, seriesCount, samplesPerSeries)
	defer cleanup()

	mint := downsampledBlock.MinTime()
	maxt := downsampledBlock.MaxTime()

	cfg := convertOpts{
		rowGroupSize:        1_000_000,
		numRowGroups:        6,
		sortingColumns:      [][]string{{schema.LabelNameToColumn(labels.MetricName)}},
		bloomfilterColumns:  [][]string{{schema.LabelNameToColumn(labels.MetricName)}},
		labelBufferPool:     parquet.NewBufferPool(),
		chunkbufferPool:     parquet.NewBufferPool(),
		encodingConcurrency: 1,
		labelPageBufferSize: int(256 * units.KiB),
		chunkPageBufferSize: int(2 * units.MiB),
		writeConcurrency:    1,
		downsampled:         true,
	}

	readers, err := shardedIndexRowReader(ctx, mint, maxt, []Convertible{downsampledBlock}, cfg)
	if err != nil {
		t.Fatalf("failed to create sharded index row reader: %v", err)
	}

	if len(readers) != 1 {
		t.Fatalf("expected 1 sharded reader, got %d", len(readers))
	}

	reader := readers[0]
	defer reader.Close()

	if reader.schema == nil {
		t.Fatal("expected schema to be non-nil")
	}

	if reader.rowBuilder == nil {
		t.Fatal("expected rowBuilder to be non-nil")
	}

	if reader.concurrency != cfg.encodingConcurrency {
		t.Fatalf("expected concurrency %d, got %d", cfg.encodingConcurrency, reader.concurrency)
	}

	// Test reading rows from the downsampled block
	buf := make([]parquet.Row, 2)
	n, err := reader.ReadRows(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read rows: %v", err)
	}

	if n != 2 {
		t.Fatalf("expected to read at two rows, got %v", n)
	}

	t.Logf("Successfully read %d rows from downsampled block", n)

	expectedValues := [][]float64{
		// Indexed by downsample.AggrType: AggrCount, AggrSum, AggrMin, AggrMax, AggrCounter
		{5, 500.0, 100.0, 100.0, 100.0},  // First row expected aggregates
		{5, 1000.0, 200.0, 200.0, 200.0}, // Second row expected aggregates
	}

	// Verify the rows have the expected structure
	for i := range n {
		row := buf[i]
		if len(row) == 0 {
			t.Fatalf("row %d is empty", i)
		}

		// Check for required columns
		hasLabelHash := false
		hasLabelIndex := false
		hasAggregateColumns := false

		for _, val := range row {
			colName := reader.schema.Columns()[val.Column()][0]
			if colName == schema.LabelHashColumn {
				hasLabelHash = true
			}
			if colName == schema.LabelIndexColumn {
				hasLabelIndex = true
			}
			if colName == schema.CountColumn ||
				colName == schema.SumColumn ||
				colName == schema.MinColumn ||
				colName == schema.MaxColumn ||
				colName == schema.CounterColumn {
				hasAggregateColumns = true
			}
		}

		if !hasLabelHash {
			t.Errorf("row %d missing label hash column", i)
		}
		if !hasLabelIndex {
			t.Errorf("row %d missing label index column", i)
		}
		if !hasAggregateColumns {
			t.Errorf("row %d missing aggregate columns", i)
		}

		row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
			colName := reader.schema.Columns()[columnIndex][0]

			switch colName {
			case schema.CountColumn:
				checkSamples(t, columnValues[0].ByteArray(), downsample.AggrCount, expectedValues[i][downsample.AggrCount])
			case schema.CounterColumn:
				checkSamples(t, columnValues[0].ByteArray(), downsample.AggrCounter, expectedValues[i][downsample.AggrCounter])
			case schema.MaxColumn:
				checkSamples(t, columnValues[0].ByteArray(), downsample.AggrMax, expectedValues[i][downsample.AggrMax])
			case schema.MinColumn:
				checkSamples(t, columnValues[0].ByteArray(), downsample.AggrMin, expectedValues[i][downsample.AggrMin])
			case schema.SumColumn:
				checkSamples(t, columnValues[0].ByteArray(), downsample.AggrSum, expectedValues[i][downsample.AggrSum])
			}

			return true
		})

	}

	// Test that schema contains expected columns
	schemaColumns := reader.schema.Columns()
	if len(schemaColumns) == 0 {
		t.Fatal("schema has no columns")
	}

	// Verify all columns exist
	if _, ok := reader.schema.Lookup(schema.LabelHashColumn); !ok {
		t.Errorf("schema missing %s", schema.LabelHashColumn)
	}
	if _, ok := reader.schema.Lookup(schema.LabelIndexColumn); !ok {
		t.Errorf("schema missing %s", schema.LabelIndexColumn)
	}
	for _, col := range []string{
		schema.SumColumn,
		schema.MinColumn,
		schema.MaxColumn,
		schema.CounterColumn,
	} {
		if _, ok := reader.schema.Lookup(col); !ok {
			t.Errorf("schema missing %s", col)
		}
	}
}

func convertSamples(t *testing.T, bts []byte, aggrType downsample.AggrType) []sample {
	values, err := BinarySamplesToString(bts, aggrType)
	if err != nil {
		t.Errorf("failed to get samples for aggrType %v: %v", aggrType, err)
	}
	return values
}

func checkSamples(t *testing.T, bts []byte, aggrType downsample.AggrType, expectedValue float64) {
	values := convertSamples(t, bts, aggrType)

	for _, s := range values {
		if s.v != expectedValue {
			t.Errorf("expected value %f, got %f at timestamp %d for aggrType %v", expectedValue, s.v, s.t, aggrType)
		}
	}
}

func BinarySamplesToString(bs []byte, aggrType downsample.AggrType) ([]sample, error) {
	length := binary.BigEndian.Uint64(bs[:8])
	bs = bs[8:]

	parts := make([]sample, 0, length)

	for len(bs) != 0 {
		timestamp := binary.BigEndian.Uint64(bs[:8])
		bs = bs[8:]

		value := binary.BigEndian.Uint64(bs[:8])
		bs = bs[8:]

		if aggrType == downsample.AggrCount || aggrType == downsample.AggrCounter {
			parts = append(parts, sample{t: int64(timestamp), v: float64(value)})
			continue
		}
		// For other aggregates, decode float64 value.
		floatValue := math.Float64frombits(value)
		if math.IsNaN(floatValue) {
			// Make this an error?
			parts = append(parts, sample{t: int64(timestamp), v: math.NaN()})
		} else {
			parts = append(parts, sample{t: int64(timestamp), v: floatValue})
		}
	}

	return parts, nil
}

func TestConversionOfDownsampled(t *testing.T) {
	bkt, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatalf("unable to create bucket: %s", err)
	}
	t.Cleanup(func() { _ = bkt.Close() })

	downsampledBlock, cleanup := createDownsampledBlock(t, 10, 1000)
	defer cleanup()
	startTime := util.NewDate(2024, 1, 1)

	opts := []ConvertOption{
		SortBy(labels.MetricName),
		RowGroupSize(250),
		RowGroupCount(2),
		LabelPageBufferSize(units.KiB), // results in 2 pages
		Downsampled(true),
	}
	if err := ConvertTSDBBlock(t.Context(), bkt, startTime, []Convertible{downsampledBlock}, opts...); err != nil {
		t.Fatalf("unable to convert tsdb block: %s", err)
	}

	// Check contents of bucket
	discoverer := locate.NewDiscoverer(bkt)
	if err := discoverer.Discover(t.Context()); err != nil {
		t.Fatalf("unable to convert parquet block: %s", err)
	}
	metas := discoverer.Metas()

	if n := len(metas); n != 1 {
		t.Fatalf("unexpected number of metas: %d", n)
	}
	meta := metas[slices.Collect(maps.Keys(metas))[0]]

	if n := meta.Shards; n != 1 {
		t.Fatalf("unexpected number of shards: %d", n)
	}

	totalRows := int64(0)
	for i := range int(meta.Shards) {
		lf, err := loadParquetFile(t.Context(), bkt, schema.LabelsPfileNameForShard(meta.Name, i))
		if err != nil {
			t.Fatalf("unable to load label parquet file for shard %d: %s", i, err)
		}
		cf, err := loadParquetFile(t.Context(), bkt, schema.ChunksPfileNameForShard(meta.Name, i))
		if err != nil {
			t.Fatalf("unable to load chunk parquet file for shard %d: %s", i, err)
		}
		if cf.NumRows() != lf.NumRows() {
			t.Fatalf("labels and chunk file have different numbers of rows for shard %d", i)
		}
		totalRows += lf.NumRows()

		for _, rg := range cf.RowGroups() {
			r := rg.Rows()
			rows := make([]parquet.Row, 10)
			n, err := r.ReadRows(rows)
			if err != nil && err != io.EOF {
				t.Fatalf("unable to read rows from chunk file for shard %d: %s", i, err)
			}
			if err == io.EOF && n == 0 {
				continue
			}
			for j := 0; j < n; j++ {
				row := rows[j]
				row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
					colName := cf.Schema().Columns()[columnIndex][0]
					var count int
					var skip bool
					switch colName {
					case schema.CountColumn:
						count = len(convertSamples(t, columnValues[0].ByteArray(), downsample.AggrCount))
					case schema.CounterColumn:
						count = len(convertSamples(t, columnValues[0].ByteArray(), downsample.AggrCounter))
					case schema.MaxColumn:
						count = len(convertSamples(t, columnValues[0].ByteArray(), downsample.AggrMax))
					case schema.MinColumn:
						count = len(convertSamples(t, columnValues[0].ByteArray(), downsample.AggrMin))
					case schema.SumColumn:
						count = len(convertSamples(t, columnValues[0].ByteArray(), downsample.AggrSum))
					case schema.LabelHashColumn, schema.LabelIndexColumn, schema.LabelColumnPrefix:
						skip = true
					default:
						t.Fatalf("unexpected column %s", colName)
					}

					if !skip && count == 0 {
						t.Fatalf("no samples found in column %s", colName)
					}
					return true
				})
			}
		}
	}
	if totalRows != int64(10) {
		t.Fatalf("too few rows: %d", totalRows)
	}
}
