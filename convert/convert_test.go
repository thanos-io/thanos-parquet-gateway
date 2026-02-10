// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/oklog/ulid/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/goleak"

	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/locate"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestConverter(t *testing.T) {
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatalf("unable to create bucket: %s", err)
	}
	t.Cleanup(func() { _ = bkt.Close() })

	app := st.Appender(t.Context())
	for i := range 1_000 {
		for range 120 {
			lbls := labels.FromStrings(
				"__name__", fmt.Sprintf("foo_%d", i/10),
				fmt.Sprintf("col_%d", i/100), fmt.Sprintf("%d", 2*i),
			)
			_, err := app.Append(0, lbls, time.Second.Milliseconds(), float64(i))
			if err != nil {
				t.Fatalf("unable to append sample: %s", err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("unable to commit samples: %s", err)
	}

	h := st.Head()
	ts := time.UnixMilli(h.MinTime()).UTC()
	d := util.NewDate(ts.Year(), ts.Month(), ts.Day())

	blkid := ulid.MustNewDefault(time.Now())

	t.Log(blkid.String())

	opts := []ConvertOption{
		SortBy(labels.MetricName),
		RowGroupSize(250),
		RowGroupCount(2),
		LabelPageBufferSize(units.KiB), // results in 2 pages
	}
	if err := ConvertTSDBBlock(t.Context(), bkt, d, schema.ExternalLabelsHash(0), []Convertible{&HeadBlock{Head: h, OverrideBLID: blkid.String()}}, opts...); err != nil {
		t.Fatalf("unable to convert tsdb block: %s", err)
	}

	discoverer := locate.NewDiscoverer(bkt)
	if err := discoverer.Discover(t.Context()); err != nil {
		t.Fatalf("unable to convert parquet block: %s", err)
	}
	streams := discoverer.Streams()

	if n := len(streams); n != 1 {
		t.Fatalf("unexpected number of metas: %d", n)
	}
	meta := streams[slices.Collect(maps.Keys(streams))[0]].Metas[0]

	if n := meta.Shards; n != 2 {
		t.Fatalf("unexpected number of shards: %d", n)
	}

	require.Contains(t, meta.ConvertedFromBLIDs, blkid)

	totalRows := int64(0)
	for i := range int(meta.Shards) {
		lf, err := loadParquetFile(t, bkt, schema.LabelsPfileNameForShard(schema.ExternalLabelsHash(0), meta.Date, i))
		if err != nil {
			t.Fatalf("unable to load label parquet file for shard %d: %s", i, err)
		}
		cf, err := loadParquetFile(t, bkt, schema.ChunksPfileNameForShard(schema.ExternalLabelsHash(0), meta.Date, i))
		if err != nil {
			t.Fatalf("unable to load chunk parquet file for shard %d: %s", i, err)
		}
		if cf.NumRows() != lf.NumRows() {
			t.Fatalf("labels and chunk file have different numbers of rows for shard %d", i)
		}
		totalRows += lf.NumRows()

		if err := hasNoNullColumns(lf); err != nil {
			t.Fatalf("unable to check for null columns: %s", err)
		}
		if err := hasExpectedIndexes(lf); err != nil {
			t.Fatalf("unable to check for null columns: %s", err)
		}
		if err := nameColumnPageBoundsAreAscending(lf); err != nil {
			t.Fatalf("unable to check that __name__ column page bounds are ascending: %s", err)
		}
		if err := nameColumnValuesAreIncreasing(t, lf); err != nil {
			t.Fatalf("unable to check that __name__ column values are increasing: %s", err)
		}
	}
	if totalRows != int64(st.DB.Head().NumSeries()) {
		t.Fatalf("too few rows: %d", totalRows)
	}
}

func TestConverterIndexWithManyLabelNames(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping, because 'short' flag was set")
	}

	// A parquet file can have 32767 columns. We create a column per unique label name
	// if we have too many unique label names in an index we need to compute its schema
	// per-shard. If we would do it for the whole index we would run into this limit otherwise.

	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatalf("unable to create bucket: %s", err)
	}
	t.Cleanup(func() { _ = bkt.Close() })

	app := st.Appender(t.Context())
	for i := range 34_000 {
		lbls := labels.FromStrings(
			"__name__", fmt.Sprintf("name_%d", i/100),
			fmt.Sprintf("col_0_%d", i), strconv.Itoa(1),
		)
		_, err := app.Append(0, lbls, time.Second.Milliseconds(), float64(i))
		if err != nil {
			t.Fatalf("unable to append sample: %s", err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("unable to commit samples: %s", err)
	}

	h := st.Head()
	ts := time.UnixMilli(h.MinTime()).UTC()
	d := util.NewDate(ts.Year(), ts.Month(), ts.Day())

	opts := []ConvertOption{
		SortBy(labels.MetricName),
		LabelPageBufferSize(units.KiB), // results in 2 pages
	}
	if err := ConvertTSDBBlock(t.Context(), bkt, d, schema.ExternalLabelsHash(0), []Convertible{&HeadBlock{Head: h, OverrideBLID: ulid.MustNewDefault(time.Now()).String()}}, opts...); err != nil {
		t.Fatalf("unable to convert tsdb block: %s", err)
	}
}

func loadParquetFile(t testing.TB, bkt objstore.BucketReader, name string) (*parquet.File, error) {
	t.Helper()

	rdr, err := bkt.Get(t.Context(), name)
	if err != nil {
		return nil, fmt.Errorf("unable to get object: %w", err)
	}
	defer func() {
		require.NoError(t, rdr.Close())
	}()

	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, rdr); err != nil {
		return nil, fmt.Errorf("unable to read object: %w", err)
	}
	return parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
}

func hasNoNullColumns(pf *parquet.File) error {
	cidxs := pf.ColumnIndexes()
	ncols := len(pf.Schema().Columns())

	nullPages := make([][]bool, ncols)
	for i := range ncols {
		for j := range cidxs {
			if j%ncols == i {
				nullPages[i] = append(nullPages[i], cidxs[j].NullPages...)
			}
		}
	}

	for i := range nullPages {
		if !slices.ContainsFunc(nullPages[i], func(np bool) bool { return np == false }) {
			return fmt.Errorf("column %d has only null pages", i)
		}
	}
	return nil
}

func hasExpectedIndexes(pf *parquet.File) error {
	cidxs := pf.ColumnIndexes()
	ncols := len(pf.Schema().Columns())

	if _, ok := pf.Schema().Lookup(schema.LabelIndexColumn); !ok {
		return fmt.Errorf("file is missing column: %s", schema.LabelIndexColumn)
	}
	for j := range cidxs {
		lminv := len(cidxs[j].MinValues)
		lmaxv := len(cidxs[j].MaxValues)

		if lminv == 0 {
			return fmt.Errorf("column is missing min values: %d", j%ncols)
		}
		if lmaxv == 0 {
			return fmt.Errorf("column is missing max values: %d", j%ncols)
		}
	}
	return nil
}

func nameColumnPageBoundsAreAscending(pf *parquet.File) error {
	lc, ok := pf.Schema().Lookup(schema.LabelNameToColumn(labels.MetricName))
	if !ok {
		return fmt.Errorf("file is missing column for label key: %s", labels.MetricName)
	}
	for _, rg := range pf.RowGroups() {
		cc := rg.ColumnChunks()[lc.ColumnIndex]
		cidx, err := cc.ColumnIndex()
		if err != nil {
			return fmt.Errorf("unable to get column index for column: %s", labels.MetricName)
		}
		// columns with 0 or 1 page are never indexed as ascending
		if !cidx.IsAscending() && cidx.NumPages() > 1 {
			return fmt.Errorf("column %q was not ascending", labels.MetricName)
		}
	}
	return nil
}

func nameColumnValuesAreIncreasing(t testing.TB, pf *parquet.File) error {
	lc, ok := pf.Schema().Lookup(schema.LabelNameToColumn(labels.MetricName))
	if !ok {
		return fmt.Errorf("file is missing column for label key: %s", labels.MetricName)
	}
	comp := parquet.ByteArrayType.Compare

	for _, rg := range pf.RowGroups() {
		cc := rg.ColumnChunks()[lc.ColumnIndex]

		pgs := cc.Pages()
		defer func() {
			require.NoError(t, pgs.Close())
		}()

		vwf := parquet.ValueWriterFunc(func(vs []parquet.Value) (int, error) {
			if len(vs) == 0 || len(vs) == 1 {
				return 0, nil
			}
			for i := range vs[:len(vs)-1] {
				if comp(vs[i], vs[i+1]) > 0 {
					return 0, fmt.Errorf("expected %q to be larger or equal to %q", vs[i+1], vs[i])
				}
			}
			return len(vs), nil
		})

		for {
			p, err := pgs.ReadPage()
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("unable to read page:%w", err)
			}
			if p == nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return errors.New("unexpected nil page")
			}
			if _, err := parquet.CopyValues(vwf, p.Values()); err != nil {
				return fmt.Errorf("unable to copy values :%w", err)
			}
		}
	}
	return nil
}
