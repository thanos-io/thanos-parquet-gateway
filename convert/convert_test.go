// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/alecthomas/units"
	jsoniter "github.com/json-iterator/go"
	"github.com/oklog/ulid/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block/metadata"
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
	d := util.BeginOfDay(time.UnixMilli(h.MinTime())).UTC()

	opts := []ConvertOption{
		SortBy(labels.MetricName),
		RowGroupSize(250),
		RowGroupCount(2),
		LabelPageBufferSize(units.KiB), // results in 2 pages
	}
	if err := ConvertTSDBBlock(t.Context(), bkt, d, []Convertible{h}, opts...); err != nil {
		t.Fatalf("unable to convert tsdb block: %s", err)
	}

	discoverer := locate.NewDiscoverer(bkt)
	if err := discoverer.Discover(t.Context()); err != nil {
		t.Fatalf("unable to convert parquet block: %s", err)
	}
	metas := discoverer.Metas()

	if n := len(metas); n != 1 {
		t.Fatalf("unexpected number of metas: %d", n)
	}
	meta := metas[slices.Collect(maps.Keys(metas))[0]]

	if n := meta.Shards; n != 2 {
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

		if err := hasNoNullColumns(lf); err != nil {
			t.Fatalf("unable to check for null columns: %s", err)
		}
		if err := hasExpectedIndexes(lf); err != nil {
			t.Fatalf("unable to check for null columns: %s", err)
		}
		if err := nameColumnPageBoundsAreAscending(lf); err != nil {
			t.Fatalf("unable to check that __name__ column page bounds are ascending: %s", err)
		}
		if err := nameColumnValuesAreIncreasing(lf); err != nil {
			t.Fatalf("unable to check that __name__ column values are increasing: %s", err)
		}
	}
	if totalRows != int64(st.DB.Head().NumSeries()) {
		t.Fatalf("too few rows: %d", totalRows)
	}
}

func loadParquetFile(ctx context.Context, bkt objstore.BucketReader, name string) (*parquet.File, error) {
	rdr, err := bkt.Get(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("unable to get object: %w", err)
	}
	defer rdr.Close()

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

func nameColumnValuesAreIncreasing(pf *parquet.File) error {
	lc, ok := pf.Schema().Lookup(schema.LabelNameToColumn(labels.MetricName))
	if !ok {
		return fmt.Errorf("file is missing column for label key: %s", labels.MetricName)
	}
	comp := parquet.ByteArrayType.Compare

	for _, rg := range pf.RowGroups() {
		cc := rg.ColumnChunks()[lc.ColumnIndex]

		pgs := cc.Pages()
		defer pgs.Close()

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

func TestUpdateTSDBBlockMetadata(t *testing.T) {
	ctx := context.Background()
	bkt, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatalf("unable to create bucket: %s", err)
	}
	t.Cleanup(func() { _ = bkt.Close() })

	mockMeta := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID: ulid.MustNew(ulid.Now(), rand.Reader),
		},
		Thanos: metadata.Thanos{
			Version: 1,
			Labels:  map[string]string{"cluster": "test"},
		},
	}

	metaPath := mockMeta.ULID.String() + "/meta.json"
	var buf bytes.Buffer
	encoder := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(&buf)
	encoder.SetIndent("", "\t")
	if err := encoder.Encode(&mockMeta); err != nil {
		t.Fatalf("unable to encode initial meta.json: %s", err)
	}
	if err := bkt.Upload(ctx, metaPath, &buf); err != nil {
		t.Fatalf("unable to upload initial meta.json: %s", err)
	}

	err = markBlockAsMigrated(ctx, bkt, mockMeta)
	if err != nil {
		t.Fatalf("unexpected error updating metadata: %s", err)
	}

	rc, err := bkt.Get(ctx, metaPath)
	if err != nil {
		t.Fatalf("unable to get updated meta.json: %s", err)
	}
	defer rc.Close()

	var updatedMeta metadata.Meta
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.NewDecoder(rc).Decode(&updatedMeta); err != nil {
		t.Fatalf("unable to decode updated meta.json: %s", err)
	}

	if updatedMeta.Thanos.Extensions == nil {
		t.Fatal("Extensions field is nil after update")
	}

	extensionsMap, ok := updatedMeta.Thanos.Extensions.(map[string]any)
	if !ok {
		t.Fatal("Extensions field is not a map[string]any")
	}

	migratedFlag, exists := extensionsMap["parquet_migrated"]
	if !exists {
		t.Fatal("parquet_migrated flag not found in Extensions")
	}

	migratedBool, ok := migratedFlag.(bool)
	if !ok {
		t.Fatalf("parquet_migrated flag is not a bool, got type: %T", migratedFlag)
	}

	if !migratedBool {
		t.Fatal("parquet_migrated flag should be true")
	}

	if updatedMeta.ULID != mockMeta.ULID {
		t.Fatal("ULID was not preserved")
	}
	if updatedMeta.Thanos.Version != mockMeta.Thanos.Version {
		t.Fatal("Thanos version was not preserved")
	}
	if !maps.Equal(updatedMeta.Thanos.Labels, mockMeta.Thanos.Labels) {
		t.Fatal("Thanos labels were not preserved")
	}
}

func TestUpdateTSDBBlockMetadataWithExistingExtensions(t *testing.T) {
	ctx := context.Background()
	bkt, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatalf("unable to create bucket: %s", err)
	}
	t.Cleanup(func() { _ = bkt.Close() })

	// Create a mock TSDB block metadata file with existing extensions
	existingExtensions := map[string]any{
		"existing_key": "existing_value",
		"number_key":   42,
	}

	mockMeta := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID: ulid.MustNew(ulid.Now(), rand.Reader),
		},
		Thanos: metadata.Thanos{
			Version:    1,
			Labels:     map[string]string{"cluster": "test"},
			Extensions: existingExtensions,
		},
	}

	metaPath := mockMeta.ULID.String() + "/meta.json"
	var buf bytes.Buffer
	encoder := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(&buf)
	encoder.SetIndent("", "\t")
	if err := encoder.Encode(&mockMeta); err != nil {
		t.Fatalf("unable to encode initial meta.json: %s", err)
	}
	if err := bkt.Upload(ctx, metaPath, &buf); err != nil {
		t.Fatalf("unable to upload initial meta.json: %s", err)
	}

	err = markBlockAsMigrated(ctx, bkt, mockMeta)
	if err != nil {
		t.Fatalf("unexpected error updating metadata: %s", err)
	}

	rc, err := bkt.Get(ctx, metaPath)
	if err != nil {
		t.Fatalf("unable to get updated meta.json: %s", err)
	}
	defer rc.Close()

	var updatedMeta metadata.Meta
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.NewDecoder(rc).Decode(&updatedMeta); err != nil {
		t.Fatalf("unable to decode updated meta.json: %s", err)
	}

	extensionsMap, ok := updatedMeta.Thanos.Extensions.(map[string]any)
	if !ok {
		t.Fatal("Extensions field is not a map[string]any")
	}

	migratedFlag, exists := extensionsMap["parquet_migrated"]
	if !exists {
		t.Fatal("parquet_migrated flag not found in Extensions")
	}

	if migratedBool, ok := migratedFlag.(bool); !ok || !migratedBool {
		t.Fatal("parquet_migrated flag should be true")
	}

	if extensionsMap["existing_key"] != "existing_value" {
		t.Fatal("existing_key was not preserved")
	}
	if extensionsMap["number_key"] != float64(42) {
		t.Fatal("number_key was not preserved")
	}
}
