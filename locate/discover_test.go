// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"go.uber.org/goleak"

	"github.com/thanos-io/thanos-parquet-gateway/convert"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestDiscoverer(t *testing.T) {
	t.Run("Discoverer discovers newly uploaded blocks", func(tt *testing.T) {
		ctx := tt.Context()
		bkt, err := filesystem.NewBucket(tt.TempDir())
		if err != nil {
			tt.Fatalf("unable to create bucket: %s", err)
		}
		discoverer := NewDiscoverer(bkt)

		d := util.NewDate(1970, time.January, 1)
		if err := createBlockForDay(ctx, tt, bkt, d); err != nil {
			tt.Fatalf("unable to create block for day: %s", err)
		}

		if err := discoverer.Discover(ctx); err != nil {
			tt.Fatalf("unable to discover tsdb metas: %s", err)
		}

		metas := discoverer.Metas()
		if expect, got := []string{"1970/01/01"}, slices.Sorted(maps.Keys(metas)); !slices.Equal(got, expect) {
			tt.Errorf("expected: %+v, got: %+v", expect, got)
		}

		// Add another block
		d = util.NewDate(1970, time.January, 2)
		if err := createBlockForDay(ctx, tt, bkt, d); err != nil {
			tt.Fatalf("unable to create block for day: %s", err)
		}

		if err := discoverer.Discover(ctx); err != nil {
			tt.Fatalf("unable to discover tsdb metas: %s", err)
		}

		metas = discoverer.Metas()
		if expect, got := []string{"1970/01/01", "1970/01/02"}, slices.Sorted(maps.Keys(metas)); !slices.Equal(got, expect) {
			tt.Errorf("expected: %+v, got: %+v", expect, got)
		}
	})
}

func TestTSDBDiscoverer(t *testing.T) {
	t.Run("Discoverer skips blocks that are not matching", func(tt *testing.T) {
		ctx := tt.Context()
		bkt, err := filesystem.NewBucket(tt.TempDir())
		if err != nil {
			tt.Fatalf("unable to create bucket: %s", err)
		}

		for _, m := range []metadata.Meta{
			{
				BlockMeta: tsdb.BlockMeta{
					ULID: ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
			},
			{
				BlockMeta: tsdb.BlockMeta{
					ULID: ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						"foo": "not-bar",
					},
				},
			},
		} {
			buf := bytes.NewBuffer(nil)
			if err := json.NewEncoder(buf).Encode(m); err != nil {
				tt.Fatalf("unable to encode meta file: %s", err)
			}
			if err := bkt.Upload(ctx, filepath.Join(m.ULID.String(), metadata.MetaFilename), buf); err != nil {
				tt.Fatalf("unable to upload meta file: %s", err)
			}
		}

		discoverer := NewTSDBDiscoverer(bkt, TSDBMatchExternalLabels(labels.MustNewMatcher(labels.MatchNotEqual, "foo", "bar")))
		if err := discoverer.Discover(ctx); err != nil {
			tt.Fatalf("unable to discover tsdb metas: %s", err)
		}

		metas := discoverer.Metas()
		if expect, got := []string{"01JT0DPYGA1HPW5RBZ1KBXCNXK"}, slices.Collect(maps.Keys(metas)); !slices.Equal(got, expect) {
			tt.Errorf("expected: %+v, got: %+v", expect, got)
		}
	})
	t.Run("Discoverer skips blocks with deletion markers", func(tt *testing.T) {
		ctx := tt.Context()
		bkt, err := filesystem.NewBucket(tt.TempDir())
		if err != nil {
			tt.Fatalf("unable to create bucket: %s", err)
		}

		meta := metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
			},
			Thanos: metadata.Thanos{
				Labels: map[string]string{
					"foo": "bar",
				},
			},
		}
		buf := bytes.NewBuffer(nil)
		if err := json.NewEncoder(buf).Encode(meta); err != nil {
			tt.Fatalf("unable to encode meta file: %s", err)
		}
		if err := bkt.Upload(ctx, filepath.Join(meta.ULID.String(), metadata.MetaFilename), buf); err != nil {
			tt.Fatalf("unable to upload meta file: %s", err)
		}

		buf.Reset()
		if err := bkt.Upload(ctx, filepath.Join(meta.ULID.String(), metadata.DeletionMarkFilename), buf); err != nil {
			tt.Fatalf("unable to upload deletion file: %s", err)
		}

		discoverer := NewTSDBDiscoverer(bkt)
		if err := discoverer.Discover(ctx); err != nil {
			tt.Fatalf("unable to discover tsdb metas: %s", err)
		}

		metas := discoverer.Metas()
		if got := slices.Collect(maps.Keys(metas)); len(got) != 0 {
			tt.Errorf("expected empty slice, got: %+v", got)
		}
	})
	t.Run("Discoverer forgets blocks that are no longer there", func(tt *testing.T) {
		ctx := tt.Context()
		bkt, err := filesystem.NewBucket(tt.TempDir())
		if err != nil {
			tt.Fatalf("unable to create bucket: %s", err)
		}

		meta := metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
			},
		}
		buf := bytes.NewBuffer(nil)
		if err := json.NewEncoder(buf).Encode(meta); err != nil {
			tt.Fatalf("unable to encode meta file: %s", err)
		}
		if err := bkt.Upload(ctx, filepath.Join(meta.ULID.String(), metadata.MetaFilename), buf); err != nil {
			tt.Fatalf("unable to upload meta file: %s", err)
		}

		discoverer := NewTSDBDiscoverer(bkt)
		if err := discoverer.Discover(ctx); err != nil {
			tt.Fatalf("unable to discover tsdb metas: %s", err)
		}

		metas := discoverer.Metas()
		if expect, got := []string{"01JS0DPYGA1HPW5RBZ1KBXCNXK"}, slices.Collect(maps.Keys(metas)); !slices.Equal(got, expect) {
			tt.Errorf("expected: %+v, got: %+v", expect, got)
		}

		// delete the block
		if err := bkt.Delete(ctx, meta.BlockMeta.ULID.String()); err != nil {
			tt.Fatalf("unable to delete block: %s", err)
		}
		if err := discoverer.Discover(ctx); err != nil {
			tt.Fatalf("unable to discover tsdb metas: %s", err)
		}

		metas = discoverer.Metas()
		if got := slices.Collect(maps.Keys(metas)); len(got) != 0 {
			tt.Errorf("expected empty slice, got: %+v", got)
		}

	})
}

func createBlockForDay(ctx context.Context, t *testing.T, bkt objstore.Bucket, d util.Date) error {
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	app := st.Appender(ctx)
	app.Append(0, labels.FromStrings("foo", "bar"), d.MinT(), 1)
	if err := app.Commit(); err != nil {
		return fmt.Errorf("unable to commit samples: %s", err)
	}

	h := st.Head()
	if err := convert.ConvertTSDBBlock(ctx, bkt, d, []convert.Convertible{&convert.HeadBlock{Head: h}}); err != nil {
		return fmt.Errorf("unable to convert blocks: %s", err)
	}
	return nil
}
