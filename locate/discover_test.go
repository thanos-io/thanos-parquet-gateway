// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"

	"github.com/thanos-io/thanos-parquet-gateway/convert"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/proto/streampb"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestDiscoverer(t *testing.T) {
	ctx := t.Context()
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	discoverer := NewDiscoverer(bkt)
	var extLabels = schema.ExternalLabels{
		"foo": "bar",
	}

	t.Run("Discoverer discovers newly uploaded blocks", func(t *testing.T) {
		d := util.NewDate(1970, time.January, 1)
		createBlockForDate(ctx, t, bkt, d, extLabels)

		require.NoError(t, discoverer.Discover(ctx))

		metas := discoverer.Streams()
		require.Len(t, metas, 1)

		stream := metas[extLabels.Hash()]

		if expect, got := []util.Date{util.NewDate(1970, time.January, 1)}, slices.SortedFunc(maps.Keys(stream.DiscoveredDays), func(a, b util.Date) int { return int(a.ToTime().Sub(b.ToTime())) }); !slices.Equal(got, expect) {
			t.Errorf("expected: %+v, got: %+v", expect, got)
		}

		d = util.NewDate(1970, time.January, 2)
		createBlockForDate(ctx, t, bkt, d, extLabels)

		if err := discoverer.Discover(ctx); err != nil {
			t.Fatalf("unable to discover tsdb metas: %s", err)
		}

		streams := discoverer.Streams()
		require.Len(t, streams, 1)

		stream = streams[extLabels.Hash()]
		if expect, got := []util.Date{util.NewDate(1970, time.January, 1), util.NewDate(1970, time.January, 2)}, slices.SortedFunc(maps.Keys(stream.DiscoveredDays), func(a, b util.Date) int { return int(a.ToTime().Sub(b.ToTime())) }); !slices.Equal(got, expect) {
			t.Errorf("expected: %+v, got: %+v", expect, got)
		}

		require.Equal(t, extLabels, stream.ExternalLabels)
	})

	t.Run("does not detect blocks with no meta.pb", func(t *testing.T) {
		require.NoError(t, bkt.Delete(ctx, path.Join(extLabels.Hash().String(), "1970-01-01", "meta.pb")))
		require.NoError(t, discoverer.Discover(ctx))

		metas := discoverer.Streams()
		require.Len(t, metas, 1)

		stream := metas[extLabels.Hash()]

		if expect, got := []util.Date{util.NewDate(1970, time.January, 2)}, slices.SortedFunc(maps.Keys(stream.DiscoveredDays), func(a, b util.Date) int { return int(a.ToTime().Sub(b.ToTime())) }); !slices.Equal(got, expect) {
			t.Errorf("expected: %+v, got: %+v", expect, got)
		}
	})

	t.Run("errors out if external labels in stream.pb does not match the actual hash", func(t *testing.T) {
		stream := &streampb.StreamDescriptor{
			ExternalLabels: map[string]string{
				"asdsadsa": "asdsadsadsadsadsa",
			},
		}
		buf, err := proto.Marshal(stream)
		require.NoError(t, err)
		require.NoError(t, bkt.Upload(ctx, path.Join(extLabels.Hash().String(), "stream.pb"), bytes.NewReader(buf)))

		require.Error(t, discoverer.Discover(ctx))

		require.NoError(t, bkt.Delete(ctx, path.Join(extLabels.Hash().String(), "stream.pb")))
		require.NoError(t, discoverer.Discover(ctx))
	})

	t.Run("detects streams with no external labels (old format)", func(t *testing.T) {
		moveObject(t, bkt, path.Join(extLabels.Hash().String(), "1970-01-02", "meta.pb"), path.Join("1970-01-02", "meta.pb"))
		moveObject(t, bkt, path.Join(extLabels.Hash().String(), "1970-01-02", "0.chunks.parquet"), path.Join("1970-01-02", "0.chunks.parquet"))
		moveObject(t, bkt, path.Join(extLabels.Hash().String(), "1970-01-02", "0.labels.parquet"), path.Join("1970-01-02", "0.labels.parquet"))

		require.NoError(t, discoverer.Discover(ctx))

		streams := discoverer.Streams()

		stream, ok := streams[0]
		require.True(t, ok)

		if expect, got := []util.Date{util.NewDate(1970, time.January, 2)}, slices.SortedFunc(maps.Keys(stream.DiscoveredDays), func(a, b util.Date) int { return int(a.ToTime().Sub(b.ToTime())) }); !slices.Equal(got, expect) {
			t.Errorf("expected: %+v, got: %+v", expect, got)
		}
	})

	t.Run("stream.pb is mandatory for non-zero hashes", func(t *testing.T) {
		nonZeroLbls := schema.ExternalLabels{
			"aaa": "bbb",
		}
		d := util.NewDate(1970, time.January, 3)
		createBlockForDate(ctx, t, bkt, d, nonZeroLbls)

		require.NoError(t, discoverer.Discover(ctx))

		streams := discoverer.Streams()

		require.Contains(t, streams, nonZeroLbls.Hash())
		require.NoError(t, bkt.Delete(ctx, path.Join(nonZeroLbls.Hash().String(), "stream.pb")))

		require.NoError(t, discoverer.Discover(ctx))

		streams = discoverer.Streams()
		require.Len(t, streams, 1)
		require.NotContains(t, streams, nonZeroLbls.Hash())
	})
}

func moveObject(t *testing.T, bkt objstore.Bucket, from, to string) {
	ctx := t.Context()
	r, err := bkt.Get(ctx, from)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, r.Close())
	})

	require.NoError(t, bkt.Upload(ctx, to, r))
	require.NoError(t, bkt.Delete(ctx, from))
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
					ULID:  ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
					Stats: tsdb.BlockStats{NumChunks: 1},
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
			},
			{
				BlockMeta: tsdb.BlockMeta{
					ULID:  ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					Stats: tsdb.BlockStats{NumChunks: 1},
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

		streams := discoverer.Streams()
		require.Equal(t, len(streams), 1)

		notBarMetas := streams[schema.ExternalLabels{"foo": "not-bar"}.Hash()].Metas

		require.Equal(t, `01JT0DPYGA1HPW5RBZ1KBXCNXK`, notBarMetas[0].ULID.String())
		require.Len(t, notBarMetas, 1)
	})
	t.Run("Discoverer skips blocks with deletion markers", func(tt *testing.T) {
		ctx := tt.Context()
		bkt, err := filesystem.NewBucket(tt.TempDir())
		if err != nil {
			tt.Fatalf("unable to create bucket: %s", err)
		}

		meta := metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:  ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
				Stats: tsdb.BlockStats{NumChunks: 1},
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

		streams := discoverer.Streams()
		if got := slices.Collect(maps.Keys(streams)); len(got) != 0 {
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
				ULID:  ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
				Stats: tsdb.BlockStats{NumChunks: 1},
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

		streams := discoverer.Streams()
		metas := streams[0].Metas
		require.Equal(t, 1, len(metas))
		require.Equal(t, `01JS0DPYGA1HPW5RBZ1KBXCNXK`, metas[0].ULID.String())

		// delete the block
		if err := bkt.Delete(ctx, meta.ULID.String()); err != nil {
			tt.Fatalf("unable to delete block: %s", err)
		}
		if err := discoverer.Discover(ctx); err != nil {
			tt.Fatalf("unable to discover tsdb metas: %s", err)
		}

		streams = discoverer.Streams()
		if got := slices.Collect(maps.Keys(streams)); len(got) != 0 {
			tt.Errorf("expected empty slice, got: %+v", got)
		}

	})
}

func createBlockForDate(ctx context.Context, t *testing.T, bkt objstore.Bucket, d util.Date, extLabels schema.ExternalLabels) {
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	app := st.Appender(ctx)
	_, err := app.Append(0, labels.FromStrings("foo", "bar"), d.MinT(), 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	require.NoError(t, convert.ConvertTSDBBlock(ctx, bkt, d, extLabels.Hash(), []convert.Convertible{&convert.HeadBlock{Head: st.Head()}}))

	streamDescriptor := &streampb.StreamDescriptor{
		ExternalLabels: extLabels,
	}
	buf, err := proto.Marshal(streamDescriptor)
	require.NoError(t, err)
	require.NoError(t, bkt.Upload(ctx, path.Join(extLabels.Hash().String(), "stream.pb"), bytes.NewReader(buf)))
}
