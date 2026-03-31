// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"bytes"
	"log/slog"
	"os"
	"path"
	"slices"
	"testing"
	"time"

	"testing/synctest"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type mockDiscoverable struct {
	st DiscovererStreams
}

func (m *mockDiscoverable) Streams() DiscovererStreams {
	return m.st
}

func TestDeleter(t *testing.T) {
	b := objstore.NewInMemBucket()
	md := &mockDiscoverable{}

	f := &RetentionDurationMarker{
		d:                 md,
		l:                 slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{})),
		bkt:               b,
		retentionDuration: 24 * time.Hour,
	}

	synctest.Test(t, func(t *testing.T) {
		md.st = DiscovererStreams{
			LastSync: time.Now(),
			Streams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				schema.ExternalLabelsHash(1): {
					DiscoveredDays: map[util.Date]struct{}{
						util.NewDate(1995, 1, 1):   {},
						util.NewDate(2000, 1, 1):   {},
						util.NewDate(1999, 12, 31): {},
						util.NewDate(1999, 12, 30): {},
					},
				},
			},
		}

		require.NoError(t, f.MarkExpiredStreams(t.Context()))

		objs := []string{}
		require.NoError(t, b.Iter(t.Context(), "", func(s string) error {
			t.Log("Found file in bucket:", s)
			objs = append(objs, s)
			return nil
		}, objstore.WithRecursiveIter()))

		require.Equal(t, true, slices.Contains(objs, `1/1995-01-01/`+DeletionMarkerName))
		require.Equal(t, false, slices.Contains(objs, `1/1999-12-31/`+DeletionMarkerName))
		require.Equal(t, true, slices.Contains(objs, `1/1999-12-30/`+DeletionMarkerName))
		require.Equal(t, false, slices.Contains(objs, `1/2000-01-01/`+DeletionMarkerName))

	})

}

func createEmptyBlock(t *testing.T, b *objstore.InMemBucket, h schema.ExternalLabelsHash, d util.Date, shards int) {
	t.Helper()

	ctx := t.Context()
	for i := range shards {
		require.NoError(t, b.Upload(ctx, schema.LabelsPfileNameForShard(h, d, i), bytes.NewReader(nil)))
		require.NoError(t, b.Upload(ctx, schema.ChunksPfileNameForShard(h, d, i), bytes.NewReader(nil)))
	}
	require.NoError(t, b.Upload(ctx, schema.MetaFileNameForBlock(d, h), bytes.NewReader(nil)))
	require.NoError(t, b.Upload(ctx, path.Join(h.String(), d.String()), bytes.NewReader(nil)))
}

func TestRetentionDurationDeleter(t *testing.T) {
	const (
		hash1 schema.ExternalLabelsHash = 111
		hash2 schema.ExternalLabelsHash = 222
	)

	date1 := util.NewDate(2020, time.January, 1)
	date2 := util.NewDate(2020, time.February, 1)

	markerPath := func(h schema.ExternalLabelsHash, d util.Date) string {
		return path.Join(h.String(), d.String(), DeletionMarkerName)
	}

	tests := []struct {
		name        string
		setup       func(t *testing.T, b *objstore.InMemBucket)
		wantPresent []string
		wantAbsent  []string
	}{
		{
			name: "no deletion markers - nothing deleted",
			setup: func(t *testing.T, b *objstore.InMemBucket) {
				createEmptyBlock(t, b, hash1, date1, 1)
			},
			wantPresent: []string{
				schema.LabelsPfileNameForShard(hash1, date1, 0),
				schema.ChunksPfileNameForShard(hash1, date1, 0),
				schema.MetaFileNameForBlock(date1, hash1),
			},
		},
		{
			name: "marker within consistency delay - files not deleted",
			setup: func(t *testing.T, b *objstore.InMemBucket) {
				createEmptyBlock(t, b, hash1, date1, 1)
				require.NoError(t, b.Upload(t.Context(), markerPath(hash1, date1), bytes.NewReader(nil)))
			},
			wantPresent: []string{
				schema.LabelsPfileNameForShard(hash1, date1, 0),
				schema.ChunksPfileNameForShard(hash1, date1, 0),
				schema.MetaFileNameForBlock(date1, hash1),
				markerPath(hash1, date1),
			},
		},
		{
			name: "marker beyond consistency delay - all files deleted",
			setup: func(t *testing.T, b *objstore.InMemBucket) {
				createEmptyBlock(t, b, hash1, date1, 1)
				mp := markerPath(hash1, date1)
				require.NoError(t, b.Upload(t.Context(), mp, bytes.NewReader(nil)))
				require.NoError(t, b.ChangeLastModified(mp, time.Now().Add(-(deleteConsistencyDelay+time.Hour))))
			},
			wantAbsent: []string{
				schema.LabelsPfileNameForShard(hash1, date1, 0),
				schema.ChunksPfileNameForShard(hash1, date1, 0),
				schema.MetaFileNameForBlock(date1, hash1),
			},
		},
		{
			name: "multiple streams - only expired one deleted",
			setup: func(t *testing.T, b *objstore.InMemBucket) {
				createEmptyBlock(t, b, hash1, date1, 1)
				mp1 := markerPath(hash1, date1)
				require.NoError(t, b.Upload(t.Context(), mp1, bytes.NewReader(nil)))
				require.NoError(t, b.ChangeLastModified(mp1, time.Now().Add(-(deleteConsistencyDelay+time.Hour))))
				createEmptyBlock(t, b, hash2, date2, 1)
			},
			wantAbsent: []string{
				schema.LabelsPfileNameForShard(hash1, date1, 0),
				schema.ChunksPfileNameForShard(hash1, date1, 0),
				schema.MetaFileNameForBlock(date1, hash1),
			},
			wantPresent: []string{
				schema.LabelsPfileNameForShard(hash2, date2, 0),
				schema.ChunksPfileNameForShard(hash2, date2, 0),
				schema.MetaFileNameForBlock(date2, hash2),
			},
		},
		{
			name: "multiple shards - all shards deleted",
			setup: func(t *testing.T, b *objstore.InMemBucket) {
				createEmptyBlock(t, b, hash1, date1, 3)
				mp := markerPath(hash1, date1)
				require.NoError(t, b.Upload(t.Context(), mp, bytes.NewReader(nil)))
				require.NoError(t, b.ChangeLastModified(mp, time.Now().Add(-(deleteConsistencyDelay+time.Hour))))
			},
			wantAbsent: []string{
				schema.LabelsPfileNameForShard(hash1, date1, 0),
				schema.ChunksPfileNameForShard(hash1, date1, 0),
				schema.LabelsPfileNameForShard(hash1, date1, 1),
				schema.ChunksPfileNameForShard(hash1, date1, 1),
				schema.LabelsPfileNameForShard(hash1, date1, 2),
				schema.ChunksPfileNameForShard(hash1, date1, 2),
				schema.MetaFileNameForBlock(date1, hash1),
			},
		},
		{
			name: "expired marker on one date, recent marker on another - only expired deleted",
			setup: func(t *testing.T, b *objstore.InMemBucket) {
				createEmptyBlock(t, b, hash1, date1, 1)
				mp1 := markerPath(hash1, date1)
				require.NoError(t, b.Upload(t.Context(), mp1, bytes.NewReader(nil)))
				require.NoError(t, b.ChangeLastModified(mp1, time.Now().Add(-(deleteConsistencyDelay+time.Hour))))
				createEmptyBlock(t, b, hash1, date2, 1)
				require.NoError(t, b.Upload(t.Context(), markerPath(hash1, date2), bytes.NewReader(nil)))
			},
			wantAbsent: []string{
				schema.LabelsPfileNameForShard(hash1, date1, 0),
				schema.ChunksPfileNameForShard(hash1, date1, 0),
				schema.MetaFileNameForBlock(date1, hash1),
			},
			wantPresent: []string{
				schema.LabelsPfileNameForShard(hash1, date2, 0),
				schema.ChunksPfileNameForShard(hash1, date2, 0),
				schema.MetaFileNameForBlock(date2, hash1),
				markerPath(hash1, date2),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := objstore.NewInMemBucket()
			tc.setup(t, b)

			d := NewRetentionDurationDeleter(b)
			require.NoError(t, d.DeleteMarkedStreams(t.Context()))

			for _, p := range tc.wantPresent {
				ok, err := b.Exists(t.Context(), p)
				require.NoError(t, err)
				require.True(t, ok, "expected %q to be present", p)
			}
			for _, p := range tc.wantAbsent {
				ok, err := b.Exists(t.Context(), p)
				require.NoError(t, err)
				require.False(t, ok, "expected %q to be absent", p)
			}
		})
	}
}
