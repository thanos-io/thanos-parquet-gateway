// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos-parquet-gateway/db"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

func TestSyncer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(ctx testing.TB, bkt *filesystem.Bucket, extLabels schema.ExternalLabels)
		metas          func(extLabels schema.ExternalLabels) map[schema.ExternalLabelsHash]schema.ParquetBlocksStream
		expectedBlocks []util.Date
	}{
		{
			name: "Syncer always has the blocks from the discovered metas",
			setup: func(t testing.TB, bkt *filesystem.Bucket, extLabels schema.ExternalLabels) {
				d := util.NewDate(1970, time.January, 1)
				createBlockForDate(t.(*testing.T).Context(), t.(*testing.T), bkt, d, extLabels)
			},
			metas: func(extLabels schema.ExternalLabels) map[schema.ExternalLabelsHash]schema.ParquetBlocksStream {
				d := util.NewDate(1970, time.January, 1)
				return map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
					extLabels.Hash(): {
						StreamDescriptor: schema.StreamDescriptor{
							ExternalLabels: extLabels,
						},
						Metas: []schema.Meta{
							{
								Version: schema.V1,
								Name:    "1970/01/01",
								Date:    d,
								Mint:    d.MinT(),
								Maxt:    d.MaxT(),
								Shards:  1,
							},
						},
						DiscoveredDays: map[util.Date]struct{}{
							d: {},
						},
					},
				}
			},
			expectedBlocks: []util.Date{util.NewDate(1970, time.Month(1), 1)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Parallel()
			ctx := tt.Context()
			bkt, err := filesystem.NewBucket(tt.TempDir())
			require.NoError(tt, err)
			syncer := NewSyncer(bkt)

			extLabels := schema.ExternalLabels{"foo": "bar"}

			tc.setup(tt, bkt, extLabels)

			m := tc.metas(extLabels)
			require.NoError(tt, syncer.Sync(ctx, m))

			blocks := syncer.Blocks()
			if !slices.EqualFunc(tc.expectedBlocks, blocks, func(l util.Date, r *db.Block) bool {
				return l == r.Meta().Date
			}) {
				tt.Errorf("expected: %+v, got: %+v", tc.expectedBlocks, blocks)
			}

			// Test that syncing with empty metas clears blocks
			require.NoError(tt, syncer.Sync(ctx, map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{}))
			blocks = syncer.Blocks()
			if len(blocks) != 0 {
				tt.Errorf("expected empty blocks after sync with empty metas, got: %+v", blocks)
			}
		})
	}
}
