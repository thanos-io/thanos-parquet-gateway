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
	t.Run("Syncer always has the blocks from the discovered metas", func(tt *testing.T) {
		ctx := tt.Context()
		bkt, err := filesystem.NewBucket(tt.TempDir())
		require.NoError(t, err)
		syncer := NewSyncer(bkt)

		lbls := schema.ExternalLabels{
			"foo": "bar",
		}

		d := util.NewDate(1970, time.January, 1)
		createBlockForDate(ctx, tt, bkt, d, lbls)

		m := map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
			lbls.Hash(): {
				StreamDescriptor: schema.StreamDescriptor{
					ExternalLabels: lbls,
				},
				Metas: []schema.Meta{
					{
						Version: schema.V1,
						Date:    util.NewDate(1970, time.Month(1), 1),
						Mint:    d.MinT(),
						Maxt:    d.MaxT(),
						Shards:  1,
					},
				},
				DiscoveredDays: map[util.Date]struct{}{
					util.NewDate(1970, time.Month(1), 1): {},
				},
			},
		}

		if err := syncer.Sync(ctx, m); err != nil {
			tt.Fatalf("unable to sync blocks: %s", err)
		}

		if expect, got := []util.Date{util.NewDate(1970, time.Month(1), 1)}, syncer.Blocks(); !slices.EqualFunc(expect, got, func(l util.Date, r *db.Block) bool { return l == r.Meta().Date }) {
			tt.Errorf("expected: %+v, got: %+v", expect, got)
		}

		if err := syncer.Sync(ctx, map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{}); err != nil {
			tt.Fatalf("unable to sync blocks: %s", err)
		}

		if expect, got := []util.Date{}, syncer.Blocks(); !slices.EqualFunc(expect, got, func(l util.Date, r *db.Block) bool { return l == r.Meta().Date }) {
			tt.Errorf("expected: %+v, got: %+v", expect, got)
		}
	})
}
