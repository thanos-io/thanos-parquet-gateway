// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package grpc

import (
	"context"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos-parquet-gateway/db"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

type mockParquetDB struct {
	blocks []db.BlockInfo

	overrideExtLabels labels.Labels
}

var _ parquetDatabase = (*mockParquetDB)(nil)

func (m *mockParquetDB) Timerange() (int64, int64) {
	var mint, maxt = int64(math.MaxInt64), int64(math.MinInt64)
	for _, b := range m.blocks {
		mint = min(mint, b.MinT)
		maxt = max(maxt, b.MaxT)
	}
	return mint, maxt
}

func (m *mockParquetDB) BlockStreams() map[schema.ExternalLabelsHash]db.BlockInfo {
	streams := make(map[schema.ExternalLabelsHash]db.BlockInfo)
	for _, b := range m.blocks {
		st, ok := streams[b.Labels.Hash()]
		if ok {
			st.MinT = min(st.MinT, b.MinT)
			st.MaxT = max(st.MaxT, b.MaxT)
			streams[b.Labels.Hash()] = st
			continue
		}
		streams[b.Labels.Hash()] = b
	}
	return streams
}

func (m *mockParquetDB) OverrideExtLabels() labels.Labels {
	return m.overrideExtLabels
}

func (m *mockParquetDB) Queryable(_ ...db.QueryableOption) *db.DBQueryable {
	return nil
}

func TestQueryServerInfo(t *testing.T) {
	t.Run("overridden ext labels", func(t *testing.T) {
		qs := &QueryServer{
			db: &mockParquetDB{
				overrideExtLabels: labels.FromStrings("cluster", "us-central1", "replica", "01"),
				blocks: []db.BlockInfo{
					{
						MinT:   100,
						MaxT:   200,
						Labels: schema.ExternalLabels{"not": "visible"},
					},
					{
						MinT: 50,
						MaxT: 250,
					},
				},
			},
		}

		resp, err := qs.Info(context.Background(), &infopb.InfoRequest{})
		require.NoError(t, err)
		require.Equal(t, int64(50), resp.Store.MinTime)
		require.Equal(t, int64(250), resp.Store.MaxTime)
		require.Equal(t, []labelpb.ZLabelSet{{
			Labels: []labelpb.ZLabel{
				{Name: "cluster", Value: "us-central1"},
				{Name: "replica", Value: "01"},
			},
		}}, resp.LabelSets)
	})

	t.Run("blocks based info", func(t *testing.T) {
		qs := &QueryServer{
			db: &mockParquetDB{
				overrideExtLabels: labels.FromStrings(),
				blocks: []db.BlockInfo{
					{
						MinT:   100,
						MaxT:   200,
						Labels: schema.ExternalLabels{"iam": "visible"},
					},
					{
						MinT:   50,
						MaxT:   250,
						Labels: schema.ExternalLabels{"me": "too"},
					},
				},
			},
		}

		resp, err := qs.Info(context.Background(), &infopb.InfoRequest{})
		require.NoError(t, err)
		require.Equal(t, int64(50), resp.Store.MinTime)
		require.Equal(t, int64(250), resp.Store.MaxTime)
		require.Equal(t, []labelpb.ZLabelSet{
			{
				Labels: []labelpb.ZLabel{
					{Name: "iam", Value: "visible"},
				},
			},
			{
				Labels: []labelpb.ZLabel{
					{Name: "me", Value: "too"},
				},
			},
		}, resp.LabelSets)
	})
}
