// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/thanos-io/thanos-parquet-gateway/db"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type MetaFilter interface {
	filterMetas([]schema.Meta) []schema.Meta
	filterBlocks([]*db.Block) []*db.Block
}

var AllMetasMetaFilter = allMetas{}

type allMetas struct {
}

func (mf allMetas) filterMetas(metas []schema.Meta) []schema.Meta { return metas }
func (mf allMetas) filterBlocks(blocks []*db.Block) []*db.Block   { return blocks }

type ThanosBackfillMetaFilter struct {
	endpoint string
	overlap  int64

	mu         sync.Mutex
	mint, maxt int64
}

func NewThanosBackfillMetaFilter(endpoint string, overlap time.Duration) *ThanosBackfillMetaFilter {
	return &ThanosBackfillMetaFilter{endpoint: endpoint, overlap: overlap.Milliseconds()}
}

func (tp *ThanosBackfillMetaFilter) filterMetas(metas []schema.Meta) []schema.Meta {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	res := make([]schema.Meta, 0, len(metas))
	for _, v := range metas {
		if util.Contains(min(tp.mint+tp.overlap, tp.maxt), tp.maxt, v.Mint, v.Maxt) {
			continue
		}
		res = append(res, v)
	}
	return res
}

func (tp *ThanosBackfillMetaFilter) filterBlocks(blks []*db.Block) []*db.Block {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	res := make([]*db.Block, 0, len(blks))
	for _, blk := range blks {
		blkMint, blkMaxt := blk.Timerange()
		if util.Contains(min(tp.mint+tp.overlap, tp.maxt), tp.maxt, blkMint, blkMaxt) {
			continue
		}
		res = append(res, blk)
	}
	return res
}

func (tp *ThanosBackfillMetaFilter) Update(ctx context.Context) error {
	// Note: we assume that thanos runs close to this server, we dont need TLS here
	cc, err := grpc.NewClient(tp.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("unable to connect: %w", err)
	}
	client := infopb.NewInfoClient(cc)

	info, err := client.Info(ctx, &infopb.InfoRequest{})
	if err != nil {
		return fmt.Errorf("unable to get store time range from thanos: %w", err)
	}

	tp.mu.Lock()
	defer tp.mu.Unlock()

	tp.mint = info.Store.MinTime
	tp.maxt = info.Store.MaxTime

	return nil
}
