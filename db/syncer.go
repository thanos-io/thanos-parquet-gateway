// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/alecthomas/units"
	"github.com/parquet-go/parquet-go"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cloudflare/parquet-tsdb-poc/internal/util"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type Syncer struct {
	bkt objstore.Bucket

	blockOpts   []BlockOption
	metaFilter  MetaFilter
	concurrency int

	mu     sync.Mutex
	blocks map[string]*Block

	cached []*Block
}

type SyncerOption func(*syncerConfig)

type syncerConfig struct {
	blockOpts   []BlockOption
	metaFilter  MetaFilter
	concurrency int
}

func BlockOptions(opts ...BlockOption) SyncerOption {
	return func(cfg *syncerConfig) {
		cfg.blockOpts = opts
	}
}

func FilterMetas(f MetaFilter) SyncerOption {
	return func(cfg *syncerConfig) {
		cfg.metaFilter = f
	}
}

func BlockConcurrency(c int) SyncerOption {
	return func(cfg *syncerConfig) {
		cfg.concurrency = c
	}
}

type BlockOption func(*blockConfig)

type blockConfig struct {
	readBufferSize units.Base2Bytes
}

func ReadBufferSize(sz units.Base2Bytes) BlockOption {
	return func(cfg *blockConfig) {
		cfg.readBufferSize = sz
	}
}

func NewSyncer(bkt objstore.Bucket, opts ...SyncerOption) *Syncer {
	cfg := syncerConfig{
		metaFilter:  AllMetasMetaFilter,
		concurrency: 1,
	}

	for _, o := range opts {
		o(&cfg)
	}

	return &Syncer{
		bkt:         bkt,
		blocks:      make(map[string]*Block),
		blockOpts:   cfg.blockOpts,
		metaFilter:  cfg.metaFilter,
		concurrency: cfg.concurrency,
	}
}

func (s *Syncer) Blocks() []*Block {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.filterBlocks(s.cached)
}

func (s *Syncer) Sync(ctx context.Context, metas map[string]Meta) error {
	type blockOrError struct {
		blk *Block
		err error
	}

	blkC := make(chan blockOrError)
	go func() {
		defer close(blkC)

		workerC := make(chan Meta, s.concurrency)
		go func() {
			defer close(workerC)

			for k, v := range s.filterMetas(metas) {
				if _, ok := s.blocks[k]; ok {
					continue
				}
				workerC <- v
			}
		}()

		var wg sync.WaitGroup
		defer wg.Wait()

		for i := 0; i < s.concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for m := range workerC {
					blk, err := newBlockForMeta(ctx, s.bkt, m, s.blockOpts...)
					if err != nil {
						blkC <- blockOrError{err: fmt.Errorf("unable to read block %q: %w", m.Name, err)}
					} else {
						blkC <- blockOrError{blk: blk}
					}
				}
			}()
		}
	}()

	blocks := make(map[string]*Block, 0)
	for b := range blkC {
		if b.err != nil {
			return fmt.Errorf("unable to read block: %w", b.err)
		}
		blocks[b.blk.meta.Name] = b.blk
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// delete blocks that are not in meta map
	maps.DeleteFunc(s.blocks, func(k string, _ *Block) bool { _, ok := metas[k]; return !ok })

	// add new blocks that we just loaded
	maps.Copy(s.blocks, blocks)

	s.cached = slices.Collect(maps.Values(s.blocks))
	sort.Slice(s.cached, func(i, j int) bool {
		ls, _ := s.cached[i].Timerange()
		rs, _ := s.cached[j].Timerange()
		return ls < rs
	})
	return nil
}

func (s *Syncer) filterMetas(metas map[string]Meta) map[string]Meta {
	return s.metaFilter.filterMetas(metas)
}

func (s *Syncer) filterBlocks(blks []*Block) []*Block {
	return s.metaFilter.filterBlocks(blks)
}

type MetaFilter interface {
	filterMetas(map[string]Meta) map[string]Meta
	filterBlocks([]*Block) []*Block
}

var AllMetasMetaFilter = allMetas{}

type allMetas struct {
}

func (mf allMetas) filterMetas(metas map[string]Meta) map[string]Meta { return metas }
func (mf allMetas) filterBlocks(blocks []*Block) []*Block             { return blocks }

type ThanosBackfillMetaFilter struct {
	endpoint string

	mu         sync.Mutex
	mint, maxt int64
}

func NewThanosBackfillMetaFilter(endpoint string) *ThanosBackfillMetaFilter {
	return &ThanosBackfillMetaFilter{endpoint: endpoint}
}

func (tp *ThanosBackfillMetaFilter) filterMetas(metas map[string]Meta) map[string]Meta {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	res := make(map[string]Meta, len(metas))
	for k, v := range metas {
		if util.Contains(tp.mint, tp.maxt, v.Mint, v.Maxt) {
			continue
		}
		res[k] = v
	}
	return res
}

func (tp *ThanosBackfillMetaFilter) filterBlocks(blks []*Block) []*Block {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	res := make([]*Block, 0, len(blks))
	for _, blk := range blks {
		blkMint, blkMaxt := blk.Timerange()
		if util.Contains(tp.mint, tp.maxt, blkMint, blkMaxt) {
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

func newBlockForMeta(ctx context.Context, bkt objstore.Bucket, m Meta, opts ...BlockOption) (*Block, error) {
	cfg := blockConfig{
		readBufferSize: 8 * units.MiB,
	}
	for _, o := range opts {
		o(&cfg)
	}

	shards, err := readShards(ctx, bkt, m, cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to read shards: %w", err)
	}

	return &Block{meta: m, shards: shards}, nil
}

func readShards(ctx context.Context, bkt objstore.Bucket, m Meta, cfg blockConfig) ([]*Shard, error) {
	shards := make([]*Shard, 0, m.Shards)
	for i := 0; i != int(m.Shards); i++ {
		shard, err := readShard(ctx, bkt, m, i, cfg)
		if err != nil {
			return nil, fmt.Errorf("unable to read shard %d: %w", i, err)
		}
		shards = append(shards, shard)
	}
	return shards, nil
}

func readShard(ctx context.Context, bkt objstore.Bucket, m Meta, i int, cfg blockConfig) (*Shard, error) {
	chunkspfile := schema.ChunksPfileNameForShard(m.Name, i)
	attrs, err := bkt.Attributes(ctx, chunkspfile)
	if err != nil {
		return nil, fmt.Errorf("unable to attr %s: %w", chunkspfile, err)
	}

	bktRdrAt := newBucketReaderAt(bkt, chunkspfile, 1*time.Minute)

	chunkspf, err := parquet.OpenFile(bktRdrAt, attrs.Size,
		parquet.FileReadMode(parquet.ReadModeAsync),
		parquet.ReadBufferSize(int(cfg.readBufferSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to open parquet file %s: %w", chunkspfile, err)
	}

	labelspfile := schema.LabelsPfileNameForShard(m.Name, i)
	rdr, err := bkt.Get(ctx, labelspfile)
	if err != nil {
		return nil, fmt.Errorf("unable to get %s: %w", labelspfile, err)
	}
	defer rdr.Close()

	labelspfileBs, err := io.ReadAll(rdr)
	if err != nil {
		return nil, fmt.Errorf("unable to read %s: %w", labelspfile, err)
	}

	labelspf, err := parquet.OpenFile(bytes.NewReader(labelspfileBs), int64(len(labelspfileBs)))
	if err != nil {
		return nil, fmt.Errorf("unable to open parquet file %s: %w", labelspfile, err)
	}

	return &Shard{meta: m, chunkspfile: chunkspf, labelspfile: labelspf}, nil
}
