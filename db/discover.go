// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	"github.com/thanos-io/objstore"
	"google.golang.org/protobuf/proto"

	"github.com/cloudflare/parquet-tsdb-poc/proto/metapb"
	"github.com/cloudflare/parquet-tsdb-poc/schema"
)

type Discoverer struct {
	bkt objstore.Bucket

	mu    sync.Mutex
	metas map[string]Meta

	concurrency int
}

type discoveryConfig struct {
	concurrency int
}

type DiscoveryOption func(*discoveryConfig)

func MetaConcurrency(c int) DiscoveryOption {
	return func(cfg *discoveryConfig) {
		cfg.concurrency = c
	}
}

func NewDiscoverer(bkt objstore.Bucket, opts ...DiscoveryOption) *Discoverer {
	cfg := discoveryConfig{
		concurrency: 1,
	}
	for _, o := range opts {
		o(&cfg)
	}
	return &Discoverer{
		bkt:         bkt,
		metas:       make(map[string]Meta),
		concurrency: cfg.concurrency,
	}
}

func (s *Discoverer) Metas() map[string]Meta {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]Meta, len(s.metas))
	maps.Copy(res, s.metas)

	return res
}

func (s *Discoverer) Discover(ctx context.Context) error {
	m := make(map[string][]string)
	err := s.bkt.Iter(ctx, "", func(n string) error {
		id, file, ok := schema.SplitBlockPath(n)
		if !ok {
			return nil
		}
		m[id] = append(m[id], file)
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return err
	}

	type metaOrError struct {
		m   Meta
		err error
	}

	metaC := make(chan metaOrError)
	go func() {
		defer close(metaC)

		workerC := make(chan string, s.concurrency)
		go func() {
			defer close(workerC)

			for k, v := range m {
				if _, ok := s.metas[k]; ok {
					continue
				}
				if !slices.Contains(v, schema.MetaFile) {
					// skip incomplete block
					continue
				}
				workerC <- k
			}
		}()

		var wg sync.WaitGroup
		defer wg.Wait()

		for i := 0; i < s.concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := range workerC {
					meta, err := readMetafile(ctx, s.bkt, k)
					if err != nil {
						metaC <- metaOrError{err: fmt.Errorf("unable to read meta file for %q: %w", k, err)}
					} else {
						metaC <- metaOrError{m: meta}
					}
				}
			}()
		}
	}()

	nm := make(map[string]Meta)
	for m := range metaC {
		if m.err != nil {
			return fmt.Errorf("unable to read meta: %w", err)
		}
		nm[m.m.Name] = m.m
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	maps.Copy(s.metas, nm)

	return nil
}

func readMetafile(ctx context.Context, bkt objstore.Bucket, name string) (Meta, error) {
	mfile := schema.MetaFileNameForBlock(name)
	if _, err := bkt.Attributes(ctx, mfile); err != nil {
		return Meta{}, fmt.Errorf("unable to attr %s: %w", mfile, err)
	}
	rdr, err := bkt.Get(ctx, mfile)
	if err != nil {
		return Meta{}, fmt.Errorf("unable to get %s: %w", mfile, err)
	}
	defer rdr.Close()

	metaBytes, err := io.ReadAll(rdr)
	if err != nil {
		return Meta{}, fmt.Errorf("unable to read %s: %w", mfile, err)
	}

	metapb := &metapb.Metadata{}
	if err := proto.Unmarshal(metaBytes, metapb); err != nil {
		return Meta{}, fmt.Errorf("unable to read %s: %w", mfile, err)
	}

	m := make(map[string][]string, 0)
	for k, v := range metapb.GetColumnsForName() {
		m[k] = v.GetColumns()
	}
	return Meta{
		Name:           name,
		Mint:           metapb.GetMint(),
		Maxt:           metapb.GetMaxt(),
		Shards:         metapb.GetShards(),
		ColumnsForName: m,
	}, nil
}
