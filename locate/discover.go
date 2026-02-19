// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/sourcegraph/conc/pool"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"

	"github.com/thanos-io/thanos-parquet-gateway/internal/slogerrcapture"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/proto/metapb"
	"github.com/thanos-io/thanos-parquet-gateway/proto/streampb"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

var bufPool sync.Pool

type discoveryConfig struct {
	concurrency int
	l           *slog.Logger
}

type DiscoveryOption func(*discoveryConfig)

func MetaConcurrency(c int) DiscoveryOption {
	return func(cfg *discoveryConfig) {
		cfg.concurrency = c
	}
}

func Logger(l *slog.Logger) DiscoveryOption {
	return func(cfg *discoveryConfig) {
		cfg.l = l
	}
}

type Discoverer struct {
	bkt objstore.Bucket

	mu     sync.Mutex
	blocks map[schema.ExternalLabelsHash]schema.ParquetBlocksStream

	concurrency int

	l *slog.Logger
}

func NewDiscoverer(bkt objstore.Bucket, opts ...DiscoveryOption) *Discoverer {
	cfg := discoveryConfig{
		concurrency: 1,
		l:           slog.Default(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	return &Discoverer{
		bkt:         bkt,
		blocks:      make(map[schema.ExternalLabelsHash]schema.ParquetBlocksStream),
		concurrency: cfg.concurrency,
		l:           cfg.l,
	}
}

func (s *Discoverer) Streams() map[schema.ExternalLabelsHash]schema.ParquetBlocksStream {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[schema.ExternalLabelsHash]schema.ParquetBlocksStream, len(s.blocks))

	for k := range s.blocks {
		res[k] = s.blocks[k]
	}

	return res
}

func (s *Discoverer) Discover(ctx context.Context) error {
	type futureBlocksStream struct {
		schema.ParquetBlocksStream

		streamFile  string
		filesByDate map[util.Date][]string
	}

	futureBlocks := make(map[schema.ExternalLabelsHash]futureBlocksStream)
	fbMtx := sync.Mutex{}

	err := s.bkt.Iter(ctx, "", func(n string) error {
		if eh, ok := schema.SplitStreamPath(n); ok {
			fbs, ok := futureBlocks[eh]
			if !ok {
				fbs = futureBlocksStream{}
			}

			if fbs.streamFile != "" {
				panic("BUG: discovered stream file twice")
			}
			fbs.streamFile = n
			futureBlocks[eh] = fbs

			return nil
		}

		if date, file, eh, ok := schema.SplitBlockPath(n); ok {
			fbs, ok := futureBlocks[eh]
			if !ok {
				fbs = futureBlocksStream{}
			}

			if fbs.DiscoveredDays == nil {
				fbs.DiscoveredDays = make(map[util.Date]struct{})
			}

			if file == schema.MetaFile {
				fbs.DiscoveredDays[date] = struct{}{}
			}

			if fbs.filesByDate == nil {
				fbs.filesByDate = make(map[util.Date][]string)
			}

			fbs.filesByDate[date] = append(fbs.filesByDate[date], file)

			futureBlocks[eh] = fbs
		}

		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return err
	}

	p := pool.New().WithContext(ctx).WithCancelOnError().WithFirstError().WithMaxGoroutines(s.concurrency)

	for discoveredHash := range futureBlocks {
		// NOTE(GiedriusS): can be without a stream file if it was previously uploaded
		// with no external labels.
		if futureBlocks[discoveredHash].streamFile == "" {
			if discoveredHash != 0 {
				delete(futureBlocks, discoveredHash)
			}
			continue
		}
	}

	downloadedDescriptors := make(map[schema.ExternalLabelsHash]schema.StreamDescriptor, len(futureBlocks))

	for discoveredHash := range futureBlocks {
		if discoveredHash == 0 {
			continue
		}
		p.Go(func(ctx context.Context) error {
			sd, err := readStreamDescriptorFile(ctx, s.bkt, discoveredHash, s.l)
			if err != nil {
				return fmt.Errorf("unable to read stream descriptor for hash %q: %w", discoveredHash.String(), err)
			}

			fbMtx.Lock()
			defer fbMtx.Unlock()

			downloadedDescriptors[discoveredHash] = sd

			return nil
		})
	}

	if err := p.Wait(); err != nil {
		return err
	}

	for discoveredHash := range downloadedDescriptors {
		fbs := futureBlocks[discoveredHash]
		fbs.StreamDescriptor = downloadedDescriptors[discoveredHash]
		futureBlocks[discoveredHash] = fbs
	}

	p = pool.New().WithContext(ctx).WithCancelOnError().WithFirstError().WithMaxGoroutines(s.concurrency)

	for discoveredHash := range futureBlocks {
		for blockDate, dateFiles := range futureBlocks[discoveredHash].filesByDate {
			if !slices.Contains(dateFiles, schema.MetaFile) {
				delete(futureBlocks[discoveredHash].filesByDate, blockDate)
			} else {
				futureBlocks[discoveredHash].DiscoveredDays[blockDate] = struct{}{}
			}
		}
	}

	downloadedMetas := make(map[schema.ExternalLabelsHash][]schema.Meta, len(futureBlocks))
	for discoveredHash := range futureBlocks {
		for blockDate := range futureBlocks[discoveredHash].filesByDate {
			p.Go(func(ctx context.Context) error {
				meta, err := readMetaFile(ctx, s.bkt, blockDate, discoveredHash, s.l)
				if err != nil {
					return fmt.Errorf("unable to read meta file for block date %q hash %q: %w", blockDate, discoveredHash.String(), err)
				}

				fbMtx.Lock()
				defer fbMtx.Unlock()

				downloadedMetas[discoveredHash] = append(downloadedMetas[discoveredHash], meta)

				return nil
			})
		}
	}

	if err := p.Wait(); err != nil {
		return err
	}

	for discoveredHash := range downloadedMetas {
		fbs := futureBlocks[discoveredHash]
		fbs.Metas = downloadedMetas[discoveredHash]
		futureBlocks[discoveredHash] = fbs
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for fbHash := range futureBlocks {
		s.blocks[fbHash] = futureBlocks[fbHash].ParquetBlocksStream
	}

	for fbHash := range s.blocks {
		if _, ok := futureBlocks[fbHash]; !ok {
			delete(s.blocks, fbHash)
		}
	}

	if len(s.blocks) != 0 {
		mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
		for _, bs := range s.blocks {
			for _, m := range bs.Metas {
				mint = min(mint, m.Mint)
				maxt = max(maxt, m.Maxt)
			}
		}
		syncMinTime.WithLabelValues(whatDiscoverer).Set(float64(mint))
		syncMaxTime.WithLabelValues(whatDiscoverer).Set(float64(maxt))
	}
	syncLastSuccessfulTime.WithLabelValues(whatDiscoverer).SetToCurrentTime()

	return nil
}

func readStreamDescriptorFile(ctx context.Context, bkt objstore.Bucket, extLabelsHash schema.ExternalLabelsHash, l *slog.Logger) (schema.StreamDescriptor, error) {
	sdfile := schema.StreamDescriptorFileNameForBlock(extLabelsHash)
	attrs, err := bkt.Attributes(ctx, sdfile)
	if err != nil {
		return schema.StreamDescriptor{}, fmt.Errorf("unable to attr %s: %w", sdfile, err)
	}
	rdr, err := bkt.Get(ctx, sdfile)
	if err != nil {
		return schema.StreamDescriptor{}, fmt.Errorf("unable to get %s: %w", sdfile, err)
	}
	defer slogerrcapture.Do(l, rdr.Close, "closing stream descriptor file reader %s", sdfile)

	bp, ok := bufPool.Get().(*[]byte)
	if !ok {
		b := make([]byte, 0, attrs.Size)
		bp = &b
	}
	defer bufPool.Put(bp)

	buf := *bp
	if cap(buf) < int(attrs.Size) {
		buf = make([]byte, attrs.Size)
		*bp = buf
	} else {
		buf = buf[:attrs.Size]
	}
	*bp = buf

	if _, err := io.ReadFull(rdr, buf); err != nil {
		return schema.StreamDescriptor{}, fmt.Errorf("unable to read %s: %w", sdfile, err)
	}

	metapb := &streampb.StreamDescriptor{}
	if err := proto.Unmarshal(buf, metapb); err != nil {
		return schema.StreamDescriptor{}, fmt.Errorf("unable to read %s: %w", sdfile, err)
	}

	if len(metapb.GetExternalLabels()) == 0 {
		return schema.StreamDescriptor{}, fmt.Errorf("stream descriptor %s has no external labels", sdfile)
	}

	extLbls := schema.ExternalLabels(metapb.GetExternalLabels())
	if extLbls.Hash() != extLabelsHash {
		return schema.StreamDescriptor{}, fmt.Errorf("stream descriptor %s has invalid external labels hash: got %d, want %d", sdfile, extLbls.Hash(), extLabelsHash)
	}

	return schema.StreamDescriptor{
		ExternalLabels: metapb.GetExternalLabels(),
	}, nil
}

func readMetaFile(ctx context.Context, bkt objstore.Bucket, date util.Date, extLabelsHash schema.ExternalLabelsHash, l *slog.Logger) (schema.Meta, error) {
	mfile := schema.MetaFileNameForBlock(date, extLabelsHash)
	attrs, err := bkt.Attributes(ctx, mfile)
	if err != nil {
		return schema.Meta{}, fmt.Errorf("unable to attr %s: %w", mfile, err)
	}

	rdr, err := bkt.Get(ctx, mfile)
	if err != nil {
		return schema.Meta{}, fmt.Errorf("unable to get %s: %w", mfile, err)
	}
	defer slogerrcapture.Do(l, rdr.Close, "closing meta file reader %s", mfile)

	bp, ok := bufPool.Get().(*[]byte)
	if !ok {
		b := make([]byte, 0, attrs.Size)
		bp = &b
	}
	defer bufPool.Put(bp)

	buf := *bp
	if cap(buf) < int(attrs.Size) {
		buf = make([]byte, attrs.Size)
		*bp = buf
	} else {
		buf = buf[:attrs.Size]
	}
	*bp = buf

	if _, err := io.ReadFull(rdr, buf); err != nil {
		return schema.Meta{}, fmt.Errorf("unable to read %s: %w", mfile, err)
	}

	metapb := &metapb.Metadata{}
	if err := proto.Unmarshal(buf, metapb); err != nil {
		return schema.Meta{}, fmt.Errorf("unable to read %s: %w", mfile, err)
	}

	// for version == 0
	m := make(map[string][]string, len(metapb.GetColumnsForName()))
	for k, v := range metapb.GetColumnsForName() {
		m[k] = v.GetColumns()
	}

	fromULIDs := make(map[ulid.ULID]struct{}, len(metapb.GetConvertedFromBLIDs()))
	for _, s := range metapb.GetConvertedFromBLIDs() {
		id, err := ulid.Parse(s)
		if err != nil {
			return schema.Meta{}, fmt.Errorf("unable to parse converted from BLID %q: %w", s, err)
		}
		fromULIDs[id] = struct{}{}
	}
	return schema.Meta{
		Version:            int(metapb.GetVersion()),
		Date:               date,
		Mint:               metapb.GetMint(),
		Maxt:               metapb.GetMaxt(),
		Shards:             metapb.GetShards(),
		ColumnsForName:     m,
		ConvertedFromBLIDs: fromULIDs,
	}, nil
}

type tsdbDiscoveryConfig struct {
	concurrency int

	externalLabelMatchers []*labels.Matcher
	minBlockAge           time.Duration
	l                     *slog.Logger
}

type TSDBDiscoveryOption func(*tsdbDiscoveryConfig)

func TSDBMetaConcurrency(c int) TSDBDiscoveryOption {
	return func(cfg *tsdbDiscoveryConfig) {
		cfg.concurrency = c
	}
}

func TSDBMatchExternalLabels(ms ...*labels.Matcher) TSDBDiscoveryOption {
	return func(cfg *tsdbDiscoveryConfig) {
		cfg.externalLabelMatchers = ms
	}
}

func WithLogger(l *slog.Logger) TSDBDiscoveryOption {
	return func(cfg *tsdbDiscoveryConfig) {
		cfg.l = l
	}
}

func TSDBMinBlockAge(d time.Duration) TSDBDiscoveryOption {
	return func(cfg *tsdbDiscoveryConfig) {
		cfg.minBlockAge = d
	}
}

type TSDBDiscoverer struct {
	bkt objstore.Bucket

	mu    sync.Mutex
	metas map[string]metadata.Meta

	externalLabelMatchers []*labels.Matcher
	minBlockAge           time.Duration

	concurrency int
	l           *slog.Logger
}

func NewTSDBDiscoverer(bkt objstore.Bucket, opts ...TSDBDiscoveryOption) *TSDBDiscoverer {
	cfg := tsdbDiscoveryConfig{
		concurrency: 1,
		l:           slog.Default(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	return &TSDBDiscoverer{
		bkt:                   bkt,
		metas:                 make(map[string]metadata.Meta),
		concurrency:           cfg.concurrency,
		externalLabelMatchers: cfg.externalLabelMatchers,
		minBlockAge:           cfg.minBlockAge,
		l:                     cfg.l,
	}
}

type TSDBStream struct {
}

func (s *TSDBDiscoverer) Streams() map[schema.ExternalLabelsHash]schema.TSDBBlocksStream {
	out := make(map[schema.ExternalLabelsHash]schema.TSDBBlocksStream)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, m := range s.metas {
		extLbls := schema.ExternalLabels(m.Thanos.Labels)
		eh := extLbls.Hash()

		bs, ok := out[eh]
		if !ok {
			bs = schema.TSDBBlocksStream{
				StreamDescriptor: schema.StreamDescriptor{
					ExternalLabels: m.Thanos.Labels,
				},
				DiscoveredDays: make(map[util.Date]struct{}),
			}
		}
		bs.Metas = append(bs.Metas, m)
		for _, d := range util.SplitIntoDates(m.MinTime, m.MaxTime) {
			bs.DiscoveredDays[d] = struct{}{}
		}
		out[eh] = bs
	}

	return out
}

func (s *TSDBDiscoverer) Discover(ctx context.Context) error {
	m := make(map[string][]string)
	err := s.bkt.Iter(ctx, "", func(n string) error {
		split := strings.Split(n, "/")
		if len(split) != 2 {
			return nil
		}
		id, f := split[0], split[1]

		m[id] = append(m[id], f)
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return err
	}

	type metaOrError struct {
		m   metadata.Meta
		err error
	}

	metaC := make(chan metaOrError)
	go func() {
		defer close(metaC)

		workerC := make(chan string, s.concurrency)
		go func() {
			defer close(workerC)

			for k, v := range m {
				if !slices.Contains(v, metadata.MetaFilename) {
					// skip incomplete block
					continue
				}
				if slices.Contains(v, metadata.DeletionMarkFilename) {
					// skip block that is about to be deleted
					continue
				}
				if _, ok := s.metas[k]; ok {
					// we already got the block
					continue
				}
				workerC <- k
			}
		}()

		var wg sync.WaitGroup
		defer wg.Wait()

		for range s.concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := range workerC {
					meta, err := s.readMetaFile(ctx, k)
					if err != nil {
						metaC <- metaOrError{err: fmt.Errorf("unable to read meta file for %q: %w", k, err)}
					} else {
						metaC <- metaOrError{m: meta}
					}
				}
			}()
		}
	}()

	am := make(map[string]struct{})
	for k, v := range m {
		if !slices.Contains(v, metadata.MetaFilename) {
			// skip incomplete block
			continue
		}
		if slices.Contains(v, metadata.DeletionMarkFilename) {
			// skip block that is about to be deleted
			continue
		}
		am[k] = struct{}{}
	}

	nm := make(map[string]metadata.Meta)
	for m := range metaC {
		if m.err != nil {
			return fmt.Errorf("unable to read meta: %w", m.err)
		}
		nm[m.m.ULID.String()] = m.m
	}

	// filter for metas that match our external labels
	maps.DeleteFunc(nm, func(_ string, v metadata.Meta) bool {
		series := labels.FromMap(v.Thanos.Labels)

		for _, m := range s.externalLabelMatchers {
			if !m.Matches(series.Get(m.Name)) {
				return true
			}
		}
		return false
	})

	// filter for metas that are not downsampled
	maps.DeleteFunc(nm, func(_ string, v metadata.Meta) bool {
		return v.Thanos.Downsample.Resolution != downsample.ResLevel0
	})

	// filter out blocks with no chunks (cannot be converted)
	maps.DeleteFunc(nm, func(_ string, v metadata.Meta) bool {
		return v.Stats.NumChunks == 0
	})

	s.mu.Lock()
	defer s.mu.Unlock()

	maps.Copy(s.metas, nm)

	// filter for metas that dont contain data after "now-minAge"
	maps.DeleteFunc(s.metas, func(_ string, v metadata.Meta) bool {
		return time.UnixMilli(v.MaxTime).After(time.Now().Add(-s.minBlockAge))
	})
	// delete metas that are no longer in the bucket
	maps.DeleteFunc(s.metas, func(k string, _ metadata.Meta) bool {
		_, ok := am[k]
		return !ok
	})

	if len(s.metas) != 0 {
		mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
		for _, v := range s.metas {
			mint = min(mint, v.MinTime)
			maxt = max(maxt, v.MaxTime)
		}
		syncMinTime.WithLabelValues(whatDiscoverer).Set(float64(mint))
		syncMaxTime.WithLabelValues(whatDiscoverer).Set(float64(maxt))
	}
	syncLastSuccessfulTime.WithLabelValues(whatDiscoverer).SetToCurrentTime()

	return nil
}

func (s *TSDBDiscoverer) readMetaFile(ctx context.Context, name string) (metadata.Meta, error) {
	mfile := fmt.Sprintf("%s/%s", name, metadata.MetaFilename)
	if _, err := s.bkt.Attributes(ctx, mfile); err != nil {
		return metadata.Meta{}, fmt.Errorf("unable to attr %s: %w", mfile, err)
	}
	rdr, err := s.bkt.Get(ctx, mfile)
	if err != nil {
		return metadata.Meta{}, fmt.Errorf("unable to get %s: %w", mfile, err)
	}
	defer slogerrcapture.Do(s.l, rdr.Close, "closing meta file reader %s", mfile)

	var m metadata.Meta
	if err := json.NewDecoder(rdr).Decode(&m); err != nil {
		return metadata.Meta{}, fmt.Errorf("unable to decode %s: %w", mfile, err)
	}
	return m, nil
}
