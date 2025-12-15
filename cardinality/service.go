// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2" // database/sql driver
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos-parquet-gateway/internal/log"
)

const (
	DefaultQueryCacheSize = 100
	DefaultQueryCacheTTL  = 10 * time.Minute

	DefaultLimit     = 100
	MaxLimit         = 1000
	MaxDateRangeDays = 365
)

type Service interface {
	GetMetricsCardinality(ctx context.Context, start, end time.Time, limit int, matchers []*Matcher) (*CardinalityResult, error)
	GetLabelsCardinality(ctx context.Context, start, end time.Time, limit int, matchers []*Matcher) (*LabelsCardinalityPerDayResult, error)
	Close() error
}

type serviceImpl struct {
	blockResolver  BlockResolver
	parquetCache   Cache
	bucket         objstore.Bucket
	db             *sql.DB
	queryCache     *QueryCache
	metricsBuilder *MetricsBuilder
	labelsBuilder  *LabelsBuilder
	reader         *ParquetIndexReader

	concurrency int
	cacheDir    string

	preloaderEnabled     bool
	preloaderDays        int
	preloaderConcurrency int
	preloaderInterval    time.Duration
	preloaderCancel      context.CancelFunc
	preloaderDone        chan struct{}
}

type ServiceOption func(*serviceImpl)

func WithQueryCacheSize(size int) ServiceOption {
	return func(s *serviceImpl) {
		s.queryCache = NewQueryCache(size, DefaultQueryCacheTTL)
	}
}

func WithQueryCacheTTL(ttl time.Duration) ServiceOption {
	return func(s *serviceImpl) {
		s.queryCache = NewQueryCache(DefaultQueryCacheSize, ttl)
	}
}

func WithQueryCache(size int, ttl time.Duration) ServiceOption {
	return func(s *serviceImpl) {
		s.queryCache = NewQueryCache(size, ttl)
	}
}

func WithConcurrency(n int) ServiceOption {
	return func(s *serviceImpl) {
		if n > 0 {
			s.concurrency = n
		}
	}
}

const (
	DefaultPreloadDays        = 30
	DefaultPreloadConcurrency = 4
	DefaultPreloadInterval    = 1 * time.Hour
)

type PreloaderConfig struct {
	Days        int
	Concurrency int
	Interval    time.Duration
}

func WithPreloader(cfg PreloaderConfig) ServiceOption {
	return func(s *serviceImpl) {
		s.preloaderEnabled = true
		if cfg.Days > 0 {
			s.preloaderDays = cfg.Days
		} else {
			s.preloaderDays = DefaultPreloadDays
		}
		if cfg.Concurrency > 0 {
			s.preloaderConcurrency = cfg.Concurrency
		} else {
			s.preloaderConcurrency = DefaultPreloadConcurrency
		}
		if cfg.Interval > 0 {
			s.preloaderInterval = cfg.Interval
		} else {
			s.preloaderInterval = DefaultPreloadInterval
		}
	}
}

type MetricCardinality struct {
	MetricName  string  `json:"metric_name"`
	SeriesCount int64   `json:"series_count"`
	Percentage  float64 `json:"percentage"`
}

type DailyMetrics struct {
	Date    string              `json:"date"`
	Metrics []MetricCardinality `json:"metrics"`
}

type CardinalityResult struct {
	Start          time.Time      `json:"start"`
	End            time.Time      `json:"end"`
	Days           []DailyMetrics `json:"days"`
	TotalSeries    int64          `json:"total_series"`
	TotalMetrics   int64          `json:"total_metrics"`
	BlocksAnalyzed int            `json:"blocks_analyzed"`
}

type LabelCardinality struct {
	LabelName    string  `json:"label_name"`
	UniqueValues int64   `json:"unique_values"`
	Percentage   float64 `json:"percentage"`
}

type DailyLabels struct {
	Date   string             `json:"date"`
	Labels []LabelCardinality `json:"labels"`
}

type LabelsCardinalityPerDayResult struct {
	Start             time.Time     `json:"start"`
	End               time.Time     `json:"end"`
	Days              []DailyLabels `json:"days"`
	TotalLabels       int           `json:"total_labels"`
	TotalUniqueValues int64         `json:"total_unique_values"`
	BlocksAnalyzed    int           `json:"blocks_analyzed"`
}

func NewService(bucket objstore.Bucket, cacheDir string, opts ...ServiceOption) (Service, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	svc := &serviceImpl{
		db:            db,
		bucket:        bucket,
		blockResolver: NewBlockResolver(bucket),
		parquetCache:  NewFileCache(cacheDir, bucket),
		queryCache:    NewQueryCache(DefaultQueryCacheSize, DefaultQueryCacheTTL),
		concurrency:   runtime.NumCPU() * 2,
		cacheDir:      cacheDir,

		metricsBuilder: NewMetricsBuilder(db, cacheDir),
		labelsBuilder:  NewLabelsBuilder(db, cacheDir),
		reader:         NewParquetIndexReader(db, cacheDir),
	}

	for _, opt := range opts {
		opt(svc)
	}

	if svc.preloaderEnabled {
		svc.startPreloader(context.Background())
	}

	return &loggingService{next: svc}, nil
}

func (s *serviceImpl) Close() error {
	s.stopPreloader()
	return s.db.Close()
}

func (s *serviceImpl) startPreloader(ctx context.Context) {
	ctx, s.preloaderCancel = context.WithCancel(ctx)
	ctx = log.With(ctx, slog.String("component", "preloader"))

	s.preloaderDone = make(chan struct{})
	go func() {
		defer close(s.preloaderDone)
		s.preloadLoop(ctx)
	}()
}

func (s *serviceImpl) stopPreloader() {
	if s.preloaderCancel != nil {
		s.preloaderCancel()
	}
	if s.preloaderDone != nil {
		<-s.preloaderDone
	}
}

func (s *serviceImpl) preloadLoop(ctx context.Context) {
	l := log.Ctx(ctx)
	s.preloadOnce(ctx)

	ticker := time.NewTicker(s.preloaderInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			l.Info("Preloader stopped")
			return
		case <-ticker.C:
			s.preloadOnce(ctx)
		}
	}
}

func (s *serviceImpl) preloadOnce(ctx context.Context) {
	l := log.Ctx(ctx).With(
		slog.String("method", "preloadOnce"),
		slog.Int("days", s.preloaderDays),
		slog.Int("concurrency", s.preloaderConcurrency),
	)
	l.Info("Starting preload cycle")
	startTime := time.Now()

	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -s.preloaderDays+1) // Include today

	blocks, err := s.blockResolver.ResolveBlocks(ctx, start, end)
	if err != nil {
		l.Error("Failed to resolve blocks", "err", err)
		return
	}

	if len(blocks) == 0 {
		l.Info("No blocks to preload")
		return
	}

	l.Debug("Resolved blocks",
		slog.Int("total_blocks", len(blocks)),
		slog.Time("start", start),
		slog.Time("end", end),
	)

	type result struct {
		success bool
	}
	resultCh := make(chan result, s.preloaderConcurrency*2)
	blockCh := make(chan string, s.preloaderConcurrency*2)

	go func() {
		for _, block := range blocks {
			blockCh <- block
		}
		close(blockCh)
	}()

	var wg sync.WaitGroup
	for i := 0; i < s.preloaderConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for block := range blockCh {
				if ctx.Err() != nil {
					return
				}
				if err := s.preloadBlock(ctx, block); err != nil {
					log.Ctx(ctx).Warn("Failed to preload block",
						slog.String("block", block),
						slog.Any("err", err),
					)
					resultCh <- result{success: false}
				} else {
					resultCh <- result{success: true}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var successCount, errorCount int64
	for r := range resultCh {
		if r.success {
			successCount++
		} else {
			errorCount++
		}
	}

	l.Info("Preload cycle completed",
		slog.Int64("success", successCount),
		slog.Int64("errors", errorCount),
		slog.Int("total_blocks", len(blocks)),
		slog.Duration("duration", time.Since(startTime)),
	)
}

func (s *serviceImpl) preloadBlock(ctx context.Context, block string) error {
	if err := s.FetchBlock(ctx, block); err != nil {
		return fmt.Errorf("fetch block: %w", err)
	}

	if err := s.labelsBuilder.EnsureBuilt(ctx, block); err != nil {
		return fmt.Errorf("preload labels index: %w", err)
	}

	if err := s.metricsBuilder.EnsureBuilt(ctx, block); err != nil {
		return fmt.Errorf("preload metrics index: %w", err)
	}

	log.Ctx(ctx).Debug("Preloaded block", slog.String("block", block))
	return nil
}

func (s *serviceImpl) GetMetricsCardinality(ctx context.Context, start, end time.Time, limit int, matchers []*Matcher) (*CardinalityResult, error) {
	l := log.Ctx(ctx)

	blocks, err := s.blockResolver.ResolveBlocks(ctx, start, end)
	if err != nil {
		return nil, fmt.Errorf("resolve blocks: %w", err)
	}

	if len(blocks) == 0 {
		return &CardinalityResult{
			Start: start,
			End:   end,
		}, nil
	}

	needsRawParquet := HasLabelMatchers(matchers)

	indexStart := time.Now()
	if needsRawParquet {
		if err := s.ensureBlocksFetched(ctx, blocks); err != nil {
			return nil, fmt.Errorf("fetch blocks: %w", err)
		}
	} else {
		if err := s.ensureMetricsBuilt(ctx, blocks); err != nil {
			return nil, fmt.Errorf("ensure metrics indexed: %w", err)
		}
	}
	l.Debug("Ensured blocks ready",
		slog.Duration("duration", time.Since(indexStart)),
		slog.Int("blocks", len(blocks)),
		slog.Bool("raw_parquet", needsRawParquet),
	)

	queryStart := time.Now()
	result, err := s.reader.QueryMetricsCardinality(ctx, start, end, limit, matchers)
	if err != nil {
		return nil, fmt.Errorf("query metrics cardinality: %w", err)
	}
	l.Debug("Queried metrics cardinality",
		slog.Duration("duration", time.Since(queryStart)),
		slog.Int("days", len(result.Days)),
	)

	return result, nil
}

func (s *serviceImpl) ensureMetricsBuilt(ctx context.Context, blocks []string) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.concurrency)

	for _, block := range blocks {
		g.Go(func() error {
			if err := s.fetchBlock(ctx, block); err != nil {
				return fmt.Errorf("fetch block %s: %w", block, err)
			}
			if err := s.metricsBuilder.EnsureBuilt(ctx, block); err != nil {
				return fmt.Errorf("ensure indexed %s: %w", block, err)
			}
			return nil
		})
	}

	return g.Wait()
}

func (s *serviceImpl) GetLabelsCardinality(ctx context.Context, start, end time.Time, limit int, matchers []*Matcher) (*LabelsCardinalityPerDayResult, error) {
	l := log.Ctx(ctx)

	blocks, err := s.blockResolver.ResolveBlocks(ctx, start, end)
	if err != nil {
		return nil, fmt.Errorf("resolve blocks: %w", err)
	}

	if len(blocks) == 0 {
		return &LabelsCardinalityPerDayResult{
			Start: start,
			End:   end,
			Days:  []DailyLabels{},
		}, nil
	}

	indexStart := time.Now()
	if err := s.ensureBlocksFetched(ctx, blocks); err != nil {
		return nil, fmt.Errorf("fetch blocks: %w", err)
	}
	l.Debug("Fetched blocks",
		slog.Duration("duration", time.Since(indexStart)),
		slog.Int("blocks", len(blocks)),
	)

	if len(matchers) == 0 {
		buildStart := time.Now()
		if err := s.ensureLabelsBuilt(ctx, blocks); err != nil {
			return nil, fmt.Errorf("ensure labels indexed: %w", err)
		}
		l.Debug("Ensured labels indexes",
			slog.Duration("duration", time.Since(buildStart)),
			slog.Int("blocks", len(blocks)),
		)
	}

	queryStart := time.Now()
	result, err := s.reader.QueryLabelsCardinality(ctx, start, end, limit, matchers)
	if err != nil {
		return nil, fmt.Errorf("query labels cardinality: %w", err)
	}
	l.Debug("Queried labels cardinality",
		slog.Duration("duration", time.Since(queryStart)),
		slog.Int("days", len(result.Days)),
	)

	return result, nil
}

func (s *serviceImpl) ensureLabelsBuilt(ctx context.Context, blocks []string) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.concurrency)

	for _, block := range blocks {
		g.Go(func() error {
			if err := s.fetchBlock(ctx, block); err != nil {
				return fmt.Errorf("fetch block %s: %w", block, err)
			}
			if err := s.labelsBuilder.EnsureBuilt(ctx, block); err != nil {
				return fmt.Errorf("ensure indexed %s: %w", block, err)
			}
			return nil
		})
	}

	return g.Wait()
}

func (s *serviceImpl) ensureBlocksFetched(ctx context.Context, blocks []string) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.concurrency)

	for _, block := range blocks {
		g.Go(func() error {
			if err := s.fetchBlock(ctx, block); err != nil {
				return fmt.Errorf("fetch block %s: %w", block, err)
			}
			return nil
		})
	}

	return g.Wait()
}

func (s *serviceImpl) cacheBlocks(ctx context.Context, blocks []string) []string {
	l := log.Ctx(ctx)
	var cached []string
	for _, blockName := range blocks {
		if ctx.Err() != nil {
			break
		}

		shardPaths, err := s.blockResolver.ListShardPaths(ctx, blockName)
		if err != nil {
			l.Warn("Failed to list shards", "err", err, slog.String("block", blockName))
			continue
		}

		allCached := true
		for _, sp := range shardPaths {
			if _, err := s.parquetCache.Get(ctx, sp); err != nil {
				l.Warn("Failed to cache shard", "err", err, slog.String("shard", sp))
				allCached = false
				break
			}
		}

		if allCached {
			cached = append(cached, blockName)
		}
	}
	return cached
}

func (s *serviceImpl) fetchBlock(ctx context.Context, block string) error {
	shardPaths, err := s.blockResolver.ListShardPaths(ctx, block)
	if err != nil {
		return fmt.Errorf("list shards: %w", err)
	}

	for _, sp := range shardPaths {
		if _, err := s.parquetCache.Get(ctx, sp); err != nil {
			return fmt.Errorf("cache shard %s: %w", sp, err)
		}
	}
	return nil
}

func (s *serviceImpl) FetchBlock(ctx context.Context, block string) error {
	return s.fetchBlock(ctx, block)
}

func startOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func fillMissingDays(days []DailyMetrics, start, end time.Time) []DailyMetrics {
	dayMap := make(map[string]DailyMetrics)
	for _, day := range days {
		dayMap[day.Date] = day
	}

	var result []DailyMetrics
	current := startOfDay(start)
	endDay := startOfDay(end)

	for !current.After(endDay) {
		dateStr := current.Format("2006-01-02")
		if existing, ok := dayMap[dateStr]; ok {
			result = append(result, existing)
		} else {
			result = append(result, DailyMetrics{
				Date:    dateStr,
				Metrics: []MetricCardinality{},
			})
		}
		current = current.AddDate(0, 0, 1)
	}

	return result
}

func fillMissingLabelDays(days []DailyLabels, start, end time.Time) []DailyLabels {
	dayMap := make(map[string]DailyLabels)
	for _, day := range days {
		dayMap[day.Date] = day
	}

	var result []DailyLabels
	current := startOfDay(start)
	endDay := startOfDay(end)

	for !current.After(endDay) {
		dateStr := current.Format("2006-01-02")
		if existing, ok := dayMap[dateStr]; ok {
			result = append(result, existing)
		} else {
			result = append(result, DailyLabels{
				Date:   dateStr,
				Labels: []LabelCardinality{},
			})
		}
		current = current.AddDate(0, 0, 1)
	}

	return result
}

func parseBlockDate(blockName string) (time.Time, error) {
	var year, month, day int
	n, err := fmt.Sscanf(blockName, "%04d/%02d/%02d", &year, &month, &day)
	if err != nil || n != 3 {
		return time.Time{}, fmt.Errorf("invalid block name format: %s", blockName)
	}
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
}
