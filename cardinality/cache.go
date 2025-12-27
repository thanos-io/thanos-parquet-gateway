// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"time"
)

const (
	HistoricalCacheTTL   = 24 * time.Hour
	TodayCacheTTL        = 5 * time.Minute
	CacheCleanupInterval = 10 * time.Minute
)

type Cache interface {
	Get(ctx context.Context, remotePath string) (localPath string, err error)
	Exists(remotePath string) bool
	Evict(ctx context.Context, maxAge time.Duration) error
	CacheDir() string
}

type MetricsEntry struct {
	Date         string              `json:"date"`
	Block        string              `json:"block"`
	Metrics      []MetricCardinality `json:"metrics"`
	TotalSeries  int64               `json:"total_series"`
	TotalMetrics int                 `json:"total_metrics"`
	CachedAt     time.Time           `json:"cached_at"`
}

type LabelsEntry struct {
	Date              string             `json:"date"`
	Block             string             `json:"block"`
	Labels            []LabelCardinality `json:"labels"`
	TotalLabels       int                `json:"total_labels"`
	TotalUniqueValues int64              `json:"total_unique_values"`
	CachedAt          time.Time          `json:"cached_at"`
}
