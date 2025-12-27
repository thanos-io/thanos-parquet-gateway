// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"log/slog"
	"time"

	"github.com/thanos-io/thanos-parquet-gateway/internal/log"
)

type loggingService struct {
	next Service
}

var _ Service = (*loggingService)(nil)

func (s *loggingService) GetMetricsCardinality(ctx context.Context, start, end time.Time, limit int, matchers []*Matcher) (*CardinalityResult, error) {
	l := log.Ctx(ctx).With(
		slog.String("method", "GetMetricsCardinality"),
		slog.Time("query.start", start),
		slog.Time("query.end", end),
		slog.Int("query.limit", limit),
		slog.Int("query.matchers", len(matchers)),
	)
	l.Info("Starting")

	result, err := s.next.GetMetricsCardinality(ctx, start, end, limit, matchers)
	if err != nil {
		l.Error("Failed", "err", err)
		return nil, err
	}

	l.Info("Completed",
		slog.Int("blocks_analyzed", result.BlocksAnalyzed),
		slog.Int("days", len(result.Days)),
		slog.Int64("total_series", result.TotalSeries),
		slog.Int64("total_metrics", result.TotalMetrics),
	)
	return result, nil
}

func (s *loggingService) GetLabelsCardinality(ctx context.Context, start, end time.Time, limit int, matchers []*Matcher) (*LabelsCardinalityPerDayResult, error) {
	l := log.Ctx(ctx).With(
		slog.String("method", "GetLabelsCardinality"),
		slog.Time("query.start", start),
		slog.Time("query.end", end),
		slog.Int("query.limit", limit),
		slog.Int("query.matchers", len(matchers)),
	)
	l.Info("Starting")

	result, err := s.next.GetLabelsCardinality(ctx, start, end, limit, matchers)
	if err != nil {
		l.Error("Failed", "err", err)
		return nil, err
	}

	l.Info("Completed",
		slog.Int("blocks_analyzed", result.BlocksAnalyzed),
		slog.Int("days", len(result.Days)),
		slog.Int("total_labels", result.TotalLabels),
		slog.Int64("total_unique_values", result.TotalUniqueValues),
	)
	return result, nil
}

func (s *loggingService) Close() error {
	l := log.Ctx(context.Background())
	l.Info("Closing")
	err := s.next.Close()
	if err != nil {
		l.Error("Failed", "err", err)
		return err
	}
	l.Info("Closed")
	return nil
}
