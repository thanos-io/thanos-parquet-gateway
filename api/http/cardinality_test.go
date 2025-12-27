// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/common/route"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos-parquet-gateway/cardinality"
)

type mockCardinalityService struct {
	getMetricsCardinalityFunc func(ctx context.Context, start, end time.Time, limit int, matchers []*cardinality.Matcher) (*cardinality.CardinalityResult, error)
	getLabelsCardinalityFunc  func(ctx context.Context, start, end time.Time, limit int, matchers []*cardinality.Matcher) (*cardinality.LabelsCardinalityPerDayResult, error)
}

func (m *mockCardinalityService) GetMetricsCardinality(ctx context.Context, start, end time.Time, limit int, matchers []*cardinality.Matcher) (*cardinality.CardinalityResult, error) {
	if m.getMetricsCardinalityFunc != nil {
		return m.getMetricsCardinalityFunc(ctx, start, end, limit, matchers)
	}
	return nil, nil
}

func (m *mockCardinalityService) GetLabelsCardinality(ctx context.Context, start, end time.Time, limit int, matchers []*cardinality.Matcher) (*cardinality.LabelsCardinalityPerDayResult, error) {
	if m.getLabelsCardinalityFunc != nil {
		return m.getLabelsCardinalityFunc(ctx, start, end, limit, matchers)
	}
	return nil, nil
}

func (m *mockCardinalityService) Close() error {
	return nil
}

// Helper to convert date to Unix timestamp
func toUnix(year, month, day int) int64 {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC).Unix()
}

func TestCardinalityAPI_GetCardinality(t *testing.T) {
	startTS := toUnix(2025, 12, 1)
	endTS := toUnix(2025, 12, 2)

	tests := []struct {
		name        string
		queryParams string
		mockResult  *cardinality.CardinalityResult
		mockErr     error
		wantStatus  int
		wantErrType string
	}{
		{
			name:        "success",
			queryParams: fmt.Sprintf("start=%d&end=%d&limit=10", startTS, endTS),
			mockResult: &cardinality.CardinalityResult{
				Start: time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2025, 12, 2, 23, 59, 59, 999999999, time.UTC),
				Days: []cardinality.DailyMetrics{
					{
						Date: "2025-12-01",
						Metrics: []cardinality.MetricCardinality{
							{MetricName: "metric_a", SeriesCount: 100, Percentage: 66.67},
							{MetricName: "metric_b", SeriesCount: 50, Percentage: 33.33},
						},
					},
				},
				TotalSeries:    150,
				TotalMetrics:   2,
				BlocksAnalyzed: 2,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "missing start",
			queryParams: fmt.Sprintf("end=%d&limit=10", endTS),
			wantStatus:  http.StatusBadRequest,
			wantErrType: errBadRequest,
		},
		{
			name:        "missing end",
			queryParams: fmt.Sprintf("start=%d&limit=10", startTS),
			wantStatus:  http.StatusBadRequest,
			wantErrType: errBadRequest,
		},
		{
			name:        "invalid start (not a number)",
			queryParams: fmt.Sprintf("start=not-a-number&end=%d&limit=10", endTS),
			wantStatus:  http.StatusBadRequest,
			wantErrType: errBadRequest,
		},
		{
			name:        "invalid end (not a number)",
			queryParams: fmt.Sprintf("start=%d&end=not-a-number&limit=10", startTS),
			wantStatus:  http.StatusBadRequest,
			wantErrType: errBadRequest,
		},
		{
			name:        "start after end",
			queryParams: fmt.Sprintf("start=%d&end=%d&limit=10", toUnix(2025, 12, 5), toUnix(2025, 12, 1)),
			wantStatus:  http.StatusBadRequest,
			wantErrType: errBadRequest,
		},
		{
			name:        "limit too high",
			queryParams: fmt.Sprintf("start=%d&end=%d&limit=2000", startTS, endTS),
			wantStatus:  http.StatusBadRequest,
			wantErrType: errBadRequest,
		},
		{
			name:        "invalid limit (not a number)",
			queryParams: fmt.Sprintf("start=%d&end=%d&limit=abc", startTS, endTS),
			wantStatus:  http.StatusBadRequest,
			wantErrType: errBadRequest,
		},
		{
			name:        "no data available",
			queryParams: fmt.Sprintf("start=%d&end=%d&limit=10", startTS, endTS),
			mockErr:     cardinality.ErrNoDataAvailable,
			wantStatus:  http.StatusBadRequest,
			wantErrType: errBadRequest,
		},
		{
			name:        "default limit applied when not specified",
			queryParams: fmt.Sprintf("start=%d&end=%d", startTS, endTS),
			mockResult: &cardinality.CardinalityResult{
				Start:          time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
				End:            time.Date(2025, 12, 2, 23, 59, 59, 999999999, time.UTC),
				Days:           []cardinality.DailyMetrics{},
				TotalSeries:    0,
				TotalMetrics:   0,
				BlocksAnalyzed: 0,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "with metric_name filter",
			queryParams: fmt.Sprintf("start=%d&end=%d&limit=10&metric_name=http_requests_total", startTS, endTS),
			mockResult: &cardinality.CardinalityResult{
				Start:          time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
				End:            time.Date(2025, 12, 2, 23, 59, 59, 999999999, time.UTC),
				Days:           []cardinality.DailyMetrics{},
				TotalSeries:    100,
				TotalMetrics:   1,
				BlocksAnalyzed: 2,
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &mockCardinalityService{
				getMetricsCardinalityFunc: func(_ context.Context, _, _ time.Time, _ int, _ []*cardinality.Matcher) (*cardinality.CardinalityResult, error) {
					if tt.mockErr != nil {
						return nil, tt.mockErr
					}
					return tt.mockResult, nil
				},
			}

			r := route.New()
			api := r.WithPrefix("/api/v1")
			RegisterCardinalityV1(api, svc)

			req := httptest.NewRequest(http.MethodGet, "/api/v1/cardinality/metrics?"+tt.queryParams, nil)
			rec := httptest.NewRecorder()

			r.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp apiResponse
			err := json.Unmarshal(rec.Body.Bytes(), &resp)
			require.NoError(t, err)

			if tt.wantErrType != "" {
				assert.Equal(t, statusError, resp.Status)
				assert.Equal(t, tt.wantErrType, resp.ErrorType)
			} else {
				assert.Equal(t, statusSuccess, resp.Status)
				assert.NotNil(t, resp.Data)
			}
		})
	}
}

func TestCardinalityAPI_GetLabelsCardinality(t *testing.T) {
	startTS := toUnix(2025, 12, 1)
	endTS := toUnix(2025, 12, 2)

	tests := []struct {
		name        string
		queryParams string
		mockResult  *cardinality.LabelsCardinalityPerDayResult
		mockErr     error
		wantStatus  int
		wantErrType string
	}{
		{
			name:        "success",
			queryParams: fmt.Sprintf("start=%d&end=%d&limit=10", startTS, endTS),
			mockResult: &cardinality.LabelsCardinalityPerDayResult{
				Days: []cardinality.DailyLabels{
					{
						Date: "2025-12-01",
						Labels: []cardinality.LabelCardinality{
							{LabelName: "instance", UniqueValues: 50, Percentage: 50.0},
						},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "missing start",
			queryParams: fmt.Sprintf("end=%d&limit=10", endTS),
			wantStatus:  http.StatusBadRequest,
			wantErrType: errBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &mockCardinalityService{
				getLabelsCardinalityFunc: func(_ context.Context, _, _ time.Time, _ int, _ []*cardinality.Matcher) (*cardinality.LabelsCardinalityPerDayResult, error) {
					if tt.mockErr != nil {
						return nil, tt.mockErr
					}
					return tt.mockResult, nil
				},
			}

			r := route.New()
			api := r.WithPrefix("/api/v1")
			RegisterCardinalityV1(api, svc)

			req := httptest.NewRequest(http.MethodGet, "/api/v1/cardinality/labels?"+tt.queryParams, nil)
			rec := httptest.NewRecorder()

			r.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp apiResponse
			err := json.Unmarshal(rec.Body.Bytes(), &resp)
			require.NoError(t, err)

			if tt.wantErrType != "" {
				assert.Equal(t, statusError, resp.Status)
				assert.Equal(t, tt.wantErrType, resp.ErrorType)
			} else {
				assert.Equal(t, statusSuccess, resp.Status)
				assert.NotNil(t, resp.Data)
			}
		})
	}
}
