// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package http

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/prometheus/common/route"
	"go.opentelemetry.io/otel/attribute"

	"github.com/thanos-io/thanos-parquet-gateway/cardinality"
	"github.com/thanos-io/thanos-parquet-gateway/internal/log"
	"github.com/thanos-io/thanos-parquet-gateway/internal/tracing"
)

type cardinalityAPI struct {
	service cardinality.Service
}

func RegisterCardinalityV1(r *route.Router, service cardinality.Service) {
	api := &cardinalityAPI{
		service: service,
	}

	withInstrumentation(r, "/cardinality/metrics", api.getMetricsCardinality)
	withInstrumentation(r, "/cardinality/labels", api.getLabelsCardinality)
}

type CardinalityRequest struct {
	Start      time.Time
	End        time.Time
	Limit      int
	MetricName string
	Match      []string
	Matchers   []*cardinality.Matcher
}

func (req *CardinalityRequest) Complete(r *http.Request) error {
	q := r.URL.Query()

	req.Limit = cardinality.DefaultLimit
	req.MetricName = q.Get("metric_name")

	var startTS, endTS int64
	startStr := q.Get("start")
	if startStr == "" {
		return validation.NewError("start", "start parameter is required")
	}
	ts, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || ts < 1 {
		return validation.NewError("start", "start must be a valid Unix timestamp")
	}
	startTS = ts

	endStr := q.Get("end")
	if endStr == "" {
		return validation.NewError("end", "end parameter is required")
	}
	ts, err = strconv.ParseInt(endStr, 10, 64)
	if err != nil || ts < 1 {
		return validation.NewError("end", "end must be a valid Unix timestamp")
	}
	endTS = ts

	if limitStr := q.Get("limit"); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil {
			return validation.NewError("limit", "limit must be a valid integer")
		}
		if limit < 0 || limit > cardinality.MaxLimit {
			return validation.NewError("limit", "limit must be between 0 and 1000")
		}
		req.Limit = limit
	}

	req.Match = q["match[]"]

	var allMatchers []*cardinality.Matcher

	if len(req.Match) > 0 {
		matchers, err := cardinality.ParseSelectors(req.Match)
		if err != nil {
			return validation.NewError("match[]", err.Error())
		}
		allMatchers = append(allMatchers, matchers...)
	}

	if req.MetricName != "" {
		hasNameMatcher := false
		for _, m := range allMatchers {
			if m.IsNameMatcher() {
				hasNameMatcher = true
				break
			}
		}
		if !hasNameMatcher {
			allMatchers = append(allMatchers, cardinality.MustNewMatcher(cardinality.MatchEqual, "__name__", req.MetricName))
		}
	}

	req.Matchers = cardinality.DeduplicateMatchers(allMatchers)

	req.Start = startOfDay(time.Unix(startTS, 0).UTC())
	req.End = startOfDay(time.Unix(endTS, 0).UTC())

	if req.Start.After(req.End) {
		return cardinality.ErrInvalidDateRange
	}

	maxRange := time.Duration(cardinality.MaxDateRangeDays) * 24 * time.Hour
	if req.End.Sub(req.Start) > maxRange {
		return cardinality.ErrDateRangeTooLarge
	}

	return nil
}

func startOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func (api *cardinalityAPI) getMetricsCardinality(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := tracing.SpanFromContext(ctx)
	l := log.Ctx(ctx)

	var req CardinalityRequest
	if err := req.Complete(r); err != nil {
		l.Error("Failed", "err", err)
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: err})
		return
	}

	span.SetAttributes(attribute.Int64("cardinality.start", req.Start.Unix()))
	span.SetAttributes(attribute.Int64("cardinality.end", req.End.Unix()))
	span.SetAttributes(attribute.Int("cardinality.limit", req.Limit))
	span.SetAttributes(attribute.Int("cardinality.matchers", len(req.Matchers)))

	result, err := api.service.GetMetricsCardinality(ctx, req.Start, req.End, req.Limit, req.Matchers)
	if err != nil {
		l.Error("Failed", "err", err)
		if errors.Is(err, cardinality.ErrNoDataAvailable) {
			writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: err})
			return
		}
		writeErrorResponse(w, errorResponse{Typ: errInternal, Err: err})
		return
	}

	writeCardinalityResponse(w, r, result)
}

func (api *cardinalityAPI) getLabelsCardinality(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := tracing.SpanFromContext(ctx)
	l := log.Ctx(ctx)

	var req CardinalityRequest
	if err := req.Complete(r); err != nil {
		l.Error("Failed", "err", err)
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: err})
		return
	}

	span.SetAttributes(attribute.Int64("cardinality.start", req.Start.Unix()))
	span.SetAttributes(attribute.Int64("cardinality.end", req.End.Unix()))
	span.SetAttributes(attribute.Int("cardinality.limit", req.Limit))
	span.SetAttributes(attribute.Int("cardinality.matchers", len(req.Matchers)))

	result, err := api.service.GetLabelsCardinality(ctx, req.Start, req.End, req.Limit, req.Matchers)
	if err != nil {
		l.Error("Failed", "err", err)
		if errors.Is(err, cardinality.ErrNoDataAvailable) {
			writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: err})
			return
		}
		writeErrorResponse(w, errorResponse{Typ: errInternal, Err: err})
		return
	}

	writeCardinalityResponse(w, r, result)
}

func writeCardinalityResponse(w http.ResponseWriter, _ *http.Request, data any) {
	w.WriteHeader(http.StatusOK)
	encoder(w).Encode(apiResponse{
		Status: statusSuccess,
		Data:   data,
	})
}
