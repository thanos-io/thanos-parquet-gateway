// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cloudflare/parquet-tsdb-poc/internal/tracing"
)

type queryAPI struct {
	engine    promql.QueryEngine
	queryable storage.Queryable

	defaultLookback time.Duration
	defaultStep     time.Duration
	defaultTimeout  time.Duration
}

type QueryAPIOption func(*queryAPI)

func DefaultLookback(d time.Duration) QueryAPIOption {
	return func(qapi *queryAPI) {
		qapi.defaultLookback = d
	}
}

func DefaultStep(s time.Duration) QueryAPIOption {
	return func(qapi *queryAPI) {
		qapi.defaultStep = s
	}
}

func DefaultTimeout(s time.Duration) QueryAPIOption {
	return func(qapi *queryAPI) {
		qapi.defaultStep = s
	}
}

func withTracing(r *route.Router, path string, h http.HandlerFunc) {
	tracedHandler := otelhttp.NewMiddleware(path)(h)

	r.Get(path, tracedHandler.ServeHTTP)
	r.Post(path, tracedHandler.ServeHTTP)
}

func RegisterQueryV1(r *route.Router, queryable storage.Queryable, engine promql.QueryEngine, opts ...QueryAPIOption) {
	qapi := &queryAPI{
		engine:          engine,
		queryable:       queryable,
		defaultLookback: 5 * time.Minute,
		defaultStep:     30 * time.Second,
		defaultTimeout:  30 * time.Second,
	}
	for i := range opts {
		opts[i](qapi)
	}

	withTracing(r, "/query", qapi.query)
	withTracing(r, "/query_range", qapi.queryRange)
	withTracing(r, "/series", qapi.series)
	withTracing(r, "/labels", qapi.labelNames)
	withTracing(r, "/label/:name/values", qapi.labelValues)
}

const (
	errBadRequest    = "bad_request"
	errInternal      = "internal"
	errCanceled      = "canceled"
	errTimeout       = "timeout"
	errUnimplemented = "unimplemented"

	statusSuccess = "success"
	statusError   = "error"
)

type apiResponse struct {
	Status    string   `json:"status"`
	Data      any      `json:"data,omitempty"`
	Warnings  []string `json:"warnings,omitempty"`
	Infos     []string `json:"infos,omitempty"`
	ErrorType string   `json:"errorType,omitempty"`
	Error     string   `json:"error,omitempty"`
}

type errorResponse struct {
	Typ string
	Err error
}

func writeErrorResponse(w http.ResponseWriter, r errorResponse) {
	switch r.Typ {
	case errUnimplemented:
		w.WriteHeader(http.StatusNotFound)
	case errBadRequest:
		w.WriteHeader(http.StatusBadRequest)
	case errInternal:
		w.WriteHeader(http.StatusInternalServerError)
	case errCanceled, errTimeout:
		w.WriteHeader(http.StatusRequestTimeout)
	}
	json.NewEncoder(w).Encode(apiResponse{
		Status:    statusError,
		ErrorType: r.Typ,
		Error:     r.Err.Error(),
	})
}

type queryResponse struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
}

func writeQueryResponse(w http.ResponseWriter, r *promql.Result) {
	w.WriteHeader(http.StatusOK)
	warns, infos := r.Warnings.AsStrings("", 0, 0)
	json.NewEncoder(w).Encode(apiResponse{
		Status: statusSuccess,
		Data: queryResponse{
			ResultType: r.Value.Type(),
			Result:     r.Value,
		},
		Warnings: warns,
		Infos:    infos,
	})
}

func writeSeriesResponse(w http.ResponseWriter, series []labels.Labels, annos annotations.Annotations) {
	w.WriteHeader(http.StatusOK)
	warns, infos := annos.AsStrings("", 0, 0)
	json.NewEncoder(w).Encode(apiResponse{
		Status:   statusSuccess,
		Data:     series,
		Warnings: warns,
		Infos:    infos,
	})
}

func writeLabelsResponse(w http.ResponseWriter, values []string, annos annotations.Annotations) {
	w.WriteHeader(http.StatusOK)
	warns, infos := annos.AsStrings("", 0, 0)
	json.NewEncoder(w).Encode(apiResponse{
		Status:   statusSuccess,
		Data:     values,
		Warnings: warns,
		Infos:    infos,
	})
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseTimeParam(r *http.Request, param string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(param)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time value for '%s': %s", param, err)
	}
	return result, nil
}

func parseDurationParam(r *http.Request, param string, defaultValue time.Duration) (time.Duration, error) {
	val := r.FormValue(param)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("invalid duration value for '%s': %s", param, err)
	}
	return result, nil
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func parseQueryParam(r *http.Request) string {
	return r.FormValue("query")
}

func parseMatchersParam(r *http.Request) ([][]*labels.Matcher, error) {
	matchers := r.Form["match[]"]

	if len(matchers) == 0 {
		return nil, errors.New("no match[] parameter provided")
	}
	matcherSets, err := parser.ParseMetricSelectors(matchers)
	if err != nil {
		return nil, err
	}

OUTER:
	for _, ms := range matcherSets {
		for _, lm := range ms {
			if lm != nil && !lm.Matches("") {
				continue OUTER
			}
		}
		return nil, errors.New("match[] must contain at least one non-empty matcher")
	}
	return matcherSets, nil
}

func parseLimitParam(r *http.Request) (int, error) {
	s := r.FormValue("limit")
	if s == "" {
		return 0, nil
	}

	limit, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %q to a valid limit", s)
	}
	if limit < 0 {
		return 0, errors.New("limit must be non-negative")
	}

	return limit, nil
}

func (qapi *queryAPI) queryOpts() promql.QueryOpts {
	return promql.NewPrometheusQueryOpts(false, qapi.defaultLookback)
}

func (qapi *queryAPI) query(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := tracing.SpanFromContext(ctx)

	if err := r.ParseForm(); err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to parse form data: %s", err)})
		return
	}

	t, err := parseTimeParam(r, "time", time.Now())
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get timestamp: %s", err)})
		return
	}
	timeout, err := parseDurationParam(r, "timeout", qapi.defaultTimeout)
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get timeout: %s", err)})
		return
	}
	q := parseQueryParam(r)

	span.SetAttributes(attribute.String("query.expr", q))
	span.SetAttributes(attribute.String("query.time", t.String()))
	span.SetAttributes(attribute.String("query.timeout", timeout.String()))

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	query, err := qapi.engine.NewInstantQuery(ctx, qapi.queryable, qapi.queryOpts(), q, t)
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errInternal, Err: fmt.Errorf("unable to create query: %s", err)})
		return
	}
	defer query.Close()

	res := query.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			writeErrorResponse(w, errorResponse{Typ: errCanceled, Err: res.Err})
		case promql.ErrQueryTimeout:
			writeErrorResponse(w, errorResponse{Typ: errTimeout, Err: res.Err})
		case promql.ErrStorage:
			writeErrorResponse(w, errorResponse{Typ: errInternal, Err: res.Err})
		default:
			writeErrorResponse(w, errorResponse{Typ: errInternal, Err: res.Err})
		}
		return
	}
	writeQueryResponse(w, res)
}

func (qapi *queryAPI) queryRange(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := tracing.SpanFromContext(ctx)

	if err := r.ParseForm(); err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to parse form data: %s", err)})
		return
	}

	start, err := parseTimeParam(r, "start", time.Now())
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get start: %s", err)})
		return
	}
	end, err := parseTimeParam(r, "end", time.Now())
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get end: %s", err)})
		return
	}
	step, err := parseDurationParam(r, "step", qapi.defaultStep)
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get step: %s", err)})
		return
	}
	timeout, err := parseDurationParam(r, "timeout", qapi.defaultTimeout)
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get timeout: %s", err)})
		return
	}
	q := parseQueryParam(r)

	span.SetAttributes(attribute.String("query.expr", q))
	span.SetAttributes(attribute.String("query.start", start.String()))
	span.SetAttributes(attribute.String("query.end", end.String()))
	span.SetAttributes(attribute.String("query.range", end.Sub(start).String()))
	span.SetAttributes(attribute.String("query.step", step.String()))
	span.SetAttributes(attribute.String("query.timeout", timeout.String()))

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	query, err := qapi.engine.NewRangeQuery(ctx, qapi.queryable, qapi.queryOpts(), q, start, end, step)
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errInternal, Err: fmt.Errorf("unable to create query: %s", err)})
		return
	}
	defer query.Close()

	res := query.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			writeErrorResponse(w, errorResponse{Typ: errCanceled, Err: res.Err})
		case promql.ErrQueryTimeout:
			writeErrorResponse(w, errorResponse{Typ: errTimeout, Err: res.Err})
		case promql.ErrStorage:
			writeErrorResponse(w, errorResponse{Typ: errInternal, Err: res.Err})
		default:
			writeErrorResponse(w, errorResponse{Typ: errInternal, Err: res.Err})
		}
		return
	}
	writeQueryResponse(w, res)
}

func (qapi *queryAPI) series(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := tracing.SpanFromContext(ctx)

	if err := r.ParseForm(); err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to parse form data: %s", err)})
		return
	}

	start, err := parseTimeParam(r, "start", time.Now())
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get start: %s", err)})
		return
	}
	end, err := parseTimeParam(r, "end", time.Now())
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get end: %s", err)})
		return
	}
	limit, err := parseLimitParam(r)
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get limit: %s", err)})
		return
	}
	ms, err := parseMatchersParam(r)
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get labelmatchers: %s", err)})
		return
	}

	span.SetAttributes(attribute.String("series.start", start.String()))
	span.SetAttributes(attribute.String("series.end", end.String()))
	span.SetAttributes(attribute.StringSlice("series.matchers", r.Form["match[]"]))
	span.SetAttributes(attribute.Int("series.limit", limit))

	ctx, cancel := context.WithTimeout(ctx, qapi.defaultTimeout)
	defer cancel()

	q, err := qapi.queryable.Querier(timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errInternal, Err: fmt.Errorf("unable to create querier: %s", err)})
		return
	}
	defer q.Close()

	var (
		series []labels.Labels
		sets   []storage.SeriesSet
	)

	hints := &storage.SelectHints{
		Limit: limit,
		Start: start.UnixMilli(),
		End:   end.UnixMilli(),
		Func:  "series",
	}

	for _, mset := range ms {
		sets = append(sets, q.Select(ctx, false, hints, mset...))
	}

	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	warnings := set.Warnings()
	for set.Next() {
		series = append(series, set.At().Labels())
		if limit > 0 && len(series) > limit {
			series = series[:limit]
			warnings.Add(errors.New("results truncated due to limit"))
			break
		}
	}
	if err := set.Err(); err != nil {
		writeErrorResponse(w, errorResponse{Typ: errInternal, Err: fmt.Errorf("unable to merge series: %s", err)})
		return
	}

	writeSeriesResponse(w, series, warnings)
}

func (qapi *queryAPI) labelValues(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := tracing.SpanFromContext(ctx)

	if err := r.ParseForm(); err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to parse form data: %s", err)})
		return
	}

	name := route.Param(ctx, "name")
	if !model.LabelNameRE.MatchString(name) {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("invalid label name: %q", name)})
	}

	// TODO: support more labels
	if name != model.MetricNameLabel {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: errors.New("label values is only supported for the __name__ label")})
	}

	start, err := parseTimeParam(r, "start", time.Now())
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get start: %s", err)})
		return
	}
	end, err := parseTimeParam(r, "end", time.Now())
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get end: %s", err)})
		return
	}
	limit, err := parseLimitParam(r)
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: fmt.Errorf("unable to get limit: %s", err)})
		return
	}
	if len(r.Form["match[]"]) != 0 {
		writeErrorResponse(w, errorResponse{Typ: errBadRequest, Err: errors.New("label values with matchers is not supported")})
		return
	}

	span.SetAttributes(attribute.String("label_values.start", start.String()))
	span.SetAttributes(attribute.String("label_values.end", end.String()))
	span.SetAttributes(attribute.StringSlice("label_values.matchers", r.Form["match[]"]))
	span.SetAttributes(attribute.Int("label_values.limit", limit))

	ctx, cancel := context.WithTimeout(ctx, qapi.defaultTimeout)
	defer cancel()

	q, err := qapi.queryable.Querier(timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errInternal, Err: fmt.Errorf("unable to create querier: %s", err)})
		return
	}
	defer q.Close()

	hints := &storage.LabelHints{
		Limit: limit,
	}

	labelValues, annos, err := q.LabelValues(ctx, name, hints)
	if err != nil {
		writeErrorResponse(w, errorResponse{Typ: errInternal, Err: fmt.Errorf("unable to query label values: %w", err)})
	}

	writeLabelsResponse(w, labelValues, annos)
}

func (qapi *queryAPI) labelNames(w http.ResponseWriter, _ *http.Request) {
	writeErrorResponse(w, errorResponse{Typ: errUnimplemented, Err: errors.New("unimplemented")})
}
