// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/stats"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"

	cfdb "github.com/cloudflare/parquet-tsdb-poc/db"
)

// Taken from https://github.com/thanos-community/thanos-promql-connector/blob/main/main.go

type infoServer struct {
	infopb.UnimplementedInfoServer

	db *cfdb.DB
}

func NewInfoServer(db *cfdb.DB) infopb.InfoServer {
	return &infoServer{db: db}
}

func (info *infoServer) Info(_ context.Context, _ *infopb.InfoRequest) (*infopb.InfoResponse, error) {
	mint, maxt := info.db.Timerange()
	extlabels := info.db.Extlabels()
	return &infopb.InfoResponse{
		ComponentType: component.Query.String(),
		LabelSets:     labelpb.ZLabelSetsFromPromLabels(extlabels),
		Store: &infopb.StoreInfo{
			MinTime: mint,
			MaxTime: maxt,
			TsdbInfos: []infopb.TSDBInfo{
				{
					MinTime: mint,
					MaxTime: maxt,
					Labels:  labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(extlabels)},
				},
			},
		},
		Query: &infopb.QueryAPIInfo{},
	}, nil
}

type queryServer struct {
	querypb.UnimplementedQueryServer

	db     *cfdb.DB
	engine promql.QueryEngine
}

func NewQueryServer(db *cfdb.DB, engine promql.QueryEngine) querypb.QueryServer {
	return &queryServer{db: db, engine: engine}
}

func (qs *queryServer) Query(req *querypb.QueryRequest, srv querypb.Query_QueryServer) error {
	ts := time.Unix(req.TimeSeconds, 0)
	timeout := time.Duration(req.TimeoutSeconds) * time.Second

	ctx, cancel := context.WithTimeout(srv.Context(), timeout)
	defer cancel()

	opts := promql.NewPrometheusQueryOpts(false, time.Duration(req.LookbackDeltaSeconds))

	qryable := qs.db.ReplicaQueryable(req.ReplicaLabels)

	qry, err := qs.engine.NewInstantQuery(ctx, qryable, opts, req.Query, ts)
	if err != nil {
		return status.Error(codes.Aborted, fmt.Sprintf("unable to create query: %s", err))
	}
	defer qry.Close()

	res := qry.Exec(ctx)
	if err := res.Err; err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("query eval error: %s", err))
	}
	if warnings := res.Warnings.AsErrors(); len(warnings) > 0 {
		errs := make([]error, 0, len(warnings))
		for _, warning := range warnings {
			errs = append(errs, warning)
		}
		if err = srv.SendMsg(querypb.NewQueryWarningsResponse(errs...)); err != nil {
			return err
		}
	}
	switch results := res.Value.(type) {
	case promql.Vector:
		for _, result := range results {
			series := &prompb.TimeSeries{
				Samples: []prompb.Sample{{Value: float64(result.F), Timestamp: int64(result.T)}},
				Labels:  zLabelsFromMetric(result.Metric),
			}
			if err := srv.Send(querypb.NewQueryResponse(series)); err != nil {
				return err
			}
		}
	case promql.Scalar:
		series := &prompb.TimeSeries{Samples: []prompb.Sample{{Value: float64(results.V), Timestamp: int64(results.T)}}}
		if err := srv.Send(querypb.NewQueryResponse(series)); err != nil {
			return err
		}
	}
	if stats := qry.Stats(); stats != nil {
		if err := srv.Send(querypb.NewQueryStatsResponse(toQueryStats(stats))); err != nil {
			return err
		}
	}
	return nil
}

func (qs *queryServer) QueryRange(req *querypb.QueryRangeRequest, srv querypb.Query_QueryRangeServer) error {
	start := time.Unix(req.StartTimeSeconds, 0)
	end := time.Unix(req.EndTimeSeconds, 0)
	step := time.Duration(req.IntervalSeconds) * time.Second
	timeout := time.Duration(req.TimeoutSeconds) * time.Second

	ctx, cancel := context.WithTimeout(srv.Context(), timeout)
	defer cancel()

	qryable := qs.db.ReplicaQueryable(req.ReplicaLabels)

	opts := promql.NewPrometheusQueryOpts(false, time.Duration(req.LookbackDeltaSeconds))
	qry, err := qs.engine.NewRangeQuery(ctx, qryable, opts, req.Query, start, end, step)
	if err != nil {
		return status.Error(codes.Aborted, fmt.Sprintf("unable to create query: %s", err))
	}
	defer qry.Close()

	res := qry.Exec(ctx)
	if err := res.Err; err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("query eval error: %s", err))
	}
	if warnings := res.Warnings.AsErrors(); len(warnings) > 0 {
		errs := make([]error, 0, len(warnings))
		for _, warning := range warnings {
			errs = append(errs, warning)
		}
		if err = srv.SendMsg(querypb.NewQueryWarningsResponse(errs...)); err != nil {
			return err
		}
	}
	switch results := res.Value.(type) {
	case promql.Matrix:
		for _, result := range results {
			series := &prompb.TimeSeries{
				Samples: samplesFromModel(result.Floats),
				Labels:  zLabelsFromMetric(result.Metric),
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}
	case promql.Vector:
		for _, result := range results {
			series := &prompb.TimeSeries{
				Samples: []prompb.Sample{{Value: float64(result.F), Timestamp: int64(result.T)}},
				Labels:  zLabelsFromMetric(result.Metric),
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}
	case promql.Scalar:
		series := &prompb.TimeSeries{Samples: []prompb.Sample{{Value: float64(results.V), Timestamp: int64(results.T)}}}
		if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
			return err
		}
	}

	if stats := qry.Stats(); stats != nil {
		if err := srv.Send(querypb.NewQueryRangeStatsResponse(toQueryStats(stats))); err != nil {
			return err
		}
	}

	return nil
}

func zLabelsFromMetric(metric labels.Labels) []labelpb.ZLabel {
	zlabels := make([]labelpb.ZLabel, 0, metric.Len())
	metric.Range(func(lbl labels.Label) {
		zlabel := labelpb.ZLabel{Name: lbl.Name, Value: lbl.Value}
		zlabels = append(zlabels, zlabel)
	})
	return zlabels
}

func samplesFromModel(samples []promql.FPoint) []prompb.Sample {
	result := make([]prompb.Sample, 0, len(samples))
	for _, s := range samples {
		result = append(result, prompb.Sample{
			Value:     float64(s.F),
			Timestamp: int64(s.T),
		})
	}
	return result
}

func toQueryStats(stats *stats.Statistics) *querypb.QueryStats {
	return &querypb.QueryStats{
		SamplesTotal: stats.Samples.TotalSamples,
		PeakSamples:  int64(stats.Samples.PeakSamples),
	}
}
