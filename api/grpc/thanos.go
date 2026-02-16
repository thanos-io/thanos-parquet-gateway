// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package grpc

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/alecthomas/units"
	"github.com/efficientgo/core/errcapture"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/stats"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"

	"github.com/thanos-io/thanos-parquet-gateway/db"
	"github.com/thanos-io/thanos-parquet-gateway/internal/limits"
	"github.com/thanos-io/thanos-parquet-gateway/internal/warnings"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

// Taken from https://github.com/thanos-community/thanos-promql-connector/blob/main/main.go

type queryGRPCConfig struct {
	concurrentQuerySemaphore           *limits.Semaphore
	selectChunkBytesQuota              units.Base2Bytes
	selectRowCountQuota                int64
	selectChunkPartitionMaxRange       units.Base2Bytes
	selectChunkPartitionMaxGap         units.Base2Bytes
	selectChunkPartitionMaxConcurrency int
	labelValuesRowCountQuota           int64
	labelNamesRowCountQuota            int64
	shardCountQuota                    int64
}

type QueryGRPCOption func(*queryGRPCConfig)

func ConcurrentQueryQuota(n int) QueryGRPCOption {
	return func(qapi *queryGRPCConfig) {
		qapi.concurrentQuerySemaphore = limits.NewSempahore(n)
	}
}

func SelectChunkBytesQuota(q units.Base2Bytes) QueryGRPCOption {
	return func(qapi *queryGRPCConfig) {
		qapi.selectChunkBytesQuota = q
	}
}

func SelectRowCountQuota(n int64) QueryGRPCOption {
	return func(qapi *queryGRPCConfig) {
		qapi.selectRowCountQuota = n
	}
}

func SelectChunkPartitionMaxRange(v units.Base2Bytes) QueryGRPCOption {
	return func(qapi *queryGRPCConfig) {
		qapi.selectChunkPartitionMaxRange = v
	}
}

func SelectChunkPartitionMaxGap(v units.Base2Bytes) QueryGRPCOption {
	return func(qapi *queryGRPCConfig) {
		qapi.selectChunkPartitionMaxGap = v
	}
}

func SelectChunkPartitionMaxConcurrency(n int) QueryGRPCOption {
	return func(qapi *queryGRPCConfig) {
		qapi.selectChunkPartitionMaxConcurrency = n
	}
}

func LabelValuesRowCountQuota(n int64) QueryGRPCOption {
	return func(qapi *queryGRPCConfig) {
		qapi.labelValuesRowCountQuota = n
	}
}

func LabelNamesRowCountQuota(n int64) QueryGRPCOption {
	return func(qapi *queryGRPCConfig) {
		qapi.labelNamesRowCountQuota = n
	}
}

func ShardCountQuota(n int64) QueryGRPCOption {
	return func(qapi *queryGRPCConfig) {
		qapi.shardCountQuota = n
	}
}

type parquetDatabase interface {
	Timerange() (int64, int64)
	BlockStreams() map[schema.ExternalLabelsHash]db.BlockInfo
	OverrideExtLabels() labels.Labels
	Queryable(options ...db.QueryableOption) *db.DBQueryable
}

type QueryServer struct {
	querypb.UnimplementedQueryServer
	infopb.UnimplementedInfoServer
	storepb.UnimplementedStoreServer

	db     parquetDatabase
	engine promql.QueryEngine

	concurrentQuerySemaphore           *limits.Semaphore
	selectChunkBytesQuota              units.Base2Bytes
	selectRowCountQuota                int64
	selectChunkPartitionMaxRange       units.Base2Bytes
	selectChunkPartitionMaxGap         units.Base2Bytes
	selectChunkPartitionMaxConcurrency int
	labelValuesRowCountQuota           int64
	labelNamesRowCountQuota            int64
	shardCountQuota                    int64
}

func (qs *QueryServer) queryable(replicaLabels ...string) *db.DBQueryable {
	return qs.db.Queryable(
		db.DropReplicaLabels(replicaLabels...),
		db.SelectChunkBytesQuota(qs.selectChunkBytesQuota),
		db.SelectRowCountQuota(qs.selectRowCountQuota),
		db.SelectChunkPartitionMaxRange(qs.selectChunkPartitionMaxRange),
		db.SelectChunkPartitionMaxGap(qs.selectChunkPartitionMaxGap),
		db.SelectChunkPartitionMaxConcurrency(qs.selectChunkPartitionMaxConcurrency),
		db.LabelValuesRowCountQuota(qs.labelValuesRowCountQuota),
		db.LabelNamesRowCountQuota(qs.labelNamesRowCountQuota),
		db.ShardCountQuota(qs.shardCountQuota),
	)
}

func NewQueryServer(db *db.DB, engine promql.QueryEngine, opts ...QueryGRPCOption) *QueryServer {
	cfg := queryGRPCConfig{concurrentQuerySemaphore: limits.UnlimitedSemaphore()}
	for i := range opts {
		opts[i](&cfg)
	}
	return &QueryServer{
		db:                                 db,
		engine:                             engine,
		selectChunkBytesQuota:              cfg.selectChunkBytesQuota,
		selectRowCountQuota:                cfg.selectRowCountQuota,
		concurrentQuerySemaphore:           cfg.concurrentQuerySemaphore,
		selectChunkPartitionMaxRange:       cfg.selectChunkPartitionMaxRange,
		selectChunkPartitionMaxGap:         cfg.selectChunkPartitionMaxGap,
		selectChunkPartitionMaxConcurrency: cfg.selectChunkPartitionMaxConcurrency,
		labelValuesRowCountQuota:           cfg.labelValuesRowCountQuota,
		labelNamesRowCountQuota:            cfg.labelNamesRowCountQuota,
		shardCountQuota:                    cfg.shardCountQuota,
	}
}

func (qs *QueryServer) Info(_ context.Context, _ *infopb.InfoRequest) (*infopb.InfoResponse, error) {
	overrideExtLabels := qs.db.OverrideExtLabels()
	if overrideExtLabels.Len() > 0 {
		extLabels := overrideExtLabels
		mint, maxt := qs.db.Timerange()

		return &infopb.InfoResponse{
			ComponentType: component.Query.String(),
			LabelSets:     zLabelSetsFromPromLabels(extLabels),
			Store: &infopb.StoreInfo{
				MinTime:                      mint,
				MaxTime:                      maxt,
				SupportsWithoutReplicaLabels: true,
				TsdbInfos: []infopb.TSDBInfo{
					{
						MinTime: mint,
						MaxTime: maxt,
						Labels:  labelpb.ZLabelSet{Labels: zLabelsFromMetric(overrideExtLabels)},
					},
				},
			},
			Query: &infopb.QueryAPIInfo{},
		}, nil
	}

	blockStreams := qs.db.BlockStreams()

	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)

	tsdbInfos := make([]infopb.TSDBInfo, 0, len(blockStreams))
	allLabelSets := make([]labels.Labels, 0, len(blockStreams))

	streamHashes := make([]uint64, 0, len(blockStreams))
	for h := range blockStreams {
		streamHashes = append(streamHashes, uint64(h))
	}
	slices.Sort(streamHashes)

	for _, sth := range streamHashes {
		sti := blockStreams[schema.ExternalLabelsHash(sth)]

		mint = min(mint, sti.MinT)
		maxt = max(maxt, sti.MaxT)

		lbls := labels.FromMap(sti.Labels)
		allLabelSets = append(allLabelSets, lbls)

		tsdbInfos = append(tsdbInfos, infopb.TSDBInfo{
			MinTime: sti.MinT,
			MaxTime: sti.MaxT,
			Labels:  labelpb.ZLabelSet{Labels: zLabelsFromMetric(lbls)},
		})
	}

	return &infopb.InfoResponse{
		ComponentType: component.Query.String(),
		LabelSets:     zLabelSetsFromPromLabels(allLabelSets...),
		Store: &infopb.StoreInfo{
			MinTime:   mint,
			MaxTime:   maxt,
			TsdbInfos: tsdbInfos,
		},
		Query: &infopb.QueryAPIInfo{},
	}, nil
}

func (qs *QueryServer) Query(req *querypb.QueryRequest, srv querypb.Query_QueryServer) error {
	ts := time.Unix(req.TimeSeconds, 0)
	timeout := time.Duration(req.TimeoutSeconds) * time.Second

	ctx, cancel := context.WithTimeout(srv.Context(), timeout)
	defer cancel()

	if err := qs.concurrentQuerySemaphore.Reserve(ctx); err != nil {
		return status.Error(codes.Aborted, fmt.Sprintf("semaphore blocked: %s", err))
	}
	defer qs.concurrentQuerySemaphore.Release()

	opts := promql.NewPrometheusQueryOpts(false, time.Duration(req.LookbackDeltaSeconds))

	qryable := qs.queryable(req.ReplicaLabels...)

	qry, err := qs.engine.NewInstantQuery(ctx, qryable, opts, req.Query, ts)
	if err != nil {
		return status.Error(codes.Aborted, fmt.Sprintf("unable to create query: %s", err))
	}
	defer qry.Close()

	res := qry.Exec(ctx)
	if err := res.Err; err != nil {
		if limits.IsResourceExhausted(err) {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		return status.Error(codes.Internal, fmt.Sprintf("query eval error: %s", err))
	}
	if warnings := res.Warnings.AsErrors(); len(warnings) > 0 {
		errs := make([]error, 0, len(warnings))
		errs = append(errs, warnings...)
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

func (qs *QueryServer) QueryRange(req *querypb.QueryRangeRequest, srv querypb.Query_QueryRangeServer) error {
	start := time.Unix(req.StartTimeSeconds, 0)
	end := time.Unix(req.EndTimeSeconds, 0)
	step := time.Duration(req.IntervalSeconds) * time.Second
	timeout := time.Duration(req.TimeoutSeconds) * time.Second

	ctx, cancel := context.WithTimeout(srv.Context(), timeout)
	defer cancel()

	if err := qs.concurrentQuerySemaphore.Reserve(ctx); err != nil {
		return status.Error(codes.Aborted, fmt.Sprintf("semaphore blocked: %s", err))
	}
	defer qs.concurrentQuerySemaphore.Release()

	qryable := qs.queryable(req.ReplicaLabels...)

	opts := promql.NewPrometheusQueryOpts(false, time.Duration(req.LookbackDeltaSeconds))
	qry, err := qs.engine.NewRangeQuery(ctx, qryable, opts, req.Query, start, end, step)
	if err != nil {
		return status.Error(codes.Aborted, fmt.Sprintf("unable to create query: %s", err))
	}
	defer qry.Close()

	res := qry.Exec(ctx)
	if err := res.Err; err != nil {
		if limits.IsResourceExhausted(err) {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		return status.Error(codes.Internal, fmt.Sprintf("query eval error: %s", err))
	}
	if warnings := res.Warnings.AsErrors(); len(warnings) > 0 {
		errs := make([]error, 0, len(warnings))
		errs = append(errs, warnings...)
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

func (qs *QueryServer) Series(request *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (rerr error) {
	qryable := qs.queryable(request.WithoutReplicaLabels...)

	ms, err := storepb.MatchersToPromMatchers(request.Matchers...)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	hints := &storage.SelectHints{
		Start: request.MinTime,
		End:   request.MaxTime,
		Limit: int(request.Limit),
	}
	if request.SkipChunks {
		hints.Func = "series"
	}

	cq, err := qryable.ChunkQuerier(request.MinTime, request.MaxTime)
	if err != nil {
		if limits.IsResourceExhausted(err) {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		return status.Error(codes.Internal, err.Error())
	}
	defer errcapture.Do(&rerr, cq.Close, "chunk querier close")

	css := cq.Select(srv.Context(), true, hints, ms...)

	var (
		i  = int64(0)
		it chunks.Iterator
	)
	for css.Next() {
		i++

		series := css.At()

		if request.Limit > 0 && i > request.Limit {
			if err := srv.Send(storepb.NewWarnSeriesResponse(warnings.ErrorTruncatedResponse)); err != nil {
				return status.Error(codes.Aborted, err.Error())
			}
			break
		}

		storeSeries := storepb.Series{Labels: zLabelsFromMetric(series.Labels())}

		it = series.Iterator(it)
		for it.Next() {
			chk := it.At()
			if chk.Chunk == nil {
				continue
			}
			storeSeries.Chunks = append(storeSeries.Chunks, storepb.AggrChunk{
				MinTime: chk.MinTime,
				MaxTime: chk.MaxTime,
				Raw: &storepb.Chunk{
					Type: chunkEncToStoreEnc(chk.Chunk.Encoding()),
					Data: chk.Chunk.Bytes(),
				},
			})
		}
		if err := it.Err(); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if err := srv.Send(storepb.NewSeriesResponse(&storeSeries)); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}

	if err := css.Err(); err != nil {
		if limits.IsResourceExhausted(err) {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		return status.Error(codes.Internal, err.Error())
	}
	for _, w := range css.Warnings() {
		if err := srv.Send(storepb.NewWarnSeriesResponse(w)); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}

	return nil
}

// chunkEncToStoreEnc converts Prometheus chunk encoding to Thanos store chunk encoding.
func chunkEncToStoreEnc(enc chunkenc.Encoding) storepb.Chunk_Encoding {
	switch enc {
	case chunkenc.EncXOR:
		return storepb.Chunk_XOR
	case chunkenc.EncHistogram:
		return storepb.Chunk_HISTOGRAM
	case chunkenc.EncFloatHistogram:
		return storepb.Chunk_FLOAT_HISTOGRAM
	default:
		panic("unknown chunk encoding")
	}
}

func (qs *QueryServer) LabelNames(ctx context.Context, request *storepb.LabelNamesRequest) (_ *storepb.LabelNamesResponse, rerr error) {
	q, err := qs.queryable(request.WithoutReplicaLabels...).Querier(request.Start, request.End)
	if err != nil {
		if limits.IsResourceExhausted(err) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer errcapture.Do(&rerr, q.Close, "querier close")

	ms, err := storepb.MatchersToPromMatchers(request.Matchers...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	lns, warnings, err := q.LabelNames(ctx, &storage.LabelHints{Limit: int(request.Limit)}, ms...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &storepb.LabelNamesResponse{
		Names:    lns,
		Warnings: warningsAsStrings(warnings),
	}, nil
}

func (qs *QueryServer) LabelValues(ctx context.Context, request *storepb.LabelValuesRequest) (_ *storepb.LabelValuesResponse, rerr error) {
	q, err := qs.queryable(request.WithoutReplicaLabels...).Querier(request.Start, request.End)
	if err != nil {
		if limits.IsResourceExhausted(err) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer errcapture.Do(&rerr, q.Close, "querier close")

	ms, err := storepb.MatchersToPromMatchers(request.Matchers...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	lns, warnings, err := q.LabelValues(ctx, request.Label, &storage.LabelHints{Limit: int(request.Limit)}, ms...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &storepb.LabelValuesResponse{
		Values:   lns,
		Warnings: warningsAsStrings(warnings),
	}, nil
}

func zLabelsFromMetric(metric labels.Labels) []labelpb.ZLabel {
	zlabels := make([]labelpb.ZLabel, 0, metric.Len())
	metric.Range(func(lbl labels.Label) {
		zlabel := labelpb.ZLabel{Name: lbl.Name, Value: lbl.Value}
		zlabels = append(zlabels, zlabel)
	})
	return zlabels
}

func warningsAsStrings(warns annotations.Annotations) []string {
	errs := warns.AsErrors()
	res := make([]string, len(errs))
	for i := range errs {
		res[i] = errs[i].Error()
	}
	return res
}

func zLabelSetsFromPromLabels(lss ...labels.Labels) []labelpb.ZLabelSet {
	sets := make([]labelpb.ZLabelSet, 0, len(lss))
	for _, ls := range lss {
		set := labelpb.ZLabelSet{
			Labels: make([]labelpb.ZLabel, 0, ls.Len()),
		}
		ls.Range(func(lbl labels.Label) {
			set.Labels = append(set.Labels, labelpb.ZLabel{
				Name:  lbl.Name,
				Value: lbl.Value,
			})
		})
		sets = append(sets, set)
	}

	return sets
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
