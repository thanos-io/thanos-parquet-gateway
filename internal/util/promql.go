// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import (
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// InjectResultMetrics adds series and sample count attributes to span for a PromQL query result.
func InjectResultMetrics(span trace.Span, value parser.Value) {
	if !span.IsRecording() {
		return
	}
	var seriesCount, floatSampleCount, histogramSampleCount int64
	switch results := value.(type) {
	case promql.Vector:
		seriesCount = int64(len(results))
		for _, s := range results {
			if s.H != nil {
				histogramSampleCount++
			} else {
				floatSampleCount++
			}
		}
	case promql.Matrix:
		seriesCount = int64(len(results))
		for _, series := range results {
			floatSampleCount += int64(len(series.Floats))
			histogramSampleCount += int64(len(series.Histograms))
		}
	case promql.Scalar:
		seriesCount = 1
		floatSampleCount = 1
	}
	span.SetAttributes(
		attribute.Int64("result.series", seriesCount),
		attribute.Int64("result.total_samples", floatSampleCount+histogramSampleCount),
		attribute.Int64("result.float_samples", floatSampleCount),
		attribute.Int64("result.histogram_samples", histogramSampleCount),
	)
}
