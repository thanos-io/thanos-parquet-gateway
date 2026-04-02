// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import (
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// ComputeResultMetrics returns series and sample counts for a PromQL query result.
func ComputeResultMetrics(value parser.Value) (seriesCount, sampleCount int64) {
	switch results := value.(type) {
	case promql.Vector:
		seriesCount = int64(len(results))
		sampleCount = int64(len(results))
	case promql.Matrix:
		seriesCount = int64(len(results))
		for _, series := range results {
			sampleCount += int64(len(series.Floats))
			sampleCount += int64(len(series.Histograms))
		}
	case promql.Scalar:
		seriesCount = 1
		sampleCount = 1
	}
	return
}
