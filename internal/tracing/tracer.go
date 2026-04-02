// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Tracer returns a tracer from the global OTel TracerProvider. The result is
// intentionally not cached because the global provider is set after init-time
// (in SetupTracingFromParsedConfig), so caching would risk capturing the noop
// provider if Tracer() were called before tracing is configured.
func Tracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer("parquet-gateway")
}

func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}
