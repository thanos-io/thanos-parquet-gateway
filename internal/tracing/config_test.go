// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package tracing

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestSetupTracingFromConfig_Jaeger(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	yamlConfig := []byte(`
type: JAEGER
config:
  service_name: test-service
  sampler_type: const
  sampler_param: 1
`)

	err := SetupTracingFromConfig(ctx, logger, yamlConfig)
	if err != nil {
		t.Fatalf("SetupTracingFromConfig failed: %v", err)
	}

	// Verify that a tracer provider was set
	tracer := otel.GetTracerProvider()
	if tracer == nil {
		t.Fatal("Expected tracer provider to be set, but got nil")
	}
}

func TestSetupTracingFromConfig_Empty(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	yamlConfig := []byte(``)

	err := SetupTracingFromConfig(ctx, logger, yamlConfig)
	if err != nil {
		t.Fatalf("SetupTracingFromConfig failed with empty config: %v", err)
	}
}

func TestSetupTracingFromConfig_InvalidType(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	yamlConfig := []byte(`
type: INVALID_TYPE
config:
  service_name: test-service
`)

	err := SetupTracingFromConfig(ctx, logger, yamlConfig)
	if err == nil {
		t.Fatal("Expected error for invalid tracing type, but got nil")
	}
}
