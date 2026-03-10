// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/tracing/jaeger"
	"github.com/thanos-io/thanos/pkg/tracing/otlp"
	"go.opentelemetry.io/otel"
	"gopkg.in/yaml.v2"
)

// TracingProvider defines the type of tracing backend.
type TracingProvider string

const (
	ProviderOTLP   TracingProvider = "OTLP"
	ProviderJaeger TracingProvider = "JAEGER"
)

// TracingConfig is the top-level config structure compatible with Thanos.
type TracingConfig struct {
	Type   TracingProvider    `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

// slogToGoKitAdapter adapts slog.Logger to go-kit log.Logger interface.
type slogToGoKitAdapter struct {
	logger *slog.Logger
}

// Ensure slogToGoKitAdapter implements log.Logger interface.
var _ log.Logger = (*slogToGoKitAdapter)(nil)

// Log implements go-kit log.Logger interface.
func (a slogToGoKitAdapter) Log(keyvals ...any) error {
	// Convert keyvals to slog attributes
	attrs := make([]slog.Attr, 0, len(keyvals)/2)
	for i := 0; i < len(keyvals)-1; i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			continue
		}
		attrs = append(attrs, slog.Any(key, keyvals[i+1]))
	}
	a.logger.LogAttrs(context.Background(), slog.LevelInfo, "", attrs...)
	return nil
}

// SetupTracingFromConfig initializes tracing from a YAML configuration.
func SetupTracingFromConfig(ctx context.Context, logger *slog.Logger, confContentYaml []byte) error {
	if len(confContentYaml) == 0 {
		// No tracing configured
		return nil
	}

	tracingConf := &TracingConfig{}
	if err := yaml.Unmarshal(confContentYaml, tracingConf); err != nil {
		return fmt.Errorf("parsing tracing YAML config: %w", err)
	}

	// Extract the config section as YAML for the provider
	configYaml, err := yaml.Marshal(tracingConf.Config)
	if err != nil {
		return fmt.Errorf("marshaling config section: %w", err)
	}

	switch strings.ToUpper(string(tracingConf.Type)) {
	case string(ProviderOTLP):
		// Use thanos OTLP package to initialize tracing
		goKitLogger := slogToGoKitAdapter{logger: logger}
		tracerProvider, err := otlp.NewTracerProvider(ctx, goKitLogger, configYaml)
		if err != nil {
			return fmt.Errorf("creating OTLP tracer provider: %w", err)
		}
		otel.SetTracerProvider(tracerProvider)
		return nil
	case string(ProviderJaeger):
		// Use thanos jaeger package to initialize tracing
		goKitLogger := slogToGoKitAdapter{logger: logger}
		tracerProvider, err := jaeger.NewTracerProvider(ctx, goKitLogger, configYaml)
		if err != nil {
			return fmt.Errorf("creating Jaeger tracer provider: %w", err)
		}
		otel.SetTracerProvider(tracerProvider)
		return nil
	default:
		return fmt.Errorf("unsupported tracing provider: %s", tracingConf.Type)
	}
}
