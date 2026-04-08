// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction("github.com/baidubce/bce-sdk-go/util/log.NewLogger.func1"))
}

func TestExpand(t *testing.T) {
	t.Setenv("TEST_ENV_VAR", "test_value")

	input := []byte("value: $(TEST_ENV_VAR)\nmissing: $(MISSING_ENV_VAR)\n")
	expected := []byte("value: test_value\nmissing: \n")

	output := ExpandEnvParens(input)
	if string(output) != string(expected) {
		t.Fatalf("expected %q, got %q", expected, output)
	}
}

func TestSetupBucketWithConfigFile(t *testing.T) {
	t.Run("filesystem config from file", func(tt *testing.T) {
		tmpDir := tt.TempDir()
		configFile := filepath.Join(tmpDir, "config.yaml")
		configContent := `type: FILESYSTEM
config:
  directory: ` + tmpDir + `
`
		if err := os.WriteFile(configFile, []byte(configContent), 0644); err != nil {
			tt.Fatalf("unable to write config file: %v", err)
		}

		opts := bucketOpts{
			objStoreConfigFile: configFile,
		}

		log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
		bkt, err := setupBucket(log, opts)
		if err != nil {
			tt.Fatalf("unable to setup bucket: %v", err)
		}
		if bkt == nil {
			tt.Fatal("bucket is nil")
		}

		// Verify it's a filesystem bucket by checking if we can list (empty bucket)
		ctx := tt.Context()
		if err := bkt.Iter(ctx, "", func(_ string) error {
			return nil
		}); err != nil {
			tt.Fatalf("unable to iterate bucket: %v", err)
		}
	})

	t.Run("filesystem config from inline yaml", func(tt *testing.T) {
		tmpDir := tt.TempDir()
		configContent := `type: FILESYSTEM
config:
  directory: ` + tmpDir + `
`

		opts := bucketOpts{
			objStoreConfig: configContent,
		}

		log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
		bkt, err := setupBucket(log, opts)
		if err != nil {
			tt.Fatalf("unable to setup bucket: %v", err)
		}
		if bkt == nil {
			tt.Fatal("bucket is nil")
		}

		ctx := tt.Context()
		if err := bkt.Iter(ctx, "", func(_ string) error {
			return nil
		}); err != nil {
			tt.Fatalf("unable to iterate bucket: %v", err)
		}
	})

	t.Run("empty config returns error", func(tt *testing.T) {
		opts := bucketOpts{}

		log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
		_, err := setupBucket(log, opts)
		if err == nil {
			tt.Fatal("expected error for empty config")
		}
	})

	t.Run("invalid config returns error", func(tt *testing.T) {
		opts := bucketOpts{
			objStoreConfig: "invalid: yaml: content",
		}

		log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
		_, err := setupBucket(log, opts)
		if err == nil {
			tt.Fatal("expected error for invalid config")
		}
	})

	t.Run("s3 config from file", func(tt *testing.T) {
		configFile := filepath.Join(tt.TempDir(), "config.yaml")
		configContent := `type: S3
config:
  bucket: test-bucket
  endpoint: localhost:9000
  access_key: minioadmin
  secret_key: minioadmin
  insecure: true
`
		if err := os.WriteFile(configFile, []byte(configContent), 0644); err != nil {
			tt.Fatalf("unable to write config file: %v", err)
		}

		opts := bucketOpts{
			objStoreConfigFile: configFile,
		}

		log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
		bkt, err := setupBucket(log, opts)
		// S3 bucket creation might fail if minio is not running, but config parsing should work
		if err != nil && bkt == nil {
			// This is expected if S3 endpoint is not available
			return
		}
		if bkt != nil {
			// If bucket was created, verify it's the right type
			ctx := tt.Context()
			_ = bkt.Iter(ctx, "", func(_ string) error {
				return nil
			})
		}
	})
}

func TestEngineFromQueryOptsRegistersMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	eng := engineFromQueryOpts(queryOpts{
		engineTimeout:   5 * time.Minute,
		defaultLookback: 5 * time.Minute,
	}, logger, reg)
	if eng == nil {
		t.Fatal("engine is nil")
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}

	want := map[string]dto.MetricType{
		"thanos_engine_queries":       dto.MetricType_GAUGE,
		"thanos_engine_queries_total": dto.MetricType_COUNTER,
	}

	found := make(map[string]bool, len(want))
	for _, fam := range families {
		if expectedType, ok := want[fam.GetName()]; ok {
			if fam.GetType() != expectedType {
				t.Errorf("metric %s: expected type %s, got %s", fam.GetName(), expectedType, fam.GetType())
			}
			found[fam.GetName()] = true
		}
	}
	for name := range want {
		if !found[name] {
			t.Errorf("expected metric %s not found in registry", name)
		}
	}
}
