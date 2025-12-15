// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos-parquet-gateway/cardinality/testutil"
)

var errObjectNotFound = errors.New("object not found")

// mockBucket implements objstore.Bucket for testing.
type mockBucket struct {
	objstore.Bucket
	files       map[string][]byte
	getCount    atomic.Int32
	existsCount atomic.Int32
	iterCount   atomic.Int32
}

func newMockBucket() *mockBucket {
	return &mockBucket{
		files: make(map[string][]byte),
	}
}

func (m *mockBucket) Get(_ context.Context, name string) (io.ReadCloser, error) {
	m.getCount.Add(1)
	data, ok := m.files[name]
	if !ok {
		return nil, errObjectNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockBucket) Exists(_ context.Context, name string) (bool, error) {
	m.existsCount.Add(1)
	_, ok := m.files[name]
	return ok, nil
}

func (m *mockBucket) Iter(_ context.Context, dir string, f func(string) error, _ ...objstore.IterOption) error {
	m.iterCount.Add(1)
	for name := range m.files {
		if len(dir) == 0 || (len(name) > len(dir) && name[:len(dir)] == dir) {
			if err := f(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestCache_GetExisting(t *testing.T) {
	tests := []struct {
		name       string
		remotePath string
		content    []byte
	}{
		{
			name:       "simple file",
			remotePath: "2025/12/01/0.labels.parquet",
			content:    []byte("test content"),
		},
		{
			name:       "nested path",
			remotePath: "2025/12/01/parts/00-08/0.labels.parquet",
			content:    []byte("partition content"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			tmpDir := t.TempDir()
			bucket := newMockBucket()
			bucket.files[tt.remotePath] = tt.content

			cache := NewFileCache(tmpDir, bucket)

			// Execute - first call should download
			localPath, err := cache.Get(context.Background(), tt.remotePath)
			require.NoError(t, err)

			// Verify file was downloaded
			got, err := os.ReadFile(localPath)
			require.NoError(t, err)
			assert.Equal(t, tt.content, got)

			// Second call should use cache (no additional download)
			getCountBefore := bucket.getCount.Load()
			localPath2, err := cache.Get(context.Background(), tt.remotePath)
			require.NoError(t, err)
			assert.Equal(t, localPath, localPath2)
			assert.Equal(t, getCountBefore, bucket.getCount.Load(), "should not download again")
		})
	}
}

func TestCache_ConcurrentDownloads(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	bucket := newMockBucket()
	remotePath := "2025/12/01/0.labels.parquet"
	bucket.files[remotePath] = []byte("test content")

	cache := NewFileCache(tmpDir, bucket)

	// Execute - multiple goroutines request same file
	var wg sync.WaitGroup
	results := make([]string, 10)
	errors := make([]error, 10)

	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			path, err := cache.Get(context.Background(), remotePath)
			results[idx] = path
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// Assert - all should succeed with same path
	for i := range 10 {
		require.NoError(t, errors[i])
		assert.Equal(t, results[0], results[i])
	}

	// Should only download once
	assert.Equal(t, int32(1), bucket.getCount.Load())
}

func TestCache_PathTraversal(t *testing.T) {
	tests := []struct {
		name       string
		remotePath string
		wantErr    bool
	}{
		{
			name:       "valid path",
			remotePath: "2025/12/01/0.labels.parquet",
			wantErr:    false,
		},
		{
			name:       "path traversal attempt",
			remotePath: "../../../etc/passwd",
			wantErr:    true,
		},
		{
			name:       "hidden path traversal",
			remotePath: "2025/12/../../../etc/passwd",
			wantErr:    true,
		},
		{
			name:       "absolute path",
			remotePath: "/etc/passwd",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			bucket := newMockBucket()
			bucket.files[tt.remotePath] = []byte("content")

			cache := NewFileCache(tmpDir, bucket)
			_, err := cache.Get(context.Background(), tt.remotePath)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidPath)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCache_Exists(t *testing.T) {
	tests := []struct {
		name       string
		remotePath string
		setup      bool // whether to pre-create the file
		want       bool
	}{
		{
			name:       "file exists",
			remotePath: "2025/12/01/0.labels.parquet",
			setup:      true,
			want:       true,
		},
		{
			name:       "file does not exist",
			remotePath: "2025/12/01/missing.parquet",
			setup:      false,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			bucket := newMockBucket()

			if tt.setup {
				// Pre-create the cached file
				localPath := filepath.Join(tmpDir, tt.remotePath)
				require.NoError(t, os.MkdirAll(filepath.Dir(localPath), 0755))
				require.NoError(t, os.WriteFile(localPath, []byte("cached"), 0644))
			}

			cache := NewFileCache(tmpDir, bucket)
			got := cache.Exists(tt.remotePath)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCache_Evict(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	bucket := newMockBucket()
	cache := NewFileCache(tmpDir, bucket)

	// Create old and new files
	oldPath := filepath.Join(tmpDir, "old", "file.parquet")
	newPath := filepath.Join(tmpDir, "new", "file.parquet")

	require.NoError(t, os.MkdirAll(filepath.Dir(oldPath), 0755))
	require.NoError(t, os.MkdirAll(filepath.Dir(newPath), 0755))

	require.NoError(t, os.WriteFile(oldPath, []byte("old"), 0644))
	require.NoError(t, os.WriteFile(newPath, []byte("new"), 0644))

	// Set old file's mod time to 2 hours ago
	oldTime := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(oldPath, oldTime, oldTime))

	// Evict files older than 1 hour
	err := cache.Evict(context.Background(), 1*time.Hour)
	require.NoError(t, err)

	// Old file should be removed
	_, err = os.Stat(oldPath)
	assert.True(t, os.IsNotExist(err), "old file should be evicted")

	// New file should remain
	_, err = os.Stat(newPath)
	assert.NoError(t, err, "new file should remain")
}

func TestCache_DownloadFailure(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	bucket := newMockBucket()
	// Don't add file to bucket - will cause download failure

	cache := NewFileCache(tmpDir, bucket)

	// Execute
	_, err := cache.Get(context.Background(), "missing/file.parquet")

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get from bucket")
}

func TestMetricsBuilder_EnsureBuilt(t *testing.T) {
	tests := []struct {
		name   string
		series []testutil.TestSeries
		verify func(t *testing.T, idx *MetricsBuilder, tmpDir, block string)
	}{
		{
			name: "creates_parquet_index",
			series: []testutil.TestSeries{
				{Name: "metric_a", Labels: map[string]string{"env": "prod"}},
				{Name: "metric_a", Labels: map[string]string{"env": "dev"}},
				{Name: "metric_b", Labels: map[string]string{"env": "prod"}},
			},
			verify: func(t *testing.T, idx *MetricsBuilder, tmpDir, block string) {
				ctx := context.Background()

				// Parquet index should not exist yet
				dateStr := DateFromBlock(block)
				parquetPath := filepath.Join(tmpDir, metricsParquetDir, dateStr+".parquet")
				_, err := os.Stat(parquetPath)
				assert.True(t, os.IsNotExist(err), "parquet index should not exist before EnsureIndexed")

				// Call EnsureIndexed
				err = idx.EnsureBuilt(ctx, block)
				require.NoError(t, err)

				// Now parquet index should exist
				_, err = os.Stat(parquetPath)
				assert.NoError(t, err, "parquet index should exist after EnsureIndexed")
			},
		},
		{
			name: "singleflight_deduplication",
			series: []testutil.TestSeries{
				{Name: "metric_a", Labels: map[string]string{"env": "prod"}},
			},
			verify: func(t *testing.T, idx *MetricsBuilder, tmpDir, block string) {
				ctx := context.Background()
				var wg sync.WaitGroup
				errors := make([]error, 10)

				// Launch multiple concurrent EnsureIndexed calls
				for i := range 10 {
					wg.Add(1)
					go func(n int) {
						defer wg.Done()
						errors[n] = idx.EnsureBuilt(ctx, block)
					}(i)
				}
				wg.Wait()

				// All should succeed
				for i := range 10 {
					require.NoError(t, errors[i])
				}

				// Parquet should exist
				dateStr := DateFromBlock(block)
				parquetPath := filepath.Join(tmpDir, metricsParquetDir, dateStr+".parquet")
				_, err := os.Stat(parquetPath)
				assert.NoError(t, err)
			},
		},
		{
			name:   "empty_block",
			series: []testutil.TestSeries{},
			verify: func(t *testing.T, idx *MetricsBuilder, tmpDir, block string) {
				ctx := context.Background()
				err := idx.EnsureBuilt(ctx, block)
				require.NoError(t, err)

				// Parquet should exist even for empty block
				dateStr := DateFromBlock(block)
				parquetPath := filepath.Join(tmpDir, metricsParquetDir, dateStr+".parquet")
				_, err = os.Stat(parquetPath)
				assert.NoError(t, err)
			},
		},
		{
			name: "idempotent",
			series: []testutil.TestSeries{
				{Name: "metric_a", Labels: map[string]string{"env": "prod"}},
			},
			verify: func(t *testing.T, idx *MetricsBuilder, tmpDir, block string) {
				ctx := context.Background()

				// Call multiple times
				for range 5 {
					err := idx.EnsureBuilt(ctx, block)
					require.NoError(t, err)
				}

				// Should still work
				dateStr := DateFromBlock(block)
				parquetPath := filepath.Join(tmpDir, metricsParquetDir, dateStr+".parquet")
				_, err := os.Stat(parquetPath)
				assert.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			require.NoError(t, err)
			defer db.Close()

			tmpDir := t.TempDir()
			idx := NewMetricsBuilder(db, tmpDir)

			block := "2024/12/01" // Historical date (not today)
			testutil.CreateTestLabelsParquet(t, tmpDir, block, tt.series)

			tt.verify(t, idx, tmpDir, block)
		})
	}
}

func TestLabelsBuilder_EnsureBuilt(t *testing.T) {
	tests := []struct {
		name   string
		series []testutil.TestSeries
		verify func(t *testing.T, idx *LabelsBuilder, tmpDir, block string)
	}{
		{
			name: "creates_parquet_index",
			series: []testutil.TestSeries{
				{Name: "metric_a", Labels: map[string]string{"env": "prod", "region": "us-east"}},
				{Name: "metric_a", Labels: map[string]string{"env": "dev", "region": "us-west"}},
				{Name: "metric_b", Labels: map[string]string{"env": "prod", "region": "eu-west"}},
			},
			verify: func(t *testing.T, idx *LabelsBuilder, tmpDir, block string) {
				ctx := context.Background()

				// Parquet index should not exist yet
				dateStr := DateFromBlock(block)
				parquetPath := filepath.Join(tmpDir, labelsParquetDir, dateStr+".parquet")
				_, err := os.Stat(parquetPath)
				assert.True(t, os.IsNotExist(err), "parquet index should not exist before EnsureIndexed")

				// Call EnsureIndexed
				err = idx.EnsureBuilt(ctx, block)
				require.NoError(t, err)

				// Now parquet index should exist
				_, err = os.Stat(parquetPath)
				assert.NoError(t, err, "parquet index should exist after EnsureIndexed")
			},
		},
		{
			name: "singleflight_deduplication",
			series: []testutil.TestSeries{
				{Name: "metric_a", Labels: map[string]string{"env": "prod"}},
			},
			verify: func(t *testing.T, idx *LabelsBuilder, tmpDir, block string) {
				ctx := context.Background()
				var wg sync.WaitGroup
				errors := make([]error, 10)

				for i := range 10 {
					wg.Add(1)
					go func(n int) {
						defer wg.Done()
						errors[n] = idx.EnsureBuilt(ctx, block)
					}(i)
				}
				wg.Wait()

				for i := range 10 {
					require.NoError(t, errors[i])
				}

				dateStr := DateFromBlock(block)
				parquetPath := filepath.Join(tmpDir, labelsParquetDir, dateStr+".parquet")
				_, err := os.Stat(parquetPath)
				assert.NoError(t, err)
			},
		},
		{
			name:   "empty_block",
			series: []testutil.TestSeries{},
			verify: func(t *testing.T, idx *LabelsBuilder, tmpDir, block string) {
				ctx := context.Background()
				err := idx.EnsureBuilt(ctx, block)
				require.NoError(t, err)

				dateStr := DateFromBlock(block)
				parquetPath := filepath.Join(tmpDir, labelsParquetDir, dateStr+".parquet")
				_, err = os.Stat(parquetPath)
				assert.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			require.NoError(t, err)
			defer db.Close()

			tmpDir := t.TempDir()
			idx := NewLabelsBuilder(db, tmpDir)

			block := "2024/12/01"
			testutil.CreateTestLabelsParquet(t, tmpDir, block, tt.series)

			tt.verify(t, idx, tmpDir, block)
		})
	}
}
