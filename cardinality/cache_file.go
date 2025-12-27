// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/thanos-io/objstore"
	"golang.org/x/sync/singleflight"

	"github.com/thanos-io/thanos-parquet-gateway/internal/log"
)

type fileCache struct {
	cacheDir string
	bucket   objstore.Bucket
	sf       singleflight.Group
}

func NewFileCache(cacheDir string, bucket objstore.Bucket) Cache {
	return &loggingCache{&fileCache{
		cacheDir: cacheDir,
		bucket:   bucket,
	}}
}

func (c *fileCache) Get(ctx context.Context, remotePath string) (string, error) {
	if err := c.validatePath(remotePath); err != nil {
		return "", err
	}

	localPath := filepath.Join(c.cacheDir, remotePath)

	if c.Exists(remotePath) {
		return localPath, nil
	}

	_, err, shared := c.sf.Do(remotePath, func() (any, error) {
		return nil, c.download(ctx, remotePath, localPath)
	})

	if err != nil {
		return "", err
	}

	if !shared {
		l := log.Ctx(ctx)
		l.Debug("Downloaded",
			slog.String("remote_path", remotePath),
			slog.String("local_path", localPath))
	}

	return localPath, nil
}

func (c *fileCache) Exists(remotePath string) bool {
	localPath := filepath.Join(c.cacheDir, remotePath)
	_, err := os.Stat(localPath)
	return err == nil
}

func (c *fileCache) Evict(ctx context.Context, maxAge time.Duration) error {
	cutoff := time.Now().Add(-maxAge)

	return filepath.Walk(c.cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if info.IsDir() {
			return nil
		}
		if info.ModTime().Before(cutoff) {
			os.Remove(path)
		}
		return nil
	})
}

func (c *fileCache) CacheDir() string {
	return c.cacheDir
}

func (c *fileCache) validatePath(remotePath string) error {
	cleaned := filepath.Clean(remotePath)
	if strings.Contains(cleaned, "..") {
		return ErrInvalidPath
	}
	if strings.HasPrefix(cleaned, "/") {
		return ErrInvalidPath
	}
	return nil
}

func (c *fileCache) download(ctx context.Context, remotePath, localPath string) error {
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	tmpPath := localPath + ".tmp"
	rc, err := c.bucket.Get(ctx, remotePath)
	if err != nil {
		return fmt.Errorf("get from bucket: %w", err)
	}
	defer rc.Close()

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	if _, err := io.Copy(f, rc); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("copy to cache: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, localPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename to final: %w", err)
	}

	return nil
}

var _ Cache = (*loggingCache)(nil)

type loggingCache struct {
	next Cache
}

func (c *loggingCache) Get(ctx context.Context, remotePath string) (string, error) {
	l := log.Ctx(ctx)

	wasCached := c.next.Exists(remotePath)

	localPath, err := c.next.Get(ctx, remotePath)
	if err != nil {
		l.Error("Cache get failed", "err", err, slog.String("remote_path", remotePath))
		return "", err
	}

	if wasCached {
		l.Debug("Cache hit",
			slog.String("remote_path", remotePath),
			slog.String("local_path", localPath))
	}
	return localPath, nil
}

func (c *loggingCache) Exists(remotePath string) bool {
	return c.next.Exists(remotePath)
}

func (c *loggingCache) Evict(ctx context.Context, maxAge time.Duration) error {
	l := log.Ctx(ctx)

	err := c.next.Evict(ctx, maxAge)
	if err != nil {
		l.Error("Cache eviction failed", "err", err, slog.Duration("max_age", maxAge))
		return err
	}

	l.Debug("Cache eviction completed", slog.Duration("max_age", maxAge))
	return nil
}

func (c *loggingCache) CacheDir() string {
	return c.next.CacheDir()
}
