// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/thanos-io/objstore"
)

// TODO: timeouts retries whatnot
type bucketReaderAt struct {
	bkt  objstore.Bucket
	name string

	timeout time.Duration
}

func newBucketReaderAt(bkt objstore.Bucket, name string, timeout time.Duration) *bucketReaderAt {
	return &bucketReaderAt{
		bkt:     bkt,
		name:    name,
		timeout: timeout,
	}
}

func (br *bucketReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), br.timeout)
	defer cancel()

	bucketRequests.Inc()

	rdc, err := br.bkt.GetRange(ctx, br.name, off, int64(len(p)))
	if err != nil {
		return 0, fmt.Errorf("unable to read range for %s: %w", br.name, err)
	}
	defer rdc.Close()

	return io.ReadFull(rdc, p)
}
