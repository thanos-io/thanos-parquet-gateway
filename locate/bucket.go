// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"context"
	"fmt"
	"io"

	"github.com/alecthomas/units"
	"github.com/efficientgo/core/errcapture"
	"github.com/thanos-io/objstore"
	"go.opentelemetry.io/otel/attribute"

	"github.com/thanos-io/thanos-parquet-gateway/internal/tracing"
)

type bucketReaderAt struct {
	ctx  context.Context
	bkt  objstore.Bucket
	name string
}

type streamingRangeReader struct {
	ctx       context.Context
	bkt       objstore.Bucket
	name      string
	minOffset int64
	maxOffset int64

	stream       io.ReadCloser
	streamOffset int64
}

func newBucketReaderAt(ctx context.Context, bkt objstore.Bucket, name string) *bucketReaderAt {
	return &bucketReaderAt{
		ctx:  ctx,
		bkt:  bkt,
		name: name,
	}
}

func (br *bucketReaderAt) ReadAt(p []byte, off int64) (_ int, rerr error) {
	ctx, span := tracing.Tracer().Start(br.ctx, "Bucket Get Range")
	defer span.End()

	span.SetAttributes(attribute.String("object", br.name))
	span.SetAttributes(attribute.Stringer("bytes", units.Base2Bytes(len(p)).Round(1)))
	span.SetAttributes(attribute.Int64("offset", off))

	bucketRequests.Inc()

	rdc, err := br.bkt.GetRange(ctx, br.name, off, int64(len(p)))
	if err != nil {
		return 0, fmt.Errorf("unable to read range for %s: %w", br.name, err)
	}
	defer errcapture.Do(&rerr, rdc.Close, "bucket reader close")

	n, err := io.ReadFull(rdc, p)
	if n == len(p) {
		return n, nil
	}
	return n, err
}

func (br *bucketReaderAt) GetStreamingRange(minOffset, maxOffset int64) io.ReaderAt {
	return &streamingRangeReader{
		ctx:       br.ctx,
		bkt:       br.bkt,
		name:      br.name,
		minOffset: minOffset,
		maxOffset: maxOffset,
	}
}

func (s *streamingRangeReader) Close() error {
	if s.stream != nil {
		return s.stream.Close()
	}
	return nil
}

// ReadAt wraps an io.Reader as an io.ReaderAt for sequential only access (offsets must increase).
// parquet-go's PagesFrom requires io.ReaderAt and doesn't support streaming reads directly.
func (s *streamingRangeReader) ReadAt(p []byte, off int64) (int, error) {
	if s.stream == nil {
		rangeSize := s.maxOffset - s.minOffset
		rc, err := s.bkt.GetRange(s.ctx, s.name, s.minOffset, rangeSize)
		if err != nil {
			return 0, fmt.Errorf("unable to open streaming range: %w", err)
		}
		s.stream = rc
		s.streamOffset = s.minOffset
	}

	if off != s.streamOffset {
		panic(fmt.Sprintf("offsets should only be increasing. non-sequential read at offset %d, expected %d", off, s.streamOffset))
	}

	n, err := s.stream.Read(p)
	s.streamOffset += int64(n)
	return n, err
}
