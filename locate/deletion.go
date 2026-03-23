// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package locate

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"time"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos-parquet-gateway/proto/metapb"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
	"google.golang.org/protobuf/proto"
)

const DeletionMarkerName = "deletion-marker.pb"

type DeletionMarkerFilter struct{}

func (d *DeletionMarkerFilter) ShouldUnload(dateFiles []string) bool {
	return slices.Contains(dateFiles, DeletionMarkerName)
}

const RetentionDurationMessage = "Retention duration expired"

const deleteConsistencyDelay = 12 * time.Hour

func NewRetentionDurationDeleter(bkt objstore.Bucket) *RetentionDurationDeleter {
	return &RetentionDurationDeleter{
		bkt: bkt,
	}
}

func (d *RetentionDurationDeleter) DeleteMarkedStreams(ctx context.Context) error {
	deletePaths := []string{}

	if err := d.bkt.IterWithAttributes(ctx, "", func(attrs objstore.IterObjectAttributes) error {
		date, fn, eh, ok := schema.SplitBlockPath(attrs.Name)
		if !ok {
			return nil
		}
		if fn != DeletionMarkerName {
			return nil
		}

		lm, ok := attrs.LastModified()
		if !ok {
			return fmt.Errorf("object %s does not have a last modified time", attrs.Name)
		}

		if time.Since(lm) < deleteConsistencyDelay {
			return nil
		}

		deletePaths = append(deletePaths, path.Join(eh.String(), date.String()))

		return nil
	}, objstore.WithRecursiveIter(), objstore.WithUpdatedAt()); err != nil {
		return fmt.Errorf("iterating objects: %w", err)
	}

	for _, dp := range deletePaths {
		if err := d.bkt.Delete(ctx, path.Join(dp, schema.MetaFile)); err != nil && !d.bkt.IsObjNotFoundErr(err) {
			return fmt.Errorf("deleting meta.pb: %w", err)
		}

		for i := 0; ; i++ {
			var notFoundErrs = 0

			if err := d.bkt.Delete(ctx, path.Join(dp, fmt.Sprintf("%d.%s", i, "labels.parquet"))); err != nil {
				if d.bkt.IsObjNotFoundErr(err) {
					notFoundErrs++
				} else {
					return fmt.Errorf("deleting shard: %w", err)
				}
			}

			if err := d.bkt.Delete(ctx, path.Join(dp, fmt.Sprintf("%d.%s", i, "chunks.parquet"))); err != nil {
				if d.bkt.IsObjNotFoundErr(err) {
					notFoundErrs++
				} else {
					return fmt.Errorf("deleting shard: %w", err)
				}
			}

			if notFoundErrs == 2 {
				break
			}
		}

		if err := d.bkt.Delete(ctx, path.Join(dp, DeletionMarkerName)); err != nil {
			return fmt.Errorf("deleting deletion marker: %w", err)
		}
	}

	return nil
}

type RetentionDurationDeleter struct {
	bkt objstore.Bucket
}

func NewRetentionDurationMarker(d Discoverable, bkt objstore.Bucket, retentionDuration time.Duration, l *slog.Logger) *RetentionDurationMarker {
	return &RetentionDurationMarker{
		d:                 d,
		bkt:               bkt,
		retentionDuration: retentionDuration,
		l:                 l,
	}
}

type RetentionDurationMarker struct {
	d   Discoverable
	bkt objstore.Bucket

	retentionDuration time.Duration
	lastSeenSyncTime  time.Time

	l *slog.Logger
}

type Discoverable interface {
	Streams() DiscovererStreams
}

func (d *RetentionDurationMarker) MarkExpiredStreams(ctx context.Context) error {
	if d.retentionDuration == 0 {
		return nil
	}
	st := d.d.Streams()

	if st.LastSync.Equal(d.lastSeenSyncTime) || st.LastSync.Before(d.lastSeenSyncTime) {
		return nil
	}

	cutOffDate := time.Now().UTC().Add(-d.retentionDuration)

	for streamHash, stream := range st.Streams {
		for day := range stream.DiscoveredDays {
			if day.ToTime().After(cutOffDate) || day.ToTime().Equal(cutOffDate) {
				continue
			}
			d.l.Info("Marking stream as expired", "stream_hash", streamHash, "date", day.String(), "retention_duration", d.retentionDuration.String())

			dm := &metapb.DeletionMark{
				Reason: RetentionDurationMessage,
			}

			md, err := proto.Marshal(dm)
			if err != nil {
				return fmt.Errorf("marshaling deletion marker: %w", err)
			}

			if err := d.bkt.Upload(ctx, path.Join(streamHash.String(), day.String(), DeletionMarkerName), bytes.NewReader(md)); err != nil {
				return fmt.Errorf("uploading deletion marker: %w", err)
			}
		}
	}

	d.lastSeenSyncTime = st.LastSync

	return nil
}
