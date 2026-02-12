// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

// mockBlocks creates test metadata for the given ULIDs
func mockBlocks(ulids ...string) []metadata.Meta {
	metas := make([]metadata.Meta, 0, len(ulids))
	for _, v := range ulids {
		metas = append(metas, metadata.Meta{
			BlockMeta: tsdb.BlockMeta{ULID: ulid.MustParse(v)},
		})
	}
	return metas
}

func TestPlanner(t *testing.T) {
	var (
		defaultExternalLabels = schema.ExternalLabels{"stream": "eu-west-1"}
		defaultHash           = defaultExternalLabels.Hash()
	)

	for _, tc := range []struct {
		name string

		notAfter       time.Time
		maxDays        int
		tsdbStreams    map[schema.ExternalLabelsHash]schema.TSDBBlocksStream
		parquetStreams map[schema.ExternalLabelsHash]schema.ParquetBlocksStream

		expectedPlan Plan
	}{
		{
			name:     "last day only partially covered",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
							MinTime: time.Date(2025, time.March, 2, 11, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Date: util.NewDate(2025, time.Month(3), 2),
						Mint: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			expectedPlan: Plan{Steps: []Step{}},
		},
		{
			name:     "three blocks cover a full day, previous parquet file for initial overlap",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2025, time.March, 2, 11, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DWY7YB7TE9TNHY5NTAYWT"),
								MinTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2025, time.March, 3, 18, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DYXS7CJ7VCFH63WD1S006"),
								MinTime: time.Date(2025, time.March, 3, 18, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2025, time.March, 4, 4, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Date: util.NewDate(2025, time.Month(3), 2),
						Mint: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date: util.NewDate(2025, time.March, 3),
						Sources: mockBlocks(
							"01JT0DPYGA1HPW5RBZ1KBXCNXK",
							"01JT0DWY7YB7TE9TNHY5NTAYWT",
							"01JT0DYXS7CJ7VCFH63WD1S006",
						),
						ExternalLabels: defaultExternalLabels,
					},
				},
			},
		},
		{
			name:     "we dont convert blocks that are too young still",
			notAfter: time.Date(2025, time.March, 1, 0, 0, 0, 0, time.UTC),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
							MinTime: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			expectedPlan: Plan{Steps: []Step{}},
		},
		{
			name:     "we have all blocks already",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2020, time.January, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 24, 18, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2020, time.January, 24, 18, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 25, 12, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Date: util.NewDate(2020, time.January, 4),
						Mint: time.Date(2020, time.January, 4, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:           util.NewDate(2020, time.January, 24),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK", "01JU0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 23),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 22),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 21),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 20),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 19),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 18),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 17),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 16),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 15),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 14),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 13),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 12),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 11),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 10),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 9),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 8),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 7),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 6),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 5),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
				},
			},
		},
		{
			name:     "we can amoritize downloads by converting as many blocks as possible",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
							MinTime: time.Date(2020, time.January, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2020, time.January, 7, 18, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:           util.NewDate(2020, time.January, 6),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 5),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 4),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
				},
			},
		},
		{
			name:     "upload gap does not stall converter",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2020, time.January, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 5, 18, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2020, time.January, 5, 18, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 6, 12, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2020, time.January, 7, 18, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 8, 6, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{
						{
							Date: util.NewDate(2020, time.January, 4),
							Mint: time.Date(2020, time.January, 4, 0, 0, 0, 0, time.UTC).UnixMilli(),
							Maxt: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
						{
							Date: util.NewDate(2020, time.January, 5),
							Mint: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
							Maxt: time.Date(2020, time.January, 6, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
					},
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:           util.NewDate(2020, time.January, 7),
						Sources:        mockBlocks("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 6),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
				},
			},
		},
		{
			name:     "gap that's longer than a day does not stall converter",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2024, time.September, 9, 12, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2024, time.September, 10, 6, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2024, time.September, 24, 4, 45, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2024, time.September, 28, 12, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{
						{
							Date: util.NewDate(2024, time.September, 9),
							Mint: time.Date(2024, time.September, 9, 0, 0, 0, 0, time.UTC).UnixMilli(),
							Maxt: time.Date(2024, time.September, 10, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
						{
							Date: util.NewDate(2024, time.September, 10),
							Mint: time.Date(2024, time.September, 10, 0, 0, 0, 0, time.UTC).UnixMilli(),
							Maxt: time.Date(2024, time.September, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
					},
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:           util.NewDate(2024, time.September, 27),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2024, time.September, 26),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2024, time.September, 25),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2024, time.September, 24),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						ExternalLabels: defaultExternalLabels,
					},
				},
			},
		},
		{
			name:     "tsdb block covering full day and a partial parquet block for it",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXZ"),
							MinTime: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Date: util.NewDate(2020, time.January, 23),
						Mint: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2020, time.January, 23, 12, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			// If we have a full day of tsdb data and a parquet block for that day that covers only
			// part of that day then we do nothing, we don't try to re-convert.
			expectedPlan: Plan{Steps: []Step{}},
		},
		{
			name:     "converted block followed by a partial day block",
			notAfter: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXZ"),
								MinTime: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 24, 4, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Date: util.NewDate(2020, time.January, 23),
						Mint: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			expectedPlan: Plan{Steps: []Step{}},
		},
		{
			name:     "converted block followed by a partial day block, followed by an unconverted block",
			notAfter: time.Date(2020, time.January, 25, 0, 0, 0, 0, time.UTC),
			maxDays:  7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXZ"),
								MinTime: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 24, 18, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2020, time.January, 25, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 25, 2, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Date: util.NewDate(2020, time.January, 23),
						Mint: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			expectedPlan: Plan{Steps: []Step{}}, // We can only convert 01/24 because of notAfter limit, but it's partial so we skip it
		},
		{
			name:     "3 * 2 day blocks, maxDays=3, should convert first two blocks",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  3,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
								MinTime: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
								MinTime: time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
								MinTime: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 7, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:           util.NewDate(2020, time.January, 6),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 5),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 4),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 3),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						ExternalLabels: defaultExternalLabels,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(tt *testing.T) {
			plan := NewPlanner(tc.notAfter, tc.maxDays).Plan(tc.tsdbStreams, tc.parquetStreams)

			if diff := cmp.Diff(tc.expectedPlan, plan,
				cmpopts.IgnoreUnexported(),
				cmpopts.EquateComparable(util.Date{}),
				cmpopts.IgnoreFields(tsdb.BlockMeta{}, "MinTime", "MaxTime"),
			); diff != "" {
				tt.Errorf("plan mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPlanHistorical_Daily(t *testing.T) {
	// Tests for PlanHistorical behavior with stream inputs
	var (
		defaultExternalLabels = schema.ExternalLabels{}
		defaultHash           = defaultExternalLabels.Hash()
	)

	for _, tc := range []struct {
		name string

		notAfter           time.Time
		historicalMaxSteps int
		tsdbStreams        map[schema.ExternalLabelsHash]schema.TSDBBlocksStream
		parquetStreams     map[schema.ExternalLabelsHash]schema.ParquetBlocksStream

		expectedPlan Plan
	}{
		{
			name:               "partial day is truncated for daily blocks",
			notAfter:           time.UnixMilli(math.MaxInt64),
			historicalMaxSteps: 7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
							MinTime: time.Date(2025, time.March, 2, 11, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Name: "2025/03/02",
						Mint: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			// Partial day 03/03 is truncated for daily blocks (use partitions for partial days)
			expectedPlan: Plan{Steps: []Step{}},
		},
		{
			name:               "we dont convert blocks that are too young still",
			notAfter:           time.Date(2025, time.March, 1, 0, 0, 0, 0, time.UTC),
			historicalMaxSteps: 7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
							MinTime: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedPlan:   Plan{Steps: []Step{}},
		},
		{
			name:               "tsdb block covering full day and a partial parquet block for it",
			notAfter:           time.UnixMilli(math.MaxInt64),
			historicalMaxSteps: 7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXZ"),
							MinTime: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Name: "2020/01/23",
						Mint: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2020, time.January, 23, 12, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			// If we have a full day of tsdb data and a parquet block for that day that covers only
			// part of that day then we do nothing, we don't try to re-convert.
			expectedPlan: Plan{Steps: []Step{}},
		},
		{
			name:               "converted block followed by a partial day block",
			notAfter:           time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC),
			historicalMaxSteps: 7,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXZ"),
								MinTime: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
								MinTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 24, 4, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Name: "2020/01/23",
						Mint: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			expectedPlan: Plan{Steps: []Step{}}, // 01/24 is excluded by notAfter
		},
		{
			name:               "historicalMaxSteps limits output",
			notAfter:           time.UnixMilli(math.MaxInt64),
			historicalMaxSteps: 3,
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
								MinTime: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
								MinTime: time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
								MinTime: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2020, time.January, 7, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:           util.NewDate(2020, time.January, 6),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 5),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 4),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						ExternalLabels: defaultExternalLabels,
					},
					{
						Date:           util.NewDate(2020, time.January, 3),
						Sources:        mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						ExternalLabels: defaultExternalLabels,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(tt *testing.T) {
			plan := NewPlannerWithPartitions(tc.notAfter, tc.historicalMaxSteps, 2*time.Hour, 0, DefaultPartitionLookback).PlanHistorical(tc.tsdbStreams, tc.parquetStreams)

			if diff := cmp.Diff(tc.expectedPlan, plan,
				cmpopts.IgnoreUnexported(),
				cmpopts.EquateComparable(util.Date{}),
				cmpopts.IgnoreFields(tsdb.BlockMeta{}, "MinTime", "MaxTime"),
			); diff != "" {
				tt.Errorf("plan mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPlanPartitions(t *testing.T) {
	// Test PlanPartitions creates partitions for days without daily blocks
	// The new design doesn't use "today" from wall clock - it uses notAfter to determine
	// which partitions are safe to convert

	notAfter := time.Date(2025, time.January, 19, 10, 0, 0, 0, time.UTC) // partitions ending before 10:00 are ready

	var (
		defaultExternalLabels = schema.ExternalLabels{}
		defaultHash           = defaultExternalLabels.Hash()
	)

	for _, tc := range []struct {
		name                   string
		tsdbStreams            map[schema.ExternalLabelsHash]schema.TSDBBlocksStream
		parquetStreams         map[schema.ExternalLabelsHash]schema.ParquetBlocksStream
		expectedPartitionCount int
	}{
		{
			name: "block entirely in today - included",
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
							MinTime: time.Date(2025, time.January, 19, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.January, 19, 6, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams:         map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedPartitionCount: 3, // 00-02, 02-04, 04-06
		},
		{
			name: "block spanning midnight - included (touches today and yesterday)",
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
							MinTime: time.Date(2025, time.January, 18, 22, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.January, 19, 4, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams:         map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedPartitionCount: 3, // yesterday 22-24, today 00-02, 02-04
		},
		{
			name: "block with existing daily block - NOT included",
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
							MinTime: time.Date(2025, time.January, 17, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.January, 18, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Name: "2025/01/17",
						Mint: time.Date(2025, time.January, 17, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2025, time.January, 18, 0, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			expectedPartitionCount: 0,
		},
		{
			name:                   "empty inputs - empty plan",
			tsdbStreams:            map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{},
			parquetStreams:         map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedPartitionCount: 0,
		},
		{
			name: "existing partition skipped",
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
							MinTime: time.Date(2025, time.January, 19, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.January, 19, 6, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Name:      "2025/01/19/parts/00-02",
						Partition: &util.Partition{}, // Mark as partition
						Mint:      time.Date(2025, time.January, 19, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt:      time.Date(2025, time.January, 19, 2, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			expectedPartitionCount: 2, // 02-04, 04-06 (00-02 already exists)
		},
	} {
		t.Run(tc.name, func(tt *testing.T) {
			planner := NewPlannerWithPartitions(notAfter, 7, 2*time.Hour, 0, DefaultPartitionLookback)
			plan := planner.PlanPartitions(tc.tsdbStreams, tc.parquetStreams)

			if len(plan.Steps) != tc.expectedPartitionCount {
				tt.Errorf("expected %d partitions, got %d", tc.expectedPartitionCount, len(plan.Steps))
			}

			// Verify all steps are partitions (not daily)
			for _, step := range plan.Steps {
				if step.Partition == nil {
					tt.Errorf("expected partition step, got daily step for %s", step.Date.String())
				}
			}
		})
	}
}

func TestPlanHistorical(t *testing.T) {
	// Test PlanHistorical filters correctly based on notAfter time
	// A day is "historical" when its end time (24:00) is before notAfter

	// notAfter = Jan 19 00:00 means Jan 18 (ending at midnight Jan 19) is historical
	notAfter := time.Date(2025, time.January, 19, 0, 0, 0, 0, time.UTC)

	var (
		defaultExternalLabels = schema.ExternalLabels{}
		defaultHash           = defaultExternalLabels.Hash()
	)

	for _, tc := range []struct {
		name               string
		tsdbStreams        map[schema.ExternalLabelsHash]schema.TSDBBlocksStream
		parquetStreams     map[schema.ExternalLabelsHash]schema.ParquetBlocksStream
		expectedDailyCount int
		expectedDates      []string
	}{
		{
			name: "block entirely in past - included",
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
							MinTime: time.Date(2025, time.January, 17, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.January, 18, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams:     map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedDailyCount: 1,
			expectedDates:      []string{"2025/01/17"},
		},
		{
			name: "block spanning midnight - NOT included (day end equals notAfter)",
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
							MinTime: time.Date(2025, time.January, 18, 22, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.January, 19, 4, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams:     map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedDailyCount: 0, // Jan 18 ends at Jan 19 00:00 which equals notAfter, so NOT historical (Before is strict)
			expectedDates:      []string{},
		},
		{
			name: "block entirely after notAfter - NOT included",
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
							MinTime: time.Date(2025, time.January, 19, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.January, 19, 6, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams:     map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedDailyCount: 0, // Jan 19 ends at Jan 20 00:00 which is after notAfter
			expectedDates:      []string{},
		},
		{
			name:               "empty inputs - empty plan",
			tsdbStreams:        map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{},
			parquetStreams:     map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedDailyCount: 0,
			expectedDates:      []string{},
		},
		{
			name: "multiple historical blocks",
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
								MinTime: time.Date(2025, time.January, 16, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2025, time.January, 17, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
						{
							BlockMeta: tsdb.BlockMeta{
								ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
								MinTime: time.Date(2025, time.January, 17, 0, 0, 0, 0, time.UTC).UnixMilli(),
								MaxTime: time.Date(2025, time.January, 18, 0, 0, 0, 0, time.UTC).UnixMilli(),
							},
						},
					},
				},
			},
			parquetStreams:     map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{},
			expectedDailyCount: 2,
			expectedDates:      []string{"2025/01/17", "2025/01/16"},
		},
		{
			name: "existing daily block skipped",
			tsdbStreams: map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []metadata.Meta{{
						BlockMeta: tsdb.BlockMeta{
							ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
							MinTime: time.Date(2025, time.January, 17, 0, 0, 0, 0, time.UTC).UnixMilli(),
							MaxTime: time.Date(2025, time.January, 18, 0, 0, 0, 0, time.UTC).UnixMilli(),
						},
					}},
				},
			},
			parquetStreams: map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
				defaultHash: {
					StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
					Metas: []schema.Meta{{
						Name: "2025/01/17",
						Mint: time.Date(2025, time.January, 17, 0, 0, 0, 0, time.UTC).UnixMilli(),
						Maxt: time.Date(2025, time.January, 18, 0, 0, 0, 0, time.UTC).UnixMilli(),
					}},
				},
			},
			expectedDailyCount: 0, // Already converted
			expectedDates:      []string{},
		},
	} {
		t.Run(tc.name, func(tt *testing.T) {
			planner := NewPlannerWithPartitions(notAfter, 7, 2*time.Hour, 0, DefaultPartitionLookback)
			plan := planner.PlanHistorical(tc.tsdbStreams, tc.parquetStreams)

			if len(plan.Steps) != tc.expectedDailyCount {
				tt.Errorf("expected %d daily steps, got %d", tc.expectedDailyCount, len(plan.Steps))
			}

			// Verify all steps are daily (not partitions)
			for _, step := range plan.Steps {
				if step.Partition != nil {
					tt.Errorf("expected daily step, got partition step for %s", step.Partition.String())
				}
			}

			// Verify expected dates
			gotDates := make(map[string]bool)
			for _, step := range plan.Steps {
				gotDates[step.Date.String()] = true
			}
			for _, expectedDate := range tc.expectedDates {
				if !gotDates[expectedDate] {
					tt.Errorf("expected date %s not found in plan", expectedDate)
				}
			}
		})
	}
}

// TestPlanPartitions_LatePartitionScenario tests the exact bug scenario:
// It's 3:25 AM on Dec 11, and the 22-24 partition for Dec 10 was never created.
// The new design should still create this partition.
func TestPlanPartitions_LatePartitionScenario(t *testing.T) {
	// Scenario: Current time is Dec 11 03:25 UTC
	// Grace period is 2 hours, so notAfter = Dec 11 01:25
	// Dec 10 22-24 partition ends at Dec 11 00:00, which is BEFORE notAfter
	// So it should be created!

	notAfter := time.Date(2025, time.December, 11, 1, 25, 0, 0, time.UTC) // now - 2h grace

	var (
		defaultExternalLabels = schema.ExternalLabels{}
		defaultHash           = defaultExternalLabels.Hash()
	)

	tsdbStreams := map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
		defaultHash: {
			StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
			Metas: []metadata.Meta{
				// TSDB block covering Dec 10 20:00 - Dec 10 22:00
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
						MinTime: time.Date(2025, time.December, 10, 20, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 10, 22, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				// TSDB block covering Dec 10 22:00 - Dec 11 00:00 (the late arriving block!)
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						MinTime: time.Date(2025, time.December, 10, 22, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
		},
	}

	// No daily block for Dec 10 yet
	parquetStreams := map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{}

	planner := NewPlannerWithPartitions(notAfter, 7, 2*time.Hour, 0, DefaultPartitionLookback)
	plan := planner.PlanPartitions(tsdbStreams, parquetStreams)

	// Should create partitions for Dec 10 that are ready:
	// Dec 10 20-22: ends at 22:00 < notAfter (01:25 Dec 11) ✓
	// Dec 10 22-24: ends at 00:00 Dec 11 < notAfter (01:25 Dec 11) ✓
	// Dec 11 00-02: ends at 02:00 Dec 11 > notAfter (01:25 Dec 11) ✗

	expectedPartitions := map[string]bool{
		"2025/12/10/parts/20-22": true,
		"2025/12/10/parts/22-24": true,
	}

	if len(plan.Steps) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(plan.Steps))
		for _, step := range plan.Steps {
			t.Logf("  got: %s", step.Partition.String())
		}
	}

	for _, step := range plan.Steps {
		if step.Partition == nil {
			t.Errorf("expected partition step, got daily step")
			continue
		}
		name := step.Partition.String()
		if !expectedPartitions[name] {
			t.Errorf("unexpected partition: %s", name)
		}
		delete(expectedPartitions, name)
	}

	for name := range expectedPartitions {
		t.Errorf("missing expected partition: %s", name)
	}
}

// TestPlanPartitions_24hWindow tests that partitions are ONLY created
// for days within the 24h window from notAfter.
// Older days should be handled by daily block conversion instead.
func TestPlanPartitions_24hWindow(t *testing.T) {
	// Scenario: Dec 13, 10:00 UTC
	// notAfter = Dec 13 08:00 (2h grace)
	// partitionCutoff = Dec 12 08:00 (24h before notAfter)
	//
	// Dec 11: entirely before cutoff (ends Dec 12 00:00 < Dec 12 08:00) - NO partitions
	// Dec 12: overlaps window (Dec 12 00:00 to Dec 13 00:00 overlaps [Dec 12 08:00, Dec 13 08:00])
	// Dec 13: within window (partial day)

	notAfter := time.Date(2025, time.December, 13, 8, 0, 0, 0, time.UTC)

	var (
		defaultExternalLabels = schema.ExternalLabels{}
		defaultHash           = defaultExternalLabels.Hash()
	)

	tsdbStreams := map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
		defaultHash: {
			StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
			Metas: []metadata.Meta{
				// Full day Dec 11 (entirely before 24h cutoff)
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
						MinTime: time.Date(2025, time.December, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 12, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				// Full day Dec 12 (partially in window - day ends Dec 13 00:00 > cutoff Dec 12 08:00)
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						MinTime: time.Date(2025, time.December, 12, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 13, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				// Partial day Dec 13 (within window)
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
						MinTime: time.Date(2025, time.December, 13, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 13, 10, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
		},
	}

	parquetStreams := map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{} // No daily blocks

	planner := NewPlannerWithPartitions(notAfter, 7, 2*time.Hour, 0, DefaultPartitionLookback)
	plan := planner.PlanPartitions(tsdbStreams, parquetStreams)

	// Dec 11: 0 partitions (day ends Dec 12 00:00, which is NOT after cutoff Dec 12 08:00)
	// Dec 12: 12 partitions (day ends Dec 13 00:00 > cutoff, all partitions before notAfter)
	// Dec 13: 3 partitions (00-02, 02-04, 04-06 end before 08:00; 06-08 ends AT 08:00 so excluded)

	// Count partitions per day
	dayCounts := make(map[string]int)
	for _, step := range plan.Steps {
		if step.Partition == nil {
			t.Errorf("expected partition step, got daily step")
			continue
		}
		dayCounts[step.Date.String()]++
	}

	if dayCounts["2025/12/11"] != 0 {
		t.Errorf("expected 0 partitions for Dec 11 (before 24h window), got %d", dayCounts["2025/12/11"])
	}
	if dayCounts["2025/12/12"] != 12 {
		t.Errorf("expected 12 partitions for Dec 12 (overlaps 24h window), got %d", dayCounts["2025/12/12"])
	}
	if dayCounts["2025/12/13"] != 3 {
		t.Errorf("expected 3 partitions for Dec 13 (in 24h window, before notAfter), got %d", dayCounts["2025/12/13"])
	}

	expectedTotal := 15 // 0 + 12 + 3
	if len(plan.Steps) != expectedTotal {
		t.Errorf("expected %d total partitions, got %d", expectedTotal, len(plan.Steps))
	}
}

// TestPlanPartitions_DailyBlockPreventsPartitions tests that if a daily block
// exists for a day, no partitions are created for that day.
func TestPlanPartitions_DailyBlockPreventsPartitions(t *testing.T) {
	notAfter := time.Date(2025, time.December, 13, 8, 0, 0, 0, time.UTC)

	var (
		defaultExternalLabels = schema.ExternalLabels{}
		defaultHash           = defaultExternalLabels.Hash()
	)

	tsdbStreams := map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
		defaultHash: {
			StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
			Metas: []metadata.Meta{
				// Full day Dec 11
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
						MinTime: time.Date(2025, time.December, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 12, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				// Full day Dec 12
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						MinTime: time.Date(2025, time.December, 12, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 13, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
		},
	}

	parquetStreams := map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{
		defaultHash: {
			StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
			Metas: []schema.Meta{{
				// Daily block exists for Dec 11 - no partitions should be created for it
				Name: "2025/12/11",
				Mint: time.Date(2025, time.December, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
				Maxt: time.Date(2025, time.December, 12, 0, 0, 0, 0, time.UTC).UnixMilli(),
			}},
		},
	}

	planner := NewPlannerWithPartitions(notAfter, 7, 2*time.Hour, 0, DefaultPartitionLookback)
	plan := planner.PlanPartitions(tsdbStreams, parquetStreams)

	// Only Dec 12 partitions should be created (12 partitions)
	expectedCount := 12
	if len(plan.Steps) != expectedCount {
		t.Errorf("expected %d partitions, got %d", expectedCount, len(plan.Steps))
	}

	// Verify all partitions are for Dec 12
	for _, step := range plan.Steps {
		if step.Partition == nil {
			t.Errorf("expected partition step, got daily step")
			continue
		}
		if step.Date.String() != "2025/12/12" {
			t.Errorf("expected partition for Dec 12, got %s", step.Date.String())
		}
	}
}

// TestPlanPartitions_NotAfterAtExactBoundary tests edge case where notAfter
// is exactly at a partition boundary.
func TestPlanPartitions_NotAfterAtExactBoundary(t *testing.T) {
	// notAfter exactly at Dec 10 22:00 - partition 20-22 ends at exactly this time
	notAfter := time.Date(2025, time.December, 10, 22, 0, 0, 0, time.UTC)

	var (
		defaultExternalLabels = schema.ExternalLabels{}
		defaultHash           = defaultExternalLabels.Hash()
	)

	tsdbStreams := map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
		defaultHash: {
			StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
			Metas: []metadata.Meta{{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
					MinTime: time.Date(2025, time.December, 10, 18, 0, 0, 0, time.UTC).UnixMilli(),
					MaxTime: time.Date(2025, time.December, 10, 24, 0, 0, 0, time.UTC).UnixMilli(),
				},
			}},
		},
	}

	parquetStreams := map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{}

	planner := NewPlannerWithPartitions(notAfter, 7, 2*time.Hour, 0, DefaultPartitionLookback)
	plan := planner.PlanPartitions(tsdbStreams, parquetStreams)

	// 18-20: ends at 20:00 < 22:00 ✓
	// 20-22: ends at 22:00 == 22:00, NOT < 22:00 ✗ (Before is strict)
	// 22-24: ends at 00:00 > 22:00 ✗

	if len(plan.Steps) != 1 {
		t.Errorf("expected 1 partition, got %d", len(plan.Steps))
		for _, step := range plan.Steps {
			t.Logf("  got: %s", step.Partition.String())
		}
	}

	if len(plan.Steps) > 0 && plan.Steps[0].Partition.String() != "2025/12/10/parts/18-20" {
		t.Errorf("expected partition 18-20, got %s", plan.Steps[0].Partition.String())
	}
}

// TestPlanPartitions_OrderMostRecentFirst tests that partitions are ordered
// with the most recent partition first (higher priority for fresh data).
func TestPlanPartitions_OrderMostRecentFirst(t *testing.T) {
	// Scenario: Program starts at 6PM on Dec 2
	// No data for Dec 1 and Dec 2
	// Grace period = 2h
	// notAfter = Dec 2 16:00 (6PM - 2h)
	//
	// Expected order:
	// 1. Dec 2 14-16 (most recent convertible partition)
	// 2. Dec 2 12-14
	// 3. Dec 2 10-12
	// ... etc
	// Then Dec 1 partitions

	notAfter := time.Date(2025, time.December, 2, 16, 0, 0, 0, time.UTC) // 6PM - 2h

	var (
		defaultExternalLabels = schema.ExternalLabels{}
		defaultHash           = defaultExternalLabels.Hash()
	)

	tsdbStreams := map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
		defaultHash: {
			StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
			Metas: []metadata.Meta{
				// Full day Dec 1
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
						MinTime: time.Date(2025, time.December, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				// Full day Dec 2
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						MinTime: time.Date(2025, time.December, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
		},
	}

	parquetStreams := map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{} // No daily blocks

	planner := NewPlannerWithPartitions(notAfter, 7, 2*time.Hour, 0, DefaultPartitionLookback)
	plan := planner.PlanPartitions(tsdbStreams, parquetStreams)

	// Dec 1: all 12 partitions ready
	// Dec 2: partitions 00-02 through 14-16 are ready (ends 16:00 == notAfter, so 14-16 NOT ready)
	// Actually 14-16 ends at 16:00, .Before(16:00) is false, so NOT included
	// So Dec 2: 00-02 through 12-14 (7 partitions)
	// Total: 12 + 7 = 19 partitions

	// Verify the first partition is the most recent convertible one
	if len(plan.Steps) == 0 {
		t.Fatal("expected partitions but got none")
	}

	// Dec 2 12-14 should be first (ends at 14:00, which is < 16:00)
	firstStep := plan.Steps[0]
	if firstStep.Partition == nil {
		t.Fatal("expected partition step, got daily step")
	}
	expectedFirst := "2025/12/02/parts/12-14"
	if firstStep.Partition.String() != expectedFirst {
		t.Errorf("expected first partition to be %s (most recent), got %s", expectedFirst, firstStep.Partition.String())
		t.Logf("All partitions in order:")
		for i, step := range plan.Steps {
			t.Logf("  %d: %s", i, step.Partition.String())
		}
	}

	// Verify ordering: each partition should be older than the previous
	for i := 1; i < len(plan.Steps); i++ {
		prev := plan.Steps[i-1]
		curr := plan.Steps[i]
		if prev.Partition.MinT() < curr.Partition.MinT() {
			t.Errorf("partitions not in descending order at index %d: %s (minT=%d) should come after %s (minT=%d)",
				i, prev.Partition.String(), prev.Partition.MinT(), curr.Partition.String(), curr.Partition.MinT())
		}
	}

	// Verify last partition is Dec 1 00-02 (oldest)
	lastStep := plan.Steps[len(plan.Steps)-1]
	expectedLast := "2025/12/01/parts/00-02"
	if lastStep.Partition.String() != expectedLast {
		t.Errorf("expected last partition to be %s (oldest), got %s", expectedLast, lastStep.Partition.String())
	}
}

// TestPlanHistorical_DoesNotConflictWithPartitions tests that historical planning
// only creates daily blocks for days that are fully complete.
func TestPlanHistorical_DoesNotConflictWithPartitions(t *testing.T) {
	// Current time: Dec 11 03:25
	// notAfter = Dec 11 01:25 (2h grace)
	// Dec 10 ends at Dec 11 00:00, which is < notAfter, so it's "historical"

	notAfter := time.Date(2025, time.December, 11, 1, 25, 0, 0, time.UTC)

	var (
		defaultExternalLabels = schema.ExternalLabels{}
		defaultHash           = defaultExternalLabels.Hash()
	)

	tsdbStreams := map[schema.ExternalLabelsHash]schema.TSDBBlocksStream{
		defaultHash: {
			StreamDescriptor: schema.StreamDescriptor{ExternalLabels: defaultExternalLabels},
			Metas: []metadata.Meta{
				// TSDB blocks for Dec 10
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
						MinTime: time.Date(2025, time.December, 10, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				// TSDB blocks for Dec 11 (partial day)
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						MinTime: time.Date(2025, time.December, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.December, 11, 4, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
		},
	}

	parquetStreams := map[schema.ExternalLabelsHash]schema.ParquetBlocksStream{}

	planner := NewPlannerWithPartitions(notAfter, 7, 2*time.Hour, 0, DefaultPartitionLookback)

	// Historical should create daily block for Dec 10
	historicalPlan := planner.PlanHistorical(tsdbStreams, parquetStreams)

	if len(historicalPlan.Steps) != 1 {
		t.Errorf("expected 1 historical daily step, got %d", len(historicalPlan.Steps))
	}

	if len(historicalPlan.Steps) > 0 {
		step := historicalPlan.Steps[0]
		if step.Partition != nil {
			t.Errorf("expected daily step, got partition")
		}
		if step.Date.String() != "2025/12/10" {
			t.Errorf("expected daily step for Dec 10, got %s", step.Date.String())
		}
	}

	// Partitions should create partitions for Dec 10 (before daily block exists) and Dec 11
	partitionPlan := planner.PlanPartitions(tsdbStreams, parquetStreams)

	// Dec 10: 12 partitions (all before notAfter)
	// Dec 11: only 00-02 is ready (ends at 02:00, notAfter is 01:25, so NOT ready)
	// Actually Dec 11 00-02 ends at 02:00 which is > 01:25, so 0 partitions for Dec 11

	dec10Count, dec11Count := 0, 0
	for _, step := range partitionPlan.Steps {
		if step.Partition == nil {
			continue
		}
		if step.Date.String() == "2025/12/10" {
			dec10Count++
		} else if step.Date.String() == "2025/12/11" {
			dec11Count++
		}
	}

	if dec10Count != 12 {
		t.Errorf("expected 12 partitions for Dec 10, got %d", dec10Count)
	}
	if dec11Count != 0 {
		t.Errorf("expected 0 partitions for Dec 11, got %d", dec11Count)
	}
}
