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

func TestPlanner(t *testing.T) {
	mockBlocks := func(ulids ...string) []metadata.Meta {
		metas := make([]metadata.Meta, 0, len(ulids))
		for _, v := range ulids {
			metas = append(metas, metadata.Meta{
				BlockMeta: tsdb.BlockMeta{ULID: ulid.MustParse(v)},
			})
		}
		return metas
	}

	for _, tc := range []struct {
		name string

		notAfter     time.Time
		maxDays      int
		tsdbMetas    map[string]metadata.Meta
		parquetMetas map[string]schema.Meta

		expectedPlan Plan
	}{
		{
			name:     "last day only partially covered",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2025, time.March, 2, 11, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2025/03/02": {
					Name: "2025/03/02",
					Mint: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectedPlan: Plan{Steps: []Step{}},
		},
		{
			name:     "three blocks cover a full day, previous parquet file for initial overlap",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2025, time.March, 2, 11, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DWY7YB7TE9TNHY5NTAYWT": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DWY7YB7TE9TNHY5NTAYWT"),
						MinTime: time.Date(2025, time.March, 3, 5, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 3, 18, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DYXS7CJ7VCFH63WD1S006": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DYXS7CJ7VCFH63WD1S006"),
						MinTime: time.Date(2025, time.March, 3, 18, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 4, 4, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2025/03/02": {
					Name: "2025/03/02",
					Mint: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
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
					},
				},
			},
		},
		{
			name:     "we dont convert blocks that are too young still",
			notAfter: time.Date(2025, time.March, 1, 0, 0, 0, 0, time.UTC),
			maxDays:  7,
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2025, time.March, 2, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2025, time.March, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{},
			expectedPlan: Plan{Steps: []Step{}},
		},
		{
			name:     "we have all blocks already",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 24, 18, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JU0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 24, 18, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 25, 12, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2020/01/04": {
					Name: "2020/01/04",
					Mint: time.Date(2020, time.January, 4, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:    util.NewDate(2020, time.January, 24),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK", "01JU0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 23),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 22),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 21),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 20),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 19),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 18),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 17),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 16),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 15),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 14),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 13),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 12),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 11),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 10),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 9),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 8),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 7),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 6),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 5),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
				},
			},
		},
		{
			name:     "we can amoritize downloads by converting as many blocks as possible",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 7, 18, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:    util.NewDate(2020, time.January, 6),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 5),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 4),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
				},
			},
		},
		{
			name:     "upload gap does not stall converter",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbMetas: map[string]metadata.Meta{
				"01JS0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 5, 18, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 5, 18, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 6, 12, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JU0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 7, 18, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 8, 6, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2020/01/04": {
					Name: "2020/01/04",
					Mint: time.Date(2020, time.January, 4, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
				"2020/01/05": {
					Name: "2020/01/05",
					Mint: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2020, time.January, 6, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:    util.NewDate(2020, time.January, 7),
						Sources: mockBlocks("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2020, time.January, 6),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
				},
			},
		},
		{
			name:     "gap that's longer than a day does not stall converter",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbMetas: map[string]metadata.Meta{
				"01JS0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JS0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2024, time.September, 9, 12, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2024, time.September, 10, 6, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2024, time.September, 24, 4, 45, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2024, time.September, 28, 12, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2024/09/09": {
					Name: "2024/09/09",
					Mint: time.Date(2024, time.September, 9, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2024, time.September, 10, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
				"2024/09/10": {
					Name: "2024/09/10",
					Mint: time.Date(2024, time.September, 10, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2024, time.September, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:    util.NewDate(2024, time.September, 27),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2024, time.September, 26),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2024, time.September, 25),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
					{
						Date:    util.NewDate(2024, time.September, 24),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
					},
				},
			},
		},
		{
			name:     "tsdb block covering full day and a partial parquet block for it",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  7,
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXZ": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXZ"),
						MinTime: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2020/01/23": {
					Name: "2020/01/23",
					Mint: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2020, time.January, 23, 12, 0, 0, 0, time.UTC).UnixMilli(),
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
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXZ": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXZ"),
						MinTime: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 24, 4, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2020/01/23": {
					Name: "2020/01/23",
					Mint: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectedPlan: Plan{Steps: []Step{}},
		},
		{
			name:     "converted block followed by a partial day block, followed by an unconverted block",
			notAfter: time.Date(2020, time.January, 25, 0, 0, 0, 0, time.UTC),
			maxDays:  7,
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXZ": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXZ"),
						MinTime: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 24, 18, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JU0DPYGA1HPW5RBZ1KBXCNXK": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JU0DPYGA1HPW5RBZ1KBXCNXK"),
						MinTime: time.Date(2020, time.January, 25, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 25, 2, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			parquetMetas: map[string]schema.Meta{
				"2020/01/23": {
					Name: "2020/01/23",
					Mint: time.Date(2020, time.January, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
					Maxt: time.Date(2020, time.January, 24, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
			expectedPlan: Plan{Steps: []Step{}}, // We can only convert 01/24 because of notAfter limit, but it's partial so we skip it
		},
		{
			name:     "3 * 2 day blocks, maxDays=3, should convert first two blocks",
			notAfter: time.UnixMilli(math.MaxInt64),
			maxDays:  3,
			tsdbMetas: map[string]metadata.Meta{
				"01JT0DPYGA1HPW5RBZ1KBXCNXA": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXA"),
						MinTime: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DPYGA1HPW5RBZ1KBXCNXB": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
						MinTime: time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
				"01JT0DPYGA1HPW5RBZ1KBXCNXC": {
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustParse("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
						MinTime: time.Date(2020, time.January, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
						MaxTime: time.Date(2020, time.January, 7, 0, 0, 0, 0, time.UTC).UnixMilli(),
					},
				},
			},
			expectedPlan: Plan{
				Steps: []Step{
					{
						Date:    util.NewDate(2020, time.January, 6),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
					},
					{
						Date:    util.NewDate(2020, time.January, 5),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXC"),
					},
					{
						Date:    util.NewDate(2020, time.January, 4),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
					},
					{
						Date:    util.NewDate(2020, time.January, 3),
						Sources: mockBlocks("01JT0DPYGA1HPW5RBZ1KBXCNXB"),
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(tt *testing.T) {
			plan := NewPlanner(tc.notAfter, tc.maxDays).Plan(tc.tsdbMetas, tc.parquetMetas)

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
