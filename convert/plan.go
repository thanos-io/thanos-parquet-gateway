// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package convert

import (
	"cmp"
	"slices"
	"time"

	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
	"github.com/thanos-io/thanos-parquet-gateway/schema"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type Step struct {
	Date    util.Date       // Date for which we are building a parquet block.
	Sources []metadata.Meta // Source TSDB blocks we will use to generate a parquet block.
}

// isFullyCovered returns true if blocks for this date cover the whole day.
// We return true even if there are gaps in coverage, we only care that there
// are blocks that cover both min and max timestamps.
func (s Step) isFullyCovered() bool {
	mint := s.Date.MinT()
	maxt := s.Date.MaxT()
	var gotMin, gotMax bool
	for _, s := range s.Sources {
		if s.MinTime <= mint {
			gotMin = true
		}
		if s.MaxTime >= maxt {
			gotMax = true
		}
	}
	return gotMin && gotMax
}

type Plan struct {
	Steps []Step
}

type Planner struct {
	// Do not create parquet blocks that are younger then this.
	notAfter time.Time
	// Maximum number of days to plan conversions for. Planner might still produce a plan with more days
	// if it would be converting a TSDB block that spans over more days, to avoid re-downloading that block
	// for the next plan.
	maxDays int
	// Optional relative time window (offsets from now). If both zero, no window filtering.
	minTimeOffset time.Duration
	maxTimeOffset time.Duration
}

type PlannerOption func(*Planner)

// WithTimeWindow sets optional relative time window offsets.
func WithTimeWindow(minOffset, maxOffset time.Duration) PlannerOption {
	return func(p *Planner) {
		p.minTimeOffset = minOffset
		p.maxTimeOffset = maxOffset
	}
}

func NewPlanner(notAfter time.Time, maxDays int, opts ...PlannerOption) Planner {
	p := Planner{notAfter: notAfter, maxDays: maxDays}
	for _, opt := range opts {
		opt(&p)
	}
	return p
}

func (p Planner) Plan(tsdbMetas map[string]metadata.Meta, parquetMetas map[string]schema.Meta) Plan {
	// Pre-compute window day bounds if configured.
	var (
		windowActive bool
		winStartDay  time.Time
		winEndDay    time.Time // exclusive
	)

	if p.minTimeOffset != 0 || p.maxTimeOffset != 0 {
		now := time.Now()
		winStart := now.Add(p.minTimeOffset)
		winEnd := now.Add(p.maxTimeOffset)
		if winStart.Before(winEnd) { // valid window
			windowActive = true
			winStartDay = util.NewDate(winStart.Year(), winStart.Month(), winStart.Day()).ToTime()
			winEndDay = util.NewDate(winEnd.Year(), winEnd.Month(), winEnd.Day()).ToTime() // exclusive
		}
	}

	// Make a list of days covered by TSDB blocks.
	tsdbDates := map[util.Date][]metadata.Meta{}
	for _, tsdb := range tsdbMetas {
		for _, partialDate := range util.SplitIntoDates(tsdb.MinTime, tsdb.MaxTime) {
			// Apply window filtering early: skip dates outside window when active.
			if windowActive {
				pt := partialDate.ToTime()
				if pt.Before(winStartDay) || !pt.Before(winEndDay) {
					continue
				}
			}
			tsdbDates[partialDate] = append(tsdbDates[partialDate], tsdb)
		}
	}

	// Make a list of days covered by parquet blocks.
	pqDates := map[util.Date]struct{}{}
	for _, pq := range parquetMetas {
		for _, partialDate := range util.SplitIntoDates(pq.Mint, pq.Maxt) {
			// Apply same window filtering for existing parquet coverage when active.
			if windowActive {
				pt := partialDate.ToTime()
				if pt.Before(winStartDay) || !pt.Before(winEndDay) {
					continue
				}
			}
			pqDates[partialDate] = struct{}{}
		}
	}

	// Find TSDB dates not covered by parquet dates.
	steps := make([]Step, 0, len(tsdbDates))
	for date, metas := range tsdbDates {
		if !date.ToTime().Before(p.notAfter) {
			// Ignore TSDB blocks that are for dates excluded from conversions.
			continue
		}

		if _, ok := pqDates[date]; ok {
			// This date is already covered by a parquet block.
			continue
		}

		// Sort our tsdb block metas from oldest to the newest.
		slices.SortFunc(metas, func(a, b metadata.Meta) int {
			return cmp.Compare(a.MinTime, b.MinTime)
		})

		steps = append(steps, Step{
			Date:    date,
			Sources: metas,
		})
	}

	// Sort our days, most recent first.
	slices.SortFunc(steps, func(a, b Step) int {
		return cmp.Compare(b.Date.MinT(), a.Date.MinT())
	})

	// Remove the most recent day if it's not fully covered by TSDB blocks.
	// We do this because we might get some delayed blocks for it.
	// Any gaps in days older than the most recent one are ignored.
	steps = truncateLastPartialDay(steps)

	// Restrict our plan to have only up to maxDays number of steps.
	// But allow more steps if they come from TSDB blocks that are only on the plan.
	steps = limitSteps(steps, p.maxDays)

	return Plan{Steps: steps}
}

// Get rid of the most recent day if we don't have TSDB blocks for the whole day.
func truncateLastPartialDay(steps []Step) []Step {
	// Empty source, return it as is.
	if len(steps) < 1 {
		return steps
	}
	// Most recent day is NOT fully covered, return all but most recent day.
	if !steps[0].isFullyCovered() {
		return steps[1:]
	}
	// Return as is.
	return steps
}

// Limit the plan to specified max number of days.
// This is a soft limit, final plan might have more days if it makes sense.
func limitSteps(steps []Step, limit int) []Step {
	if len(steps) <= limit {
		// Plan is <= the limit, return it as is.
		return steps
	}

	metas := MergeMetas(steps[:limit])
	for i := limit; i < len(steps); i++ {
		// Loop over all excess days and check if they would require a new block.
		// If yes then exclude them from the plan.
		// If no then keep them on the plan.
		var newBlock bool
		for _, meta := range steps[i].Sources {
			if !slices.ContainsFunc(metas, func(m metadata.Meta) bool {
				return meta.ULID == m.ULID
			}) {
				newBlock = true
			}
		}
		if newBlock {
			break
		}
		limit = i + 1
	}

	return steps[:limit]
}

// Merge dates from all steps into a single slice.
func MergeDates(steps []Step) []util.Date {
	dates := make([]util.Date, 0, len(steps))
	for _, step := range steps {
		dates = append(dates, step.Date)
	}
	return dates
}

// Merge TSDB block metas from all steps into a single slice.
func MergeMetas(steps []Step) (metas []metadata.Meta) {
	for _, step := range steps {
		for _, m := range step.Sources {
			if !slices.ContainsFunc(metas, func(meta metadata.Meta) bool {
				return meta.ULID == m.ULID
			}) {
				metas = append(metas, m)
			}
		}
	}
	return metas
}
