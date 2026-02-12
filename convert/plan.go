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
	Date           util.Date        // Date for which we are building a parquet block.
	Partition      *util.Partition  // Partition within the day (nil for daily blocks).
	Sources        []metadata.Meta  // Source TSDB blocks we will use to generate a parquet block.
	ExternalLabels schema.ExternalLabels
}

// String returns a human-readable representation of the step.
func (s Step) String() string {
	if s.Partition != nil {
		return s.Partition.String()
	}
	return s.Date.String()
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

const (
	// DefaultPartitionLookback is the default time window for partition conversion.
	DefaultPartitionLookback = 24 * time.Hour
)

type Planner struct {
	// Do not create parquet blocks that are younger then this.
	notAfter time.Time
	// Maximum number of days to plan conversions for. Planner might still produce a plan with more days
	// if it would be converting a TSDB block that spans over more days, to avoid re-downloading that block
	// for the next plan.
	maxDays int

	// Partition configuration
	partitionDuration time.Duration // Duration of each partition (e.g., 2h)
	partitionMaxSteps int           // Maximum partitions to convert per cycle
	partitionLookback time.Duration // Time window from notAfter to consider for partitions
}

// NewPlanner creates a new planner for historical (daily) conversions.
func NewPlanner(notAfter time.Time, maxDays int) Planner {
	return Planner{
		notAfter:          notAfter,
		maxDays:           maxDays,
		partitionDuration: 24 * time.Hour,
		partitionLookback: DefaultPartitionLookback,
	}
}

// NewPlannerWithPartitions creates a new planner with partition support.
func NewPlannerWithPartitions(notAfter time.Time, maxDays int, partitionDuration time.Duration, partitionMaxSteps int, partitionLookback time.Duration) Planner {
	if partitionLookback == 0 {
		partitionLookback = DefaultPartitionLookback
	}
	return Planner{
		notAfter:          notAfter,
		maxDays:           maxDays,
		partitionDuration: partitionDuration,
		partitionMaxSteps: partitionMaxSteps,
		partitionLookback: partitionLookback,
	}
}

func (p Planner) planStream(tsdb schema.TSDBBlocksStream, parquet schema.ParquetBlocksStream) Plan {
	// Make a list of days covered by TSDB blocks.
	tsdbDates := map[util.Date][]metadata.Meta{}
	for _, tsdb := range tsdb.Metas {
		for _, partialDate := range util.SplitIntoDates(tsdb.MinTime, tsdb.MaxTime) {
			tsdbDates[partialDate] = append(tsdbDates[partialDate], tsdb)
		}
	}

	// Make a list of days covered by parquet blocks.
	pqDates := map[util.Date]struct{}{}
	for _, pq := range parquet.Metas {
		for _, partialDate := range util.SplitIntoDates(pq.Mint, pq.Maxt) {
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

func (p Planner) Plan(tsdbStreams map[schema.ExternalLabelsHash]schema.TSDBBlocksStream, parquetStreams map[schema.ExternalLabelsHash]schema.ParquetBlocksStream) Plan {
	outPlan := Plan{Steps: []Step{}}

	for tsdbEH := range tsdbStreams {
		parquet, ok := parquetStreams[tsdbEH]
		if !ok {
			parquet = schema.ParquetBlocksStream{}
		}

		streamPlan := p.planStream(tsdbStreams[tsdbEH], parquet)
		for i := range streamPlan.Steps {
			streamPlan.Steps[i].ExternalLabels = tsdbStreams[tsdbEH].ExternalLabels
		}

		outPlan.Steps = append(outPlan.Steps, streamPlan.Steps...)
	}

	return outPlan
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

// PlanPartitions plans partitions for recent data only (within lookback window).
// Groups by external labels hash to handle multi-tenant scenarios.
func (p Planner) PlanPartitions(
	tsdbStreams map[schema.ExternalLabelsHash]schema.TSDBBlocksStream,
	parquetStreams map[schema.ExternalLabelsHash]schema.ParquetBlocksStream,
) Plan {
	outPlan := Plan{Steps: []Step{}}

	for tsdbEH, tsdbStream := range tsdbStreams {
		parquetStream, ok := parquetStreams[tsdbEH]
		if !ok {
			parquetStream = schema.ParquetBlocksStream{}
		}

		streamSteps := p.planStreamPartitions(tsdbStream, parquetStream)
		for i := range streamSteps {
			streamSteps[i].ExternalLabels = tsdbStream.ExternalLabels
		}

		outPlan.Steps = append(outPlan.Steps, streamSteps...)
	}

	// Sort steps, most recent first
	if len(outPlan.Steps) > 0 {
		slices.SortFunc(outPlan.Steps, func(a, b Step) int {
			return cmp.Compare(b.Partition.MinT(), a.Partition.MinT())
		})

		if p.partitionMaxSteps > 0 && len(outPlan.Steps) > p.partitionMaxSteps {
			outPlan.Steps = outPlan.Steps[:p.partitionMaxSteps]
		}
	}

	return outPlan
}

// planStreamPartitions plans partitions for a single stream.
func (p Planner) planStreamPartitions(
	tsdbStream schema.TSDBBlocksStream,
	parquetStream schema.ParquetBlocksStream,
) []Step {
	lookbackCutoff := p.notAfter.Add(-p.partitionLookback)

	// Find all days that have daily parquet blocks in this stream
	daysWithDailyBlocks := make(map[string]struct{})
	for _, pqMeta := range parquetStream.Metas {
		if pqMeta.Partition != nil {
			continue // Skip partition blocks
		}
		for _, date := range util.SplitIntoDates(pqMeta.Mint, pqMeta.Maxt) {
			daysWithDailyBlocks[date.String()] = struct{}{}
		}
	}

	// Find TSDB blocks that cover days within the partition window
	dayToBlocks := make(map[string][]metadata.Meta)
	for _, tsdb := range tsdbStream.Metas {
		for _, date := range util.SplitIntoDates(tsdb.MinTime, tsdb.MaxTime) {
			dayStr := date.String()
			dayStart := time.UnixMilli(date.MinT())
			dayEnd := time.UnixMilli(date.MaxT())

			if !dayEnd.After(lookbackCutoff) || !dayStart.Before(p.notAfter) {
				continue
			}

			if _, hasDailyBlock := daysWithDailyBlocks[dayStr]; hasDailyBlock {
				continue
			}

			dayToBlocks[dayStr] = append(dayToBlocks[dayStr], tsdb)
		}
	}

	// Build existing partitions set from parquet stream
	existingPartitions := make(map[string]struct{})
	for _, pqMeta := range parquetStream.Metas {
		if pqMeta.Partition != nil {
			existingPartitions[pqMeta.Name] = struct{}{}
		}
	}

	// Plan partitions for each eligible day
	steps := make([]Step, 0)
	for dayStr, blocks := range dayToBlocks {
		date, err := util.DateFromString(dayStr)
		if err != nil {
			continue
		}
		daySteps := p.planDayPartitionsFromBlocks(blocks, existingPartitions, date)
		steps = append(steps, daySteps...)
	}

	return steps
}

// PlanHistorical returns daily conversion steps for days that are fully in the past.
// Groups by external labels hash to handle multi-tenant scenarios.
func (p Planner) PlanHistorical(
	tsdbStreams map[schema.ExternalLabelsHash]schema.TSDBBlocksStream,
	parquetStreams map[schema.ExternalLabelsHash]schema.ParquetBlocksStream,
) Plan {
	outPlan := Plan{Steps: []Step{}}

	for tsdbEH, tsdbStream := range tsdbStreams {
		parquetStream, ok := parquetStreams[tsdbEH]
		if !ok {
			parquetStream = schema.ParquetBlocksStream{}
		}

		streamSteps := p.planStreamHistorical(tsdbStream, parquetStream)
		for i := range streamSteps {
			streamSteps[i].ExternalLabels = tsdbStream.ExternalLabels
		}

		outPlan.Steps = append(outPlan.Steps, streamSteps...)
	}

	return outPlan
}

// planStreamHistorical plans historical daily blocks for a single stream.
func (p Planner) planStreamHistorical(
	tsdbStream schema.TSDBBlocksStream,
	parquetStream schema.ParquetBlocksStream,
) []Step {
	// Build set of existing daily parquet blocks
	pqDates := make(map[util.Date]struct{})
	for _, pqMeta := range parquetStream.Metas {
		if pqMeta.Partition != nil {
			continue // Skip partition blocks for daily planning
		}
		for _, date := range util.SplitIntoDates(pqMeta.Mint, pqMeta.Maxt) {
			pqDates[date] = struct{}{}
		}
	}

	// Group TSDB blocks by date
	tsdbDates := make(map[util.Date][]metadata.Meta)
	for _, tsdb := range tsdbStream.Metas {
		for _, date := range util.SplitIntoDates(tsdb.MinTime, tsdb.MaxTime) {
			// Only include if day is fully in the past
			dayEnd := time.UnixMilli(date.MaxT())
			if !dayEnd.Before(p.notAfter) {
				continue
			}
			tsdbDates[date] = append(tsdbDates[date], tsdb)
		}
	}

	// Plan steps for dates not yet converted
	steps := make([]Step, 0, len(tsdbDates))
	for date, metas := range tsdbDates {
		if _, ok := pqDates[date]; ok {
			continue
		}

		slices.SortFunc(metas, func(a, b metadata.Meta) int {
			return cmp.Compare(a.MinTime, b.MinTime)
		})

		steps = append(steps, Step{
			Date:      date,
			Partition: nil,
			Sources:   metas,
		})
	}

	// Sort by date descending (most recent first)
	slices.SortFunc(steps, func(a, b Step) int {
		return cmp.Compare(b.Date.MinT(), a.Date.MinT())
	})

	// Remove the most recent day if it's not fully covered by TSDB blocks.
	steps = truncateLastPartialDay(steps)

	return limitSteps(steps, p.maxDays)
}

// planDayPartitionsFromBlocks plans partition steps for a single day.
func (p Planner) planDayPartitionsFromBlocks(
	tsdbBlocks []metadata.Meta,
	existingPartitions map[string]struct{},
	day util.Date,
) []Step {
	dayStart, dayEnd := day.MinT(), day.MaxT()
	partitions := util.SplitIntoPartitions(dayStart, dayEnd, p.partitionDuration)

	steps := make([]Step, 0)

	for _, partition := range partitions {
		if !partition.End().Before(p.notAfter) {
			continue
		}

		partitionName := partition.String()
		if _, exists := existingPartitions[partitionName]; exists {
			continue
		}

		var partitionBlocks []metadata.Meta
		for _, tsdb := range tsdbBlocks {
			overlaps := tsdb.MaxTime > partition.MinT() && tsdb.MinTime < partition.MaxT()
			if overlaps {
				partitionBlocks = append(partitionBlocks, tsdb)
			}
		}

		if len(partitionBlocks) == 0 {
			continue
		}

		slices.SortFunc(partitionBlocks, func(a, b metadata.Meta) int {
			return cmp.Compare(a.MinTime, b.MinTime)
		})

		part := partition // Copy for pointer
		steps = append(steps, Step{
			Date:      day,
			Partition: &part,
			Sources:   partitionBlocks,
		})
	}

	return steps
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
