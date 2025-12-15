// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import (
	"fmt"
	"time"
)

type Partition struct {
	start    time.Time
	duration time.Duration
}

func NewPartition(start time.Time, duration time.Duration) Partition {
	return Partition{
		start:    start.Truncate(duration),
		duration: duration,
	}
}

func NewPartitionFromTimestamp(timestamp int64, duration time.Duration) Partition {
	start := time.UnixMilli(timestamp).UTC()
	return NewPartition(start, duration)
}

func (p Partition) String() string {
	if p.duration == 24*time.Hour {
		return p.start.Format("2006/01/02")
	}

	startHour := p.start.Format("15")
	endHour := p.End().Format("15")
	if endHour == "00" {
		endHour = "24"
	}
	return fmt.Sprintf("%s/parts/%s-%s", p.start.Format("2006/01/02"), startHour, endHour)
}

func (p Partition) Start() time.Time {
	return p.start
}

func (p Partition) End() time.Time {
	return p.start.Add(p.duration)
}

func (p Partition) Duration() time.Duration {
	return p.duration
}

func (p Partition) MinT() int64 {
	return p.start.UnixMilli()
}

func (p Partition) MaxT() int64 {
	return p.start.Add(p.duration).UnixMilli()
}

func (p Partition) Contains(timestamp int64) bool {
	ts := time.UnixMilli(timestamp).UTC()
	return !ts.Before(p.start) && ts.Before(p.End())
}

func SplitIntoPartitions(mint, maxt int64, duration time.Duration) []Partition {
	if duration <= 0 {
		duration = 24 * time.Hour
	}

	start := time.UnixMilli(mint).UTC()
	end := time.UnixMilli(maxt).UTC()

	var partitions []Partition
	current := start.Truncate(duration)

	for current.Before(end) {
		partition := NewPartition(current, duration)
		partitions = append(partitions, partition)
		current = current.Add(duration)
	}

	return partitions
}

func PartitionFromString(s string) (Partition, error) {
	if len(s) == 10 {
		if t, err := time.Parse("2006/01/02", s); err == nil {
			return NewPartition(t, 24*time.Hour), nil
		}
	}

	if len(s) == 22 && s[11:16] == "parts" && s[19] == '-' {
		datePart := s[:10]
		hourPart := s[17:22]

		t, err := time.Parse("2006/01/02", datePart)
		if err != nil {
			return Partition{}, fmt.Errorf("unable to parse date in partition string: %s", s)
		}

		startHourStr := hourPart[:2]
		var startHour int
		if _, err := fmt.Sscanf(startHourStr, "%02d", &startHour); err != nil {
			return Partition{}, fmt.Errorf("unable to parse start hour in partition string: %s", s)
		}

		endHourStr := hourPart[3:]
		var endHour int
		if endHourStr == "24" {
			endHour = 24
		} else {
			if _, err := fmt.Sscanf(endHourStr, "%02d", &endHour); err != nil {
				return Partition{}, fmt.Errorf("unable to parse end hour in partition string: %s", s)
			}
		}

		partitionDuration := time.Duration(endHour-startHour) * time.Hour
		if partitionDuration <= 0 {
			return Partition{}, fmt.Errorf("invalid hour range in partition string: %s", s)
		}

		startTime := t.Add(time.Duration(startHour) * time.Hour)
		return NewPartition(startTime, partitionDuration), nil
	}

	return Partition{}, fmt.Errorf("unable to parse partition string: %s", s)
}
