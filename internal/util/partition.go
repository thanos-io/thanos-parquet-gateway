// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import (
	"fmt"
	"time"
)

// Partition represents a time window for sub-daily parquet block generation.
type Partition struct {
	start    time.Time
	duration time.Duration
}

// NewPartition creates a new partition starting at the given time with the specified duration.
// The start time is truncated to align with the duration boundary.
func NewPartition(start time.Time, duration time.Duration) Partition {
	return Partition{
		start:    start.Truncate(duration),
		duration: duration,
	}
}

// NewPartitionFromTimestamp creates a partition from a Unix millisecond timestamp.
func NewPartitionFromTimestamp(timestamp int64, duration time.Duration) Partition {
	start := time.UnixMilli(timestamp).UTC()
	return NewPartition(start, duration)
}

// String returns a string representation of the partition for use in file names.
// Format: "YYYY/MM/DD" for 24h partitions, "YYYY/MM/DD/parts/HH-HH" for sub-daily partitions.
// Examples: "2025/01/19" (daily), "2025/01/19/parts/00-02" (2h partition), "2025/01/19/parts/22-24" (ends at midnight)
func (p Partition) String() string {
	if p.duration == 24*time.Hour {
		// For 24h partitions, use the date format
		return p.start.Format("2006/01/02")
	}

	// For sub-daily partitions, use parts/HH-HH format
	startHour := p.start.Format("15")
	endHour := p.End().Format("15")
	// Midnight displays as "24" for end time (e.g., 22-24 instead of 22-00)
	if endHour == "00" {
		endHour = "24"
	}
	return fmt.Sprintf("%s/parts/%s-%s", p.start.Format("2006/01/02"), startHour, endHour)
}

// Start returns the start time of the partition.
func (p Partition) Start() time.Time {
	return p.start
}

// End returns the end time of the partition.
func (p Partition) End() time.Time {
	return p.start.Add(p.duration)
}

// Duration returns the duration of the partition.
func (p Partition) Duration() time.Duration {
	return p.duration
}

// MinT returns the minimum timestamp (start) in Unix milliseconds.
func (p Partition) MinT() int64 {
	return p.start.UnixMilli()
}

// MaxT returns the maximum timestamp (end) in Unix milliseconds.
func (p Partition) MaxT() int64 {
	return p.start.Add(p.duration).UnixMilli()
}

// Contains checks if a timestamp falls within this partition.
func (p Partition) Contains(timestamp int64) bool {
	ts := time.UnixMilli(timestamp).UTC()
	return !ts.Before(p.start) && ts.Before(p.End())
}

// SplitIntoPartitions splits a time range into partitions of the specified duration.
func SplitIntoPartitions(mint, maxt int64, duration time.Duration) []Partition {
	if duration <= 0 {
		// Default to 24h if invalid duration
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

// PartitionFromString attempts to parse a partition string back to a Partition.
// Supports formats:
//   - "YYYY/MM/DD" (24h daily partition)
//   - "YYYY/MM/DD/parts/HH-HH" (sub-daily partition, e.g., "2025/01/19/parts/00-02")
func PartitionFromString(s string) (Partition, error) {
	// Try parsing as date format first (YYYY/MM/DD) - exact 10 characters
	if len(s) == 10 {
		if t, err := time.Parse("2006/01/02", s); err == nil {
			return NewPartition(t, 24*time.Hour), nil
		}
	}

	// Try parsing as parts/HH-HH format (YYYY/MM/DD/parts/HH-HH) - exactly 22 characters
	if len(s) == 22 && s[10:11] == "/" && s[11:16] == "parts" && s[16:17] == "/" && s[19:20] == "-" {
		datePart := s[:10]   // "2025/01/19"
		hourPart := s[17:22] // "00-02"

		t, err := time.Parse("2006/01/02", datePart)
		if err != nil {
			return Partition{}, fmt.Errorf("unable to parse date in partition string: %s", s)
		}

		// Parse start hour
		startHourStr := hourPart[:2]
		var startHour int
		if _, err := fmt.Sscanf(startHourStr, "%02d", &startHour); err != nil {
			return Partition{}, fmt.Errorf("unable to parse start hour in partition string: %s", s)
		}

		// Parse end hour (may be "24" for midnight)
		endHourStr := hourPart[3:]
		var endHour int
		if endHourStr == "24" {
			endHour = 24
		} else {
			if _, err := fmt.Sscanf(endHourStr, "%02d", &endHour); err != nil {
				return Partition{}, fmt.Errorf("unable to parse end hour in partition string: %s", s)
			}
		}

		// Calculate duration from hour range
		partitionDuration := time.Duration(endHour-startHour) * time.Hour
		if partitionDuration <= 0 {
			return Partition{}, fmt.Errorf("invalid hour range in partition string: %s", s)
		}

		startTime := t.Add(time.Duration(startHour) * time.Hour)
		return NewPartition(startTime, partitionDuration), nil
	}

	return Partition{}, fmt.Errorf("unable to parse partition string: %s", s)
}
