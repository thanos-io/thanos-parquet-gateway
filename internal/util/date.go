// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import (
	"fmt"
	"time"
)

func SplitDays(start time.Time, end time.Time) ([]time.Time, error) {
	if !alignsToStartOfDay(start) {
		return nil, fmt.Errorf("start %q needs to align to start of day", start)
	}
	if !alignsToStartOfDay(end) {
		return nil, fmt.Errorf("end %q needs to align to start of day", end)
	}
	res := make([]time.Time, 0)

	cur := start
	res = append(res, cur.UTC())
	for cur != end {
		cur = cur.AddDate(0, 0, 1)
		res = append(res, cur.UTC())
	}

	return res, nil
}

func BeginOfDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

func EndOfDay(t time.Time) time.Time {
	return BeginOfDay(t).AddDate(0, 0, 1)
}

func alignsToStartOfDay(t time.Time) bool {
	return t.Equal(BeginOfDay(t))
}
