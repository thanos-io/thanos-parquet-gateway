// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import (
	"time"
)

func NewDate(y int, m time.Month, d int) Date {
	return Date{
		t: time.Date(y, m, d, 0, 0, 0, 0, time.UTC),
	}
}

type Date struct {
	t time.Time
}

func (d Date) String() string {
	return d.t.Format(time.DateOnly)
}

func (d Date) ToTime() time.Time {
	return d.t
}

func (d Date) MinT() int64 {
	return d.t.UnixMilli()
}

func (d Date) MaxT() int64 {
	return d.t.AddDate(0, 0, 1).UnixMilli()
}

func nextDay(t time.Time) time.Time {
	t = t.AddDate(0, 0, 1)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func SplitIntoDates(mint, maxt int64) []Date {
	start := time.UnixMilli(mint).UTC()
	end := time.UnixMilli(maxt).UTC()
	res := []Date{}
	for {
		res = append(res, NewDate(start.Year(), start.Month(), start.Day()))
		start = nextDay(start)
		if !start.Before(end) {
			break
		}
	}
	return res
}

func BeginOfDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func EndOfDay(t time.Time) time.Time {
	return BeginOfDay(t).AddDate(0, 0, 1)
}
