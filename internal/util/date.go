// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

import (
	"fmt"
	"time"
)

const dateFormat = "2006/01/02"

func NewDate(y int, m time.Month, d int) Date {
	t := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)

	return Date{
		t:    t,
		tstr: t.Format(dateFormat),
	}
}

// DateFromString parses a date string in YYYY/MM/DD format.
func DateFromString(s string) (Date, error) {
	if len(s) != 10 {
		return Date{}, fmt.Errorf("invalid date format: %s", s)
	}
	t, err := time.Parse(dateFormat, s)
	if err != nil {
		return Date{}, fmt.Errorf("unable to parse date: %w", err)
	}
	return NewDate(t.Year(), t.Month(), t.Day()), nil
}

type Date struct {
	t    time.Time
	tstr string
}

func (d Date) String() string {
	return d.tstr
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

// AddDays returns a new Date with the specified number of days added.
func (d Date) AddDays(days int) Date {
	t := d.t.AddDate(0, 0, days)
	return NewDate(t.Year(), t.Month(), t.Day())
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
