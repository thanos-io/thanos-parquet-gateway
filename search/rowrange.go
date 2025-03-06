// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"sort"
)

type rowRange struct {
	from  int64
	count int64
}

// intersect intserects the row ranges from left hand sight with the row ranges from rhs
// it assumes that lhs and rhs are simplified and returns a simplified result.
// it operates in o(l+r) time by cursoring through ranges with a two pointer approach.
func intersectRowRanges(lhs []rowRange, rhs []rowRange) []rowRange {
	res := make([]rowRange, 0)
	for l, r := 0, 0; l < len(lhs) && r < len(rhs); {
		al, bl := lhs[l].from, lhs[l].from+lhs[l].count
		ar, br := rhs[r].from, rhs[r].from+rhs[r].count

		// check if rows intersect
		if al <= br && ar <= bl {
			os, oe := max(al, ar), min(bl, br)
			res = append(res, rowRange{from: os, count: oe - os})
		}

		// advance the cursor of the range that ends first
		if bl <= br {
			l++
		} else {
			r++
		}
	}

	return simplify(res)
}

func simplify(rr []rowRange) []rowRange {
	if len(rr) == 0 {
		return nil
	}

	sort.Slice(rr, func(i, j int) bool {
		return rr[i].from < rr[j].from
	})

	tmp := make([]rowRange, 0)
	l := rr[0]
	for i := 1; i < len(rr); i++ {
		r := rr[i]
		al, bl := l.from, l.from+l.count
		ar, br := r.from, r.from+r.count
		if bl < ar {
			tmp = append(tmp, l)
			l = r
			continue
		}

		from := min(al, ar)
		count := max(bl, br) - from
		if count == 0 {
			continue
		}

		l = rowRange{
			from:  from,
			count: count,
		}
	}

	tmp = append(tmp, l)
	res := make([]rowRange, 0, len(tmp))
	for i := range tmp {
		if tmp[i].count != 0 {
			res = append(res, tmp[i])
		}
	}

	return res
}
