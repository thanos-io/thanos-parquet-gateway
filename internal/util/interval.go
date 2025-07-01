// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package util

// Intersects returns if [a, b] and [c, d] intersect.
func Intersects(a, b, c, d int64) bool {
	return !(c > b || a > d)
}

// Contains returns if [a, b] contains [c, d].
func Contains(a, b, c, d int64) bool {
	return a <= c && b >= d
}

// Intersection gives the intersection of [a, b] and [c, d].
// It should first be checked if both intersect using Intersects
func Intersection(a, b, c, d int64) (int64, int64) {
	return max(a, c), min(b, d)
}
