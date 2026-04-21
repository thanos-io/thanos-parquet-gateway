// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package matchers

import "github.com/prometheus/prometheus/model/labels"

// ToStringSlice converts a slice of Prometheus matchers to a slice of strings.
func ToStringSlice(matchers []*labels.Matcher) []string {
	res := make([]string, len(matchers))
	for i := range matchers {
		res[i] = matchers[i].String()
	}
	return res
}
