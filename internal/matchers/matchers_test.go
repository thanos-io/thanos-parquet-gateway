// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package matchers

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

func TestToStringSlice(t *testing.T) {
	tests := []struct {
		name     string
		matchers []*labels.Matcher
		want     []string
	}{
		{
			name:     "nil input",
			matchers: nil,
			want:     []string{},
		},
		{
			name:     "empty input",
			matchers: []*labels.Matcher{},
			want:     []string{},
		},
		{
			name: "single matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
			},
			want: []string{`__name__="up"`},
		},
		{
			name: "multiple matchers with different types",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
				labels.MustNewMatcher(labels.MatchNotEqual, "job", "test"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "localhost.*"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "env", "prod.*"),
			},
			want: []string{
				`__name__="http_requests_total"`,
				`job!="test"`,
				`instance=~"localhost.*"`,
				`env!~"prod.*"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToStringSlice(tt.matchers)
			if len(got) != len(tt.want) {
				t.Fatalf("len(ToStringSlice()) = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("ToStringSlice()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
