// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSelector(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []*Matcher
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "empty selector braces",
			input:    "{}",
			expected: nil,
		},
		{
			name:     "whitespace only selector",
			input:    "  {}  ",
			expected: nil,
		},
		{
			name:  "single exact match",
			input: `{__name__="http_requests_total"}`,
			expected: []*Matcher{
				{Name: "__name__", Type: MatchEqual, Value: "http_requests_total"},
			},
		},
		{
			name:  "single exact match with spaces",
			input: `{ __name__ = "http_requests_total" }`,
			expected: []*Matcher{
				{Name: "__name__", Type: MatchEqual, Value: "http_requests_total"},
			},
		},
		{
			name:  "multiple matchers",
			input: `{__name__="up",job="prometheus"}`,
			expected: []*Matcher{
				{Name: "__name__", Type: MatchEqual, Value: "up"},
				{Name: "job", Type: MatchEqual, Value: "prometheus"},
			},
		},
		{
			name:  "multiple matchers with spaces",
			input: `{__name__="up", job="prometheus", instance="localhost:9090"}`,
			expected: []*Matcher{
				{Name: "__name__", Type: MatchEqual, Value: "up"},
				{Name: "job", Type: MatchEqual, Value: "prometheus"},
				{Name: "instance", Type: MatchEqual, Value: "localhost:9090"},
			},
		},
		{
			name:  "regex matcher",
			input: `{namespace=~"prod-.*"}`,
			expected: []*Matcher{
				{Name: "namespace", Type: MatchRegexp, Value: "prod-.*"},
			},
		},
		{
			name:  "negative exact match",
			input: `{instance!="localhost:9090"}`,
			expected: []*Matcher{
				{Name: "instance", Type: MatchNotEqual, Value: "localhost:9090"},
			},
		},
		{
			name:  "negative regex match",
			input: `{job!~"test-.*"}`,
			expected: []*Matcher{
				{Name: "job", Type: MatchNotRegexp, Value: "test-.*"},
			},
		},
		{
			name:  "complex selector with all match types",
			input: `{__name__=~"http_.*",namespace="prod",env!="staging",job!~"test-.*"}`,
			expected: []*Matcher{
				{Name: "__name__", Type: MatchRegexp, Value: "http_.*"},
				{Name: "namespace", Type: MatchEqual, Value: "prod"},
				{Name: "env", Type: MatchNotEqual, Value: "staging"},
				{Name: "job", Type: MatchNotRegexp, Value: "test-.*"},
			},
		},
		{
			name:  "escaped quotes in value",
			input: `{msg="hello \"world\""}`,
			expected: []*Matcher{
				{Name: "msg", Type: MatchEqual, Value: `hello "world"`},
			},
		},
		{
			name:  "escaped backslash in value",
			input: `{path="C:\\Users\\test"}`,
			expected: []*Matcher{
				{Name: "path", Type: MatchEqual, Value: `C:\Users\test`},
			},
		},
		{
			name:  "newline escape in value",
			input: `{msg="line1\nline2"}`,
			expected: []*Matcher{
				{Name: "msg", Type: MatchEqual, Value: "line1\nline2"},
			},
		},
		{
			name:  "tab escape in value",
			input: `{msg="col1\tcol2"}`,
			expected: []*Matcher{
				{Name: "msg", Type: MatchEqual, Value: "col1\tcol2"},
			},
		},
		{
			name:  "value with comma inside quotes",
			input: `{msg="a,b,c",name="test"}`,
			expected: []*Matcher{
				{Name: "msg", Type: MatchEqual, Value: "a,b,c"},
				{Name: "name", Type: MatchEqual, Value: "test"},
			},
		},
		{
			name:  "empty value",
			input: `{job=""}`,
			expected: []*Matcher{
				{Name: "job", Type: MatchEqual, Value: ""},
			},
		},
		{
			name:  "label name with underscore prefix",
			input: `{_private="value"}`,
			expected: []*Matcher{
				{Name: "_private", Type: MatchEqual, Value: "value"},
			},
		},
		{
			name:  "label name with numbers",
			input: `{label123="value"}`,
			expected: []*Matcher{
				{Name: "label123", Type: MatchEqual, Value: "value"},
			},
		},
		// Error cases
		{
			name:    "invalid - no braces",
			input:   `__name__="foo"`,
			wantErr: true,
			errMsg:  "must be enclosed in braces",
		},
		{
			name:    "invalid - missing closing brace",
			input:   `{__name__="foo"`,
			wantErr: true,
			errMsg:  "must be enclosed in braces",
		},
		{
			name:    "invalid - missing opening brace",
			input:   `__name__="foo"}`,
			wantErr: true,
			errMsg:  "must be enclosed in braces",
		},
		{
			name:    "invalid - bad label name starting with number",
			input:   `{123invalid="foo"}`,
			wantErr: true,
			errMsg:  "invalid label name",
		},
		{
			name:    "invalid - bad label name with hyphen",
			input:   `{my-label="foo"}`,
			wantErr: true,
			errMsg:  "invalid label name",
		},
		{
			name:    "invalid - empty label name",
			input:   `{="foo"}`,
			wantErr: true,
			errMsg:  "empty label name",
		},
		{
			name:    "invalid - bad regex pattern",
			input:   `{name=~"[invalid"}`,
			wantErr: true,
			errMsg:  "invalid regex",
		},
		{
			name:    "invalid - no operator",
			input:   `{name"value"}`,
			wantErr: true,
			errMsg:  "no operator",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSelector(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.expected))
			for i, m := range got {
				assert.Equal(t, tt.expected[i].Name, m.Name, "matcher %d name", i)
				assert.Equal(t, tt.expected[i].Type, m.Type, "matcher %d type", i)
				assert.Equal(t, tt.expected[i].Value, m.Value, "matcher %d value", i)
			}
		})
	}
}

func TestParseSelectors(t *testing.T) {
	tests := []struct {
		name      string
		selectors []string
		expected  int // number of matchers
		wantErr   bool
	}{
		{
			name:      "empty slice",
			selectors: nil,
			expected:  0,
		},
		{
			name:      "single selector",
			selectors: []string{`{__name__="foo"}`},
			expected:  1,
		},
		{
			name:      "multiple selectors",
			selectors: []string{`{__name__="foo"}`, `{job="bar"}`},
			expected:  2,
		},
		{
			name:      "selector with multiple matchers",
			selectors: []string{`{__name__="foo",job="bar"}`},
			expected:  2,
		},
		{
			name:      "invalid selector in list",
			selectors: []string{`{__name__="foo"}`, `invalid`},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSelectors(tt.selectors)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.expected)
		})
	}
}

func TestMatcherMatches(t *testing.T) {
	tests := []struct {
		name      string
		matchType MatchType
		pattern   string
		value     string
		expected  bool
	}{
		// MatchEqual
		{"equal match", MatchEqual, "foo", "foo", true},
		{"equal no match", MatchEqual, "foo", "bar", false},
		{"equal empty match", MatchEqual, "", "", true},
		{"equal empty no match", MatchEqual, "", "foo", false},

		// MatchNotEqual
		{"not equal match", MatchNotEqual, "foo", "bar", true},
		{"not equal no match", MatchNotEqual, "foo", "foo", false},
		{"not equal empty", MatchNotEqual, "", "foo", true},

		// MatchRegexp
		{"regex match simple", MatchRegexp, "foo.*", "foobar", true},
		{"regex match exact", MatchRegexp, "foo", "foo", true},
		{"regex no match", MatchRegexp, "foo.*", "bar", false},
		{"regex match prefix", MatchRegexp, "http_.*", "http_requests_total", true},
		{"regex no partial match", MatchRegexp, "foo", "foobar", false}, // anchored

		// MatchNotRegexp
		{"not regex match", MatchNotRegexp, "foo.*", "bar", true},
		{"not regex no match", MatchNotRegexp, "foo.*", "foobar", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewMatcher(tt.matchType, "test", tt.pattern)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, m.Matches(tt.value))
		})
	}
}

func TestMatcherToSQL(t *testing.T) {
	tests := []struct {
		name         string
		matchType    MatchType
		labelName    string
		value        string
		expectedSQL  string
		expectedArgs []any
	}{
		{
			name:         "equal",
			matchType:    MatchEqual,
			labelName:    "__name__",
			value:        "http_requests_total",
			expectedSQL:  `"___cf_meta_label___name__" = ?`,
			expectedArgs: []any{"http_requests_total"},
		},
		{
			name:         "not equal",
			matchType:    MatchNotEqual,
			labelName:    "job",
			value:        "prometheus",
			expectedSQL:  `("___cf_meta_label_job" IS NULL OR "___cf_meta_label_job" != ?)`,
			expectedArgs: []any{"prometheus"},
		},
		{
			name:         "regex",
			matchType:    MatchRegexp,
			labelName:    "namespace",
			value:        "prod-.*",
			expectedSQL:  `regexp_matches("___cf_meta_label_namespace", ?)`,
			expectedArgs: []any{"prod-.*"},
		},
		{
			name:         "not regex",
			matchType:    MatchNotRegexp,
			labelName:    "env",
			value:        "test-.*",
			expectedSQL:  `("___cf_meta_label_env" IS NULL OR NOT regexp_matches("___cf_meta_label_env", ?))`,
			expectedArgs: []any{"test-.*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewMatcher(tt.matchType, tt.labelName, tt.value)
			require.NoError(t, err)

			sql, args := m.ToSQL()
			assert.Equal(t, tt.expectedSQL, sql)
			assert.Equal(t, tt.expectedArgs, args)
		})
	}
}

func TestMatchersToSQL(t *testing.T) {
	tests := []struct {
		name         string
		matchers     []*Matcher
		expectedSQL  string
		expectedArgs []any
	}{
		{
			name:         "empty matchers",
			matchers:     nil,
			expectedSQL:  "",
			expectedArgs: nil,
		},
		{
			name: "single matcher",
			matchers: []*Matcher{
				MustNewMatcher(MatchEqual, "__name__", "foo"),
			},
			expectedSQL:  `"___cf_meta_label___name__" = ?`,
			expectedArgs: []any{"foo"},
		},
		{
			name: "multiple matchers",
			matchers: []*Matcher{
				MustNewMatcher(MatchEqual, "__name__", "foo"),
				MustNewMatcher(MatchRegexp, "namespace", "prod-.*"),
			},
			expectedSQL:  `"___cf_meta_label___name__" = ? AND regexp_matches("___cf_meta_label_namespace", ?)`,
			expectedArgs: []any{"foo", "prod-.*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args := MatchersToSQL(tt.matchers)
			assert.Equal(t, tt.expectedSQL, sql)
			assert.Equal(t, tt.expectedArgs, args)
		})
	}
}

func TestMatcherString(t *testing.T) {
	tests := []struct {
		matchType MatchType
		name      string
		value     string
		expected  string
	}{
		{MatchEqual, "__name__", "foo", `__name__="foo"`},
		{MatchNotEqual, "job", "bar", `job!="bar"`},
		{MatchRegexp, "namespace", "prod-.*", `namespace=~"prod-.*"`},
		{MatchNotRegexp, "env", "test", `env!~"test"`},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			m := MustNewMatcher(tt.matchType, tt.name, tt.value)
			assert.Equal(t, tt.expected, m.String())
		})
	}
}

func TestHasLabelMatchers(t *testing.T) {
	tests := []struct {
		name     string
		matchers []*Matcher
		expected bool
	}{
		{
			name:     "nil matchers",
			matchers: nil,
			expected: false,
		},
		{
			name: "only name matcher",
			matchers: []*Matcher{
				MustNewMatcher(MatchEqual, "__name__", "foo"),
			},
			expected: false,
		},
		{
			name: "name and label matcher",
			matchers: []*Matcher{
				MustNewMatcher(MatchEqual, "__name__", "foo"),
				MustNewMatcher(MatchEqual, "job", "bar"),
			},
			expected: true,
		},
		{
			name: "only label matcher",
			matchers: []*Matcher{
				MustNewMatcher(MatchEqual, "job", "bar"),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, HasLabelMatchers(tt.matchers))
		})
	}
}

func TestSplitMatchers(t *testing.T) {
	matchers := []*Matcher{
		MustNewMatcher(MatchEqual, "__name__", "foo"),
		MustNewMatcher(MatchEqual, "job", "bar"),
		MustNewMatcher(MatchRegexp, "__name__", "http_.*"),
		MustNewMatcher(MatchNotEqual, "env", "test"),
	}

	nameMatchers, labelMatchers := SplitMatchers(matchers)

	assert.Len(t, nameMatchers, 2)
	assert.Len(t, labelMatchers, 2)

	assert.Equal(t, "__name__", nameMatchers[0].Name)
	assert.Equal(t, "__name__", nameMatchers[1].Name)
	assert.Equal(t, "job", labelMatchers[0].Name)
	assert.Equal(t, "env", labelMatchers[1].Name)
}

func TestDeduplicateMatchers(t *testing.T) {
	tests := []struct {
		name     string
		matchers []*Matcher
		expected int
	}{
		{
			name:     "nil matchers",
			matchers: nil,
			expected: 0,
		},
		{
			name: "no duplicates",
			matchers: []*Matcher{
				MustNewMatcher(MatchEqual, "__name__", "foo"),
				MustNewMatcher(MatchEqual, "job", "bar"),
			},
			expected: 2,
		},
		{
			name: "with duplicates",
			matchers: []*Matcher{
				MustNewMatcher(MatchEqual, "__name__", "foo"),
				MustNewMatcher(MatchEqual, "job", "bar"),
				MustNewMatcher(MatchRegexp, "__name__", "http_.*"), // duplicate __name__
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DeduplicateMatchers(tt.matchers)
			if tt.expected == 0 {
				assert.Nil(t, result)
			} else {
				assert.Len(t, result, tt.expected)
			}
		})
	}

	// Test that last duplicate wins
	matchers := []*Matcher{
		MustNewMatcher(MatchEqual, "__name__", "first"),
		MustNewMatcher(MatchEqual, "__name__", "second"),
	}
	result := DeduplicateMatchers(matchers)
	require.Len(t, result, 1)
	assert.Equal(t, "second", result[0].Value)
}

func TestMetricNameToMatchers(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		expected   int
	}{
		{"empty", "", 0},
		{"non-empty", "http_requests_total", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MetricNameToMatchers(tt.metricName)
			assert.Len(t, result, tt.expected)
			if tt.expected > 0 {
				assert.Equal(t, "__name__", result[0].Name)
				assert.Equal(t, MatchEqual, result[0].Type)
				assert.Equal(t, tt.metricName, result[0].Value)
			}
		})
	}
}

func TestMetricNameToSelector(t *testing.T) {
	tests := []struct {
		metricName string
		expected   string
	}{
		{"", ""},
		{"foo", `{__name__="foo"}`},
		{"http_requests_total", `{__name__="http_requests_total"}`},
	}

	for _, tt := range tests {
		t.Run(tt.metricName, func(t *testing.T) {
			assert.Equal(t, tt.expected, MetricNameToSelector(tt.metricName))
		})
	}
}

func TestMatchTypeString(t *testing.T) {
	tests := []struct {
		matchType MatchType
		expected  string
	}{
		{MatchEqual, "="},
		{MatchNotEqual, "!="},
		{MatchRegexp, "=~"},
		{MatchNotRegexp, "!~"},
		{MatchType(99), "?"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.matchType.String())
		})
	}
}
