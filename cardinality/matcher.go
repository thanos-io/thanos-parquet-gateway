// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/thanos-io/thanos-parquet-gateway/schema"
)

type MatchType int

const (
	MatchEqual     MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (m MatchType) String() string {
	switch m {
	case MatchEqual:
		return "="
	case MatchNotEqual:
		return "!="
	case MatchRegexp:
		return "=~"
	case MatchNotRegexp:
		return "!~"
	}
	return "?"
}

type Matcher struct {
	Name  string
	Type  MatchType
	Value string
	re    *regexp.Regexp
}

func NewMatcher(matchType MatchType, name, value string) (*Matcher, error) {
	m := &Matcher{
		Name:  name,
		Type:  matchType,
		Value: value,
	}

	if matchType == MatchRegexp || matchType == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return nil, fmt.Errorf("invalid regex %q: %w", value, err)
		}
		m.re = re
	}

	return m, nil
}

func MustNewMatcher(matchType MatchType, name, value string) *Matcher {
	m, err := NewMatcher(matchType, name, value)
	if err != nil {
		panic(err)
	}
	return m
}

func (m *Matcher) Matches(value string) bool {
	switch m.Type {
	case MatchEqual:
		return value == m.Value
	case MatchNotEqual:
		return value != m.Value
	case MatchRegexp:
		return m.re.MatchString(value)
	case MatchNotRegexp:
		return !m.re.MatchString(value)
	}
	return false
}

func (m *Matcher) String() string {
	return fmt.Sprintf(`%s%s"%s"`, m.Name, m.Type.String(), m.Value)
}

func (m *Matcher) IsNameMatcher() bool {
	return m.Name == "__name__"
}

func (m *Matcher) ToSQL() (string, []any) {
	columnName := schema.LabelNameToColumn(m.Name)
	return m.toSQLColumn(columnName)
}

func (m *Matcher) toSQLColumn(columnName string) (string, []any) {
	quotedCol := `"` + columnName + `"`

	switch m.Type {
	case MatchEqual:
		return fmt.Sprintf(`%s = ?`, quotedCol), []any{m.Value}
	case MatchNotEqual:
		return fmt.Sprintf(`(%s IS NULL OR %s != ?)`, quotedCol, quotedCol), []any{m.Value}
	case MatchRegexp:
		return fmt.Sprintf(`regexp_matches(%s, ?)`, quotedCol), []any{m.Value}
	case MatchNotRegexp:
		return fmt.Sprintf(`(%s IS NULL OR NOT regexp_matches(%s, ?))`, quotedCol, quotedCol), []any{m.Value}
	}
	return "1=1", nil
}

func MatchersToSQL(matchers []*Matcher) (string, []any) {
	if len(matchers) == 0 {
		return "", nil
	}

	var clauses []string
	var args []any

	for _, m := range matchers {
		clause, matcherArgs := m.ToSQL()
		clauses = append(clauses, clause)
		args = append(args, matcherArgs...)
	}

	return strings.Join(clauses, " AND "), args
}

func HasLabelMatchers(matchers []*Matcher) bool {
	for _, m := range matchers {
		if !m.IsNameMatcher() {
			return true
		}
	}
	return false
}

func GetNameMatcher(matchers []*Matcher) *Matcher {
	var nameMatcher *Matcher
	for _, m := range matchers {
		if m.IsNameMatcher() {
			nameMatcher = m
		}
	}
	return nameMatcher
}

func SplitMatchers(matchers []*Matcher) (nameMatchers, labelMatchers []*Matcher) {
	for _, m := range matchers {
		if m.IsNameMatcher() {
			nameMatchers = append(nameMatchers, m)
		} else {
			labelMatchers = append(labelMatchers, m)
		}
	}
	return nameMatchers, labelMatchers
}
