// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"fmt"
	"strings"
)

func ParseSelector(selector string) ([]*Matcher, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return nil, nil
	}

	if !strings.HasPrefix(selector, "{") || !strings.HasSuffix(selector, "}") {
		return nil, fmt.Errorf("selector must be enclosed in braces: %q", selector)
	}
	selector = selector[1 : len(selector)-1]

	if selector == "" {
		return nil, nil
	}

	var matchers []*Matcher
	for _, part := range splitMatchers(selector) {
		m, err := parseMatcher(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)
	}

	return matchers, nil
}

func ParseSelectors(selectors []string) ([]*Matcher, error) {
	var allMatchers []*Matcher
	for _, selector := range selectors {
		matchers, err := ParseSelector(selector)
		if err != nil {
			return nil, fmt.Errorf("invalid selector %q: %w", selector, err)
		}
		allMatchers = append(allMatchers, matchers...)
	}
	return allMatchers, nil
}

func splitMatchers(s string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	escaped := false

	for _, r := range s {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\':
			current.WriteRune(r)
			escaped = true
		case r == '"':
			current.WriteRune(r)
			inQuotes = !inQuotes
		case r == ',' && !inQuotes:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

func parseMatcher(s string) (*Matcher, error) {
	var opStart, opEnd int
	var matchType MatchType
	found := false

	for i := 0; i < len(s); i++ {
		if i+1 < len(s) {
			twoChar := s[i : i+2]
			switch twoChar {
			case "=~":
				opStart, opEnd, matchType = i, i+2, MatchRegexp
				found = true
			case "!~":
				opStart, opEnd, matchType = i, i+2, MatchNotRegexp
				found = true
			case "!=":
				opStart, opEnd, matchType = i, i+2, MatchNotEqual
				found = true
			}
			if found {
				break
			}
		}

		if s[i] == '=' {
			opStart, opEnd, matchType = i, i+1, MatchEqual
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("invalid matcher (no operator): %q", s)
	}

	if opStart == 0 {
		return nil, fmt.Errorf("invalid matcher (empty label name): %q", s)
	}

	name := strings.TrimSpace(s[:opStart])
	value := strings.TrimSpace(s[opEnd:])

	if !isValidLabelName(name) {
		return nil, fmt.Errorf("invalid label name: %q", name)
	}

	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		value = value[1 : len(value)-1]
		value = unescapeString(value)
	} else if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		value = value[1 : len(value)-1]
		value = unescapeString(value)
	}

	return NewMatcher(matchType, name, value)
}

func isValidLabelName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if i == 0 {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
				return false
			}
		} else {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
				return false
			}
		}
	}
	return true
}

func unescapeString(s string) string {
	var result strings.Builder
	escaped := false
	for _, r := range s {
		if escaped {
			switch r {
			case 'n':
				result.WriteRune('\n')
			case 't':
				result.WriteRune('\t')
			case '\\':
				result.WriteRune('\\')
			case '"':
				result.WriteRune('"')
			case '\'':
				result.WriteRune('\'')
			default:
				result.WriteRune(r)
			}
			escaped = false
		} else if r == '\\' {
			escaped = true
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func DeduplicateMatchers(matchers []*Matcher) []*Matcher {
	if len(matchers) == 0 {
		return nil
	}

	seen := make(map[string]int)
	var result []*Matcher

	for _, m := range matchers {
		if idx, ok := seen[m.Name]; ok {
			result[idx] = m // Replace existing
		} else {
			seen[m.Name] = len(result)
			result = append(result, m)
		}
	}

	return result
}

func MetricNameToSelector(metricName string) string {
	if metricName == "" {
		return ""
	}
	return fmt.Sprintf(`{__name__="%s"}`, metricName)
}

func MetricNameToMatchers(metricName string) []*Matcher {
	if metricName == "" {
		return nil
	}
	return []*Matcher{MustNewMatcher(MatchEqual, "__name__", metricName)}
}
