// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDateFromBlock(t *testing.T) {
	tests := []struct {
		blockName string
		expected  string
	}{
		{"2024/12/10", "2024-12-10"},
		{"2024/01/01", "2024-01-01"},
		{"2025/06/15", "2025-06-15"},
		{"2024/12/10/parts/00-02", "2024-12-10"},
	}

	for _, tt := range tests {
		t.Run(tt.blockName, func(t *testing.T) {
			got := DateFromBlock(tt.blockName)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestIsToday(t *testing.T) {
	today := time.Now().UTC().Format("2006-01-02")
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")

	assert.True(t, IsToday(today))
	assert.False(t, IsToday(yesterday))
	assert.False(t, IsToday("2024-01-01"))
}
