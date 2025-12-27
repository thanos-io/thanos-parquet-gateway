// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import (
	"time"
)

// IsToday checks if a date string represents today's date.
func IsToday(dateStr string) bool {
	today := time.Now().UTC().Format("2006-01-02")
	return dateStr == today
}

// DateFromBlock extracts a date string from a block name (format: YYYY/MM/DD).
func DateFromBlock(blockName string) string {
	// Block name is like "2024/12/10" or "2024/12/10/parts/00-02" - convert to "2024-12-10"
	if len(blockName) >= 10 {
		return blockName[0:4] + "-" + blockName[5:7] + "-" + blockName[8:10]
	}
	return blockName
}
