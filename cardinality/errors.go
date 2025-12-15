// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package cardinality

import "errors"

var (
	ErrNoDataAvailable     = errors.New("no data available for the specified date range")
	ErrInvalidDateRange    = errors.New("start date must be before or equal to end date")
	ErrDateRangeTooLarge   = errors.New("date range exceeds maximum of 365 days")
	ErrInvalidPath         = errors.New("invalid file path")
	ErrCacheDownloadFailed = errors.New("failed to download file to cache")
	ErrMetricNotFound      = errors.New("metric not found")
)
