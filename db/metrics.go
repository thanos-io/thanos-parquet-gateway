// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package db

import (
	"github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	bucketRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bucket_requests_total",
		Help: "Total amount of requests to object storage",
	})
)

func RegisterMetrics(reg prometheus.Registerer) error {
	return multierror.Append(nil,
		reg.Register(bucketRequests),
	).ErrorOrNil()
}
