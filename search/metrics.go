// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package search

import (
	"github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pagesDiscarded = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pages_discarded_total",
		Help: "Pages discarded during parquet index scans",
	}, []string{"column"},
	)
	pagesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pages_read_total",
		Help: "Pages read during parquet page scans",
	}, []string{"column"},
	)
)

func RegisterMetrics(reg prometheus.Registerer) error {
	return multierror.Append(nil,
		reg.Register(pagesDiscarded),
		reg.Register(pagesRead),
	).ErrorOrNil()
}
