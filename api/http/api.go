// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package http

import (
	"net/http"

	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-io/thanos-parquet-gateway/cardinality"
	"github.com/thanos-io/thanos-parquet-gateway/db"
	"github.com/thanos-io/thanos-parquet-gateway/ui"
)

type apiConfig struct {
	queryAPIOpts       []QueryAPIOption
	cardinalityService cardinality.Service
	corsAllowedOrigins []string
}

type APIOption func(*apiConfig)

func QueryOptions(opts ...QueryAPIOption) APIOption {
	return func(cfg *apiConfig) {
		cfg.queryAPIOpts = opts
	}
}

func CardinalityService(svc cardinality.Service) APIOption {
	return func(cfg *apiConfig) {
		cfg.cardinalityService = svc
	}
}

func CORSAllowedOrigins(origins []string) APIOption {
	return func(cfg *apiConfig) {
		cfg.corsAllowedOrigins = origins
	}
}

func corsMiddleware(allowedOrigins []string, next http.Handler) http.Handler {
	allowedSet := make(map[string]struct{}, len(allowedOrigins))
	allowAll := false
	for _, o := range allowedOrigins {
		if o == "*" {
			allowAll = true
			break
		}
		allowedSet[o] = struct{}{}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			if allowAll {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else if _, ok := allowedSet[origin]; ok {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func NewAPI(db *db.DB, engine promql.QueryEngine, opts ...APIOption) http.Handler {
	cfg := &apiConfig{}
	for i := range opts {
		opts[i](cfg)
	}

	r := route.New()

	api := r.WithPrefix("/api/v1")
	RegisterQueryV1(api, db, engine, cfg.queryAPIOpts...)

	if cfg.cardinalityService != nil {
		RegisterCardinalityV1(api, cfg.cardinalityService)
		r.Get("/ui", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/ui/", http.StatusMovedPermanently)
		})
		uiHandler := http.StripPrefix("/ui", ui.CardinalityExplorerHandler())
		r.Get("/ui/*path", uiHandler.ServeHTTP)
	}

	if len(cfg.corsAllowedOrigins) > 0 {
		return corsMiddleware(cfg.corsAllowedOrigins, r)
	}

	return r
}
