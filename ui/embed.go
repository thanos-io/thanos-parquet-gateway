// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed cardinality-explorer/dist/*
var cardinalityExplorerFS embed.FS

// CardinalityExplorerHandler returns an http.Handler that serves the cardinality explorer UI.
// The handler serves static files from the embedded filesystem and handles SPA routing
// by serving index.html for any path that doesn't match a file.
func CardinalityExplorerHandler() http.Handler {
	sub, err := fs.Sub(cardinalityExplorerFS, "cardinality-explorer/dist")
	if err != nil {
		// This should never happen as the path is hardcoded and validated at compile time
		panic("failed to create sub filesystem: " + err.Error())
	}

	fileServer := http.FileServer(http.FS(sub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try to serve the file directly
		path := r.URL.Path
		if path == "/" {
			path = "/index.html"
		}

		// Check if the file exists
		f, err := sub.Open(path[1:]) // Remove leading slash
		if err == nil {
			f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}

		// File doesn't exist, serve index.html for SPA routing
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}
