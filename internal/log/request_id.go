// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Package log provides context-aware logging utilities.
package log

import (
	"context"
	"log/slog"
	"net/http"
)

// requestIDKey is the context key for request ID.
type requestIDKey struct{}

// loggerKey is the context key for the logger.
type loggerKey struct{}

// RequestIDFromContext extracts the request ID from context.
// Returns empty string if not present.
func RequestIDFromContext(ctx context.Context) string {
	if v := ctx.Value(requestIDKey{}); v != nil {
		return v.(string)
	}
	return ""
}

// ContextWithRequestID returns a new context with the request ID.
func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, requestID)
}

// defaultLogger is used when no logger is found in context.
var defaultLogger = slog.Default()

// SetDefaultLogger sets the default logger used when no logger is in context.
func SetDefaultLogger(logger *slog.Logger) {
	defaultLogger = logger
}

// WithLogger returns a new context with the given logger stored in it.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// Ctx retrieves the logger from context.
// Returns the default logger if none is stored.
func Ctx(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return defaultLogger
}

// With returns a new context with additional attributes added to the logger.
// This is the slog equivalent of zerolog's WithContext pattern.
func With(ctx context.Context, args ...any) context.Context {
	logger := Ctx(ctx).With(args...)
	return WithLogger(ctx, logger)
}

// FromRequest gets the logger from the request's context.
// This is a shortcut for Ctx(r.Context()).
func FromRequest(r *http.Request) *slog.Logger {
	return Ctx(r.Context())
}
