// Package logging provides safe structured logger context helpers.
package logging

import (
	"context"
	"log/slog"
)

type loggerContextKey struct{}

// NewContext adds logger to ctx. The logger is immutable; derived loggers are
// returned rather than mutating a shared logger.
func NewContext(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		logger = slog.Default()
	}
	return context.WithValue(ctx, loggerContextKey{}, logger)
}

// FromContext returns the context logger or a safe process default.
func FromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerContextKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}
	return slog.Default()
}
