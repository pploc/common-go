package logging

import (
	"log/slog"

	"github.com/pploc/common-go/auth"
)

// WithClaims derives a logger containing only approved, non-sensitive request
// fields. It deliberately omits user IDs, tokens, membership, and trace IDs.
func WithClaims(logger *slog.Logger, claims auth.Claims) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return logger.With("role", string(claims.Role))
}

// WithCorrelation derives a logger with a compatibility correlation ID. Use it
// only when W3C trace context is unavailable.
func WithCorrelation(logger *slog.Logger, correlationID string) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	if correlationID == "" {
		return logger
	}
	return logger.With("correlation_id", correlationID)
}
