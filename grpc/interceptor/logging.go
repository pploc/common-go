package interceptor

import (
	"context"
	"log/slog"
	"time"

	"github.com/pploc/common-go/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// LoggingUnary logs bounded request completion fields only.
func LoggingUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := handler(ctx, req)
		finalErr := mapError(ctx, err)
		logging.FromContext(ctx).Info("grpc request completed",
			slog.String("method", info.FullMethod),
			slog.String("status", status.Code(finalErr).String()),
			slog.Duration("duration", time.Since(started)),
		)
		return response, finalErr
	}
}

// LoggingStream logs bounded stream completion fields only.
func LoggingStream() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		started := time.Now()
		err := mapError(stream.Context(), handler(srv, stream))
		logging.FromContext(stream.Context()).Info("grpc stream completed",
			slog.String("method", info.FullMethod),
			slog.String("status", status.Code(err).String()),
			slog.Duration("duration", time.Since(started)),
		)
		return err
	}
}
