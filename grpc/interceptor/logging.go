package interceptor

import (
	"context"
	"log/slog"
	"time"

	"github.com/pploc/common-go/auth"
	"github.com/pploc/common-go/logging"
	"github.com/pploc/common-go/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// LoggingUnary logs bounded request completion fields only.
func LoggingUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		logger := enrichedLogger(ctx)
		response, err := handler(logging.NewContext(ctx, logger), req)
		finalErr := mapError(ctx, err)
		logger.Info("grpc request completed",
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
		ctx := stream.Context()
		logger := enrichedLogger(ctx)
		err := mapError(ctx, handler(srv, &contextStream{ServerStream: stream, ctx: logging.NewContext(ctx, logger)}))
		logger.Info("grpc stream completed",
			slog.String("method", info.FullMethod),
			slog.String("status", status.Code(err).String()),
			slog.Duration("duration", time.Since(started)),
		)
		return err
	}
}

func enrichedLogger(ctx context.Context) *slog.Logger {
	logger := logging.FromContext(ctx)
	if claims, ok := auth.FromContext(ctx); ok {
		logger = logging.WithClaims(logger, claims)
	}
	if _, validW3C := observability.ValidSpanContext(ctx); !validW3C {
		if correlationID, ok := observability.CorrelationID(ctx); ok {
			logger = logging.WithCorrelation(logger, correlationID)
		}
	}
	return logger
}
