package interceptor

import (
	"context"
	"time"

	"github.com/pploc/common-go/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// MetricsUnary records bounded request metrics after handlers complete.
func MetricsUnary(metrics *observability.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := handler(ctx, req)
		finalErr := mapError(ctx, err)
		metrics.RecordRequest(ctx, info.FullMethod, status.Code(finalErr).String(), time.Since(started))
		return response, finalErr
	}
}

// MetricsStream records bounded stream metrics after handlers complete.
func MetricsStream(metrics *observability.Metrics) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		started := time.Now()
		err := mapError(stream.Context(), handler(srv, stream))
		metrics.RecordRequest(stream.Context(), info.FullMethod, status.Code(err).String(), time.Since(started))
		return err
	}
}
