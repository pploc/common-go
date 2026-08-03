package interceptor

import (
	"context"
	"fmt"
	"log/slog"

	commonerrors "github.com/pploc/common-go/errors"
	"github.com/pploc/common-go/logging"
	"github.com/pploc/common-go/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RecoveryUnary turns handler panics into safe internal gRPC failures.
func RecoveryUnary(metrics *observability.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		defer recoverPanic(ctx, info.FullMethod, metrics, &err)
		return handler(ctx, req)
	}
}

// RecoveryStream turns handler and stream-wrapper panics into safe internal
// gRPC failures.
func RecoveryStream(metrics *observability.Metrics) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer recoverPanic(stream.Context(), info.FullMethod, metrics, &err)
		return handler(srv, stream)
	}
}

func recoverPanic(ctx context.Context, method string, metrics *observability.Metrics, err *error) {
	if value := recover(); value != nil {
		if metrics != nil {
			metrics.RecordPanic(ctx, method)
		}
		logging.FromContext(ctx).Error("grpc handler panic", slog.String("method", method), slog.String("panic_type", fmt.Sprintf("%T", value)))
		mapped := commonerrors.ToGRPC(commonerrors.New(commonerrors.CategoryInternal, "INTERNAL", commonerrors.InternalDescription))
		_ = grpc.SetTrailer(ctx, metadata.Pairs(commonerrors.TrailerErrorCode, mapped.Code))
		*err = mapped.Err
	}
}
