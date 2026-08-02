package interceptor

import (
	"context"

	commonerrors "github.com/pploc/common-go/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ErrorUnary maps returned domain failures to gRPC status errors and emits their
// stable application code as an x-error-code trailer.
func ErrorUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, req)
		return response, mapError(ctx, err)
	}
}

// ErrorStream maps returned domain failures to gRPC status errors and emits
// their stable application code as an x-error-code trailer.
func ErrorStream() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return mapError(stream.Context(), handler(srv, stream))
	}
}

func mapError(ctx context.Context, err error) error {
	mapped := commonerrors.ToGRPC(err)
	if mapped.Code != "" {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(commonerrors.TrailerErrorCode, mapped.Code))
	}
	return mapped.Err
}
