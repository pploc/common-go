package interceptor

import (
	"context"

	"github.com/pploc/common-go/observability"
	"google.golang.org/grpc"
)

// PropagationStream extracts W3C trace context before other stream interceptors
// observe it. The wrapper also preserves a fallback correlation ID when W3C
// context is unavailable.
func PropagationStream() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &contextStream{ServerStream: stream, ctx: observability.ExtractIncoming(stream.Context())})
	}
}

// PropagationUnary extracts W3C trace context before other unary interceptors
// observe it.
func PropagationUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(observability.ExtractIncoming(ctx), req)
	}
}
