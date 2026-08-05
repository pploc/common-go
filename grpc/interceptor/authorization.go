package interceptor

import (
	"context"

	"github.com/pploc/common-go/auth"
	commonerrors "github.com/pploc/common-go/errors"
	"github.com/pploc/common-go/grpc/middleware"
	"google.golang.org/grpc"
)

// AuthorizationUnary runs policy after authentication. Public methods may reach
// here without claims; the policy table decides (PUBLIC/AUTHENTICATED allow empty
// claims; role/membership rules fail closed).
func AuthorizationUnary(policy middleware.Policy) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if policy == nil {
			return nil, authorizationRequired()
		}
		claims, _ := auth.FromContext(ctx)
		if err := policy.Authorize(ctx, info.FullMethod, claims); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// AuthorizationStream runs policy after authentication. Public methods may reach
// here without claims; the policy table decides.
func AuthorizationStream(policy middleware.Policy) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if policy == nil {
			return authorizationRequired()
		}
		claims, _ := auth.FromContext(stream.Context())
		if err := policy.Authorize(stream.Context(), info.FullMethod, claims); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func authorizationRequired() error {
	return commonerrors.New(commonerrors.CategoryUnauthorized, "AUTHENTICATION_REQUIRED", "authentication is required")
}
