// Package interceptor provides composable gRPC server interceptors.
package interceptor

import (
	"context"

	"github.com/pploc/common-go/auth"
	commonerrors "github.com/pploc/common-go/errors"
	"google.golang.org/grpc"
)

// AuthOptions configures trusted-header claim requirements and explicit public
// method bypasses.
type AuthOptions struct {
	Claims        auth.Options
	PublicMethods []string
}

// AuthUnary extracts gateway claims for authenticated methods.
func AuthUnary(options AuthOptions) grpc.UnaryServerInterceptor {
	claimsOptions := authenticatedOptions(options.Claims)
	public := methods(options.PublicMethods)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if public[info.FullMethod] {
			return handler(ctx, req)
		}
		claims, err := auth.FromIncomingContext(ctx, claimsOptions)
		if err != nil {
			return nil, commonerrors.New(commonerrors.CategoryUnauthorized, "AUTHENTICATION_REQUIRED", "authentication is required")
		}
		return handler(auth.NewContext(ctx, claims), req)
	}
}

// AuthStream extracts gateway claims and overrides the stream context.
func AuthStream(options AuthOptions) grpc.StreamServerInterceptor {
	claimsOptions := authenticatedOptions(options.Claims)
	public := methods(options.PublicMethods)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if public[info.FullMethod] {
			return handler(srv, stream)
		}
		claims, err := auth.FromIncomingContext(stream.Context(), claimsOptions)
		if err != nil {
			return commonerrors.New(commonerrors.CategoryUnauthorized, "AUTHENTICATION_REQUIRED", "authentication is required")
		}
		return handler(srv, &contextStream{ServerStream: stream, ctx: auth.NewContext(stream.Context(), claims)})
	}
}

type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextStream) Context() context.Context { return s.ctx }

func authenticatedOptions(options auth.Options) auth.Options {
	if !options.RequireUserID && !options.RequireRole && !options.RequireGymID && !options.RequireMembership {
		options.RequireUserID = true
		options.RequireRole = true
	}
	return options
}

func methods(values []string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}
