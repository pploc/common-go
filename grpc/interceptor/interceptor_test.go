package interceptor

import (
	"context"
	"testing"

	"github.com/pploc/common-go/auth"
	commonerrors "github.com/pploc/common-go/errors"
	"github.com/pploc/common-go/grpc/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthUnaryAttachesClaimsAndPublicBypasses(t *testing.T) {
	interceptor := AuthUnary(AuthOptions{Claims: auth.DefaultOptions(), PublicMethods: []string{"/gym.Service/Health"}})
	info := &grpc.UnaryServerInfo{FullMethod: "/gym.Service/Book"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(auth.HeaderUserID, "one", auth.HeaderUserRole, "admin"))
	_, err := interceptor(ctx, nil, info, func(ctx context.Context, _ any) (any, error) {
		claims, ok := auth.FromContext(ctx)
		if !ok || claims.Role != auth.RoleAdmin {
			t.Fatalf("claims missing: %#v", claims)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/gym.Service/Health"}, func(context.Context, any) (any, error) { return nil, nil })
	if err != nil {
		t.Fatalf("public method did not bypass auth: %v", err)
	}
}

func TestErrorUnaryAddsDomainTrailer(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), nil)
	_, err := ErrorUnary()(ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) {
		return nil, commonerrors.New(commonerrors.CategoryValidation, "INVALID_NAME", "invalid name")
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unexpected code: %v", status.Code(err))
	}
}

func TestAuthorizationUnaryFailsClosed(t *testing.T) {
	_, err := AuthorizationUnary(middleware.RequireRoles(auth.RoleAdmin))(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/gym.Service/Admin"}, func(context.Context, any) (any, error) { return nil, nil })
	mapped := commonerrors.ToGRPC(err)
	if status.Code(mapped.Err) != codes.Unauthenticated {
		t.Fatalf("unexpected code: %v", status.Code(mapped.Err))
	}
}

func TestRecoveryUnaryReturnsSafeInternal(t *testing.T) {
	_, err := RecoveryUnary(nil)(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/gym.Service/Panic"}, func(context.Context, any) (any, error) {
		panic("secret")
	})
	mapped := commonerrors.ToGRPC(err)
	if status.Code(mapped.Err) != codes.Internal || status.Convert(mapped.Err).Message() != commonerrors.InternalDescription {
		t.Fatalf("unexpected recovery response: %v", mapped.Err)
	}
}
