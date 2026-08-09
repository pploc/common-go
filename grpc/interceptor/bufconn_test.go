package interceptor

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pploc/common-go/auth"
	commonerrors "github.com/pploc/common-go/errors"
	"github.com/pploc/common-go/grpc/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

const testMethod = "/test.Service/Call"

type testService interface {
	Call(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

type testServer struct {
	claims chan auth.Claims
	err    error
}

func (s *testServer) Call(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if s.claims != nil {
		s.claims <- auth.MustFromContext(ctx)
	}
	return &emptypb.Empty{}, s.err
}

var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.Service",
	HandlerType: (*testService)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Call",
		Handler: func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			request := new(emptypb.Empty)
			if err := decode(request); err != nil {
				return nil, err
			}
			handler := func(ctx context.Context, request any) (any, error) {
				return srv.(testService).Call(ctx, request.(*emptypb.Empty))
			}
			if interceptor == nil {
				return handler(ctx, request)
			}
			return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: testMethod}, handler)
		},
	}},
}

func TestServerOptionsBufconnAuthAndTrailer(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := &testServer{claims: make(chan auth.Claims, 1)}
	policy, err := middleware.NewRegistry(middleware.MethodRule{Method: testMethod, Kind: middleware.MethodAuthenticated})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(ServerOptions(AuthOptions{Claims: auth.DefaultOptions()}, policy, nil, validator)...)
	grpcServer.RegisterService(&testServiceDesc, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(auth.HeaderUserID, "member-1", auth.HeaderUserRole, "customer"))
	if err := conn.Invoke(ctx, testMethod, &emptypb.Empty{}, &emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	select {
	case claims := <-server.claims:
		if claims.UserID != "member-1" || claims.Role != auth.RoleCustomer {
			t.Fatalf("unexpected claims: %#v", claims)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not receive claims")
	}

	server.err = commonerrors.New(commonerrors.CategoryValidation, "INVALID_TEST", "invalid test")
	var trailers metadata.MD
	err = conn.Invoke(ctx, testMethod, &emptypb.Empty{}, &emptypb.Empty{}, grpc.Trailer(&trailers))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unexpected error: %v", err)
	}
	if values := trailers.Get(commonerrors.TrailerErrorCode); len(values) != 1 || values[0] != "INVALID_TEST" {
		t.Fatalf("unexpected trailers: %#v", trailers)
	}
}
