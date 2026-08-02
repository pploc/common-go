package errors

import (
	stderrors "errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCMappings(t *testing.T) {
	cases := []struct {
		category Category
		want     codes.Code
	}{
		{CategoryValidation, codes.InvalidArgument}, {CategoryUnauthorized, codes.Unauthenticated},
		{CategoryForbidden, codes.PermissionDenied}, {CategoryNotFound, codes.NotFound},
		{CategoryConflict, codes.AlreadyExists}, {CategoryUnsupported, codes.Unimplemented},
		{CategoryUnprocessable, codes.FailedPrecondition}, {CategoryRateLimited, codes.ResourceExhausted},
		{CategoryUnavailable, codes.Unavailable},
	}
	for _, test := range cases {
		t.Run(string(test.category), func(t *testing.T) {
			mapped := ToGRPC(New(test.category, "APP_CODE", "safe message"))
			if status.Code(mapped.Err) != test.want || mapped.Code != "APP_CODE" {
				t.Fatalf("unexpected mapping: %#v", mapped)
			}
		})
	}
}

func TestToGRPCRedactsAndPreservesStatuses(t *testing.T) {
	internal := ToGRPC(New(CategoryInternal, "INTERNAL", "secret details"))
	if status.Code(internal.Err) != codes.Internal || status.Convert(internal.Err).Message() != InternalDescription {
		t.Fatalf("internal error leaked: %v", internal.Err)
	}
	unknown := ToGRPC(stderrors.New("secret details"))
	if status.Code(unknown.Err) != codes.Internal || status.Convert(unknown.Err).Message() != InternalDescription {
		t.Fatalf("unknown error leaked: %v", unknown.Err)
	}
	existing := status.Error(codes.Aborted, "preserve")
	if ToGRPC(existing).Err != existing {
		t.Fatal("existing status error was not preserved")
	}
}

func TestWrapPreservesCause(t *testing.T) {
	cause := stderrors.New("cause")
	wrapped := Wrap(cause, CategoryValidation, "INVALID_NAME", "invalid name")
	if !stderrors.Is(wrapped, cause) {
		t.Fatal("wrapped cause is not discoverable")
	}
}
