package errors

import (
	stderrors "errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// TrailerErrorCode is the gRPC trailer containing a stable application code.
	TrailerErrorCode = "x-error-code"
	// InternalDescription is the only client-visible message for unclassified
	// and internal failures.
	InternalDescription = "Internal server error"
)

// GRPCError contains the status error and optional application error code for a
// gRPC interceptor to emit as a trailer.
type GRPCError struct {
	Err  error
	Code string
}

// ToGRPC maps domain errors to their specified gRPC status. Existing gRPC
// status errors are returned unchanged. Unknown and internal errors are redacted.
func ToGRPC(err error) GRPCError {
	if err == nil {
		return GRPCError{}
	}
	if _, ok := status.FromError(err); ok {
		return GRPCError{Err: err}
	}

	var domain *Error
	if !stderrors.As(err, &domain) || domain == nil || !domain.Category.IsKnown() {
		return GRPCError{Err: status.Error(codes.Internal, InternalDescription)}
	}
	if domain.Category == CategoryInternal {
		return GRPCError{Err: status.Error(codes.Internal, InternalDescription), Code: domain.Code}
	}
	return GRPCError{
		Err:  status.Error(categoryCode(domain.Category), domain.Message),
		Code: domain.Code,
	}
}

func categoryCode(category Category) codes.Code {
	switch category {
	case CategoryValidation:
		return codes.InvalidArgument
	case CategoryUnauthorized:
		return codes.Unauthenticated
	case CategoryForbidden:
		return codes.PermissionDenied
	case CategoryNotFound:
		return codes.NotFound
	case CategoryConflict:
		return codes.AlreadyExists
	case CategoryUnsupported:
		return codes.Unimplemented
	case CategoryUnprocessable:
		return codes.FailedPrecondition
	case CategoryRateLimited:
		return codes.ResourceExhausted
	case CategoryUnavailable:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}
