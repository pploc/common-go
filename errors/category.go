// Package errors defines client-safe domain errors and gRPC conversion.
package errors

// Category classifies a domain failure for transport conversion.
type Category string

const (
	CategoryValidation    Category = "VALIDATION"
	CategoryUnauthorized  Category = "UNAUTHORIZED"
	CategoryForbidden     Category = "FORBIDDEN"
	CategoryNotFound      Category = "NOT_FOUND"
	CategoryConflict      Category = "CONFLICT"
	CategoryUnsupported   Category = "UNSUPPORTED"
	CategoryUnprocessable Category = "UNPROCESSABLE"
	CategoryRateLimited   Category = "RATE_LIMITED"
	CategoryUnavailable   Category = "UNAVAILABLE"
	CategoryInternal      Category = "INTERNAL"
)

// IsKnown reports whether category is part of the versioned error contract.
func (c Category) IsKnown() bool {
	switch c {
	case CategoryValidation, CategoryUnauthorized, CategoryForbidden, CategoryNotFound,
		CategoryConflict, CategoryUnsupported, CategoryUnprocessable, CategoryRateLimited,
		CategoryUnavailable, CategoryInternal:
		return true
	default:
		return false
	}
}
