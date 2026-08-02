package auth

import "context"

type contextKey struct{}

// NewContext returns a context with an immutable copy of claims.
func NewContext(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, contextKey{}, claims)
}

// FromContext returns gateway claims attached to ctx.
func FromContext(ctx context.Context) (Claims, bool) {
	claims, ok := ctx.Value(contextKey{}).(Claims)
	return claims, ok
}

// MustFromContext returns gateway claims or panics when none were attached.
func MustFromContext(ctx context.Context) Claims {
	claims, ok := FromContext(ctx)
	if !ok {
		panic("auth: claims missing from context")
	}
	return claims
}
