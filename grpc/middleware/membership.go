// Package middleware provides explicit gRPC authorization policies.
package middleware

import (
	"context"
	"fmt"

	"github.com/pploc/common-go/auth"
	commonerrors "github.com/pploc/common-go/errors"
)

// Policy authorizes a request using its full gRPC method and gateway claims.
type Policy interface {
	Authorize(context.Context, string, auth.Claims) error
}

type policyFunc func(context.Context, string, auth.Claims) error

func (fn policyFunc) Authorize(ctx context.Context, method string, claims auth.Claims) error {
	return fn(ctx, method, claims)
}

// RequireRoles permits calls only for one of roles.
func RequireRoles(roles ...auth.Role) Policy {
	allowed := make(map[auth.Role]struct{}, len(roles))
	for _, role := range roles {
		allowed[role] = struct{}{}
	}
	return policyFunc(func(_ context.Context, _ string, claims auth.Claims) error {
		if _, ok := allowed[claims.Role]; !ok {
			return commonerrors.New(commonerrors.CategoryForbidden, "ROLE_FORBIDDEN", "role is not authorized")
		}
		return nil
	})
}

// RequireActiveMembership permits calls only for explicitly active membership.
// Do not enable it until the gateway has evidence for header stripping/injection.
func RequireActiveMembership() Policy {
	return policyFunc(func(_ context.Context, _ string, claims auth.Claims) error {
		if claims.Membership != auth.MembershipActive {
			return commonerrors.New(commonerrors.CategoryForbidden, "MEMBERSHIP_REQUIRED", "active membership is required")
		}
		return nil
	})
}

// PublicMethods bypasses its wrapped policy only for exact gRPC full method
// names, such as /package.Service/Method.
func PublicMethods(methods []string, next Policy) Policy {
	allowed := make(map[string]struct{}, len(methods))
	for _, method := range methods {
		allowed[method] = struct{}{}
	}
	return policyFunc(func(ctx context.Context, method string, claims auth.Claims) error {
		if _, ok := allowed[method]; ok {
			return nil
		}
		if next == nil {
			return fmt.Errorf("middleware: no authorization policy configured")
		}
		return next.Authorize(ctx, method, claims)
	})
}

// Combine applies policies in order and stops at the first denial.
func Combine(policies ...Policy) Policy {
	return policyFunc(func(ctx context.Context, method string, claims auth.Claims) error {
		for _, policy := range policies {
			if policy == nil {
				continue
			}
			if err := policy.Authorize(ctx, method, claims); err != nil {
				return err
			}
		}
		return nil
	})
}
