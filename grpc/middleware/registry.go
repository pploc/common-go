package middleware

import (
	"context"
	"fmt"

	"github.com/pploc/common-go/auth"
	commonerrors "github.com/pploc/common-go/errors"
)

// MethodKind is the explicit trust policy assigned to every registered RPC.
type MethodKind string

const (
	MethodPublic           MethodKind = "PUBLIC"
	MethodAuthenticated    MethodKind = "AUTHENTICATED"
	MethodRoleRestricted   MethodKind = "ROLE_RESTRICTED"
	MethodActiveMembership MethodKind = "ACTIVE_MEMBERSHIP"
	MethodInternalWorkload MethodKind = "INTERNAL_WORKLOAD"
)

// MethodRule declares one exact gRPC full-method policy. Workload verification is
// intentionally delegated to the hosting gRPC/TLS transport; user metadata never
// proves workload identity.
type MethodRule struct {
	Method string
	Kind   MethodKind
	Roles  []auth.Role
	Policy Policy
}

// Registry is an immutable exact-method policy table. It fails closed for an
// absent method and exposes public methods to the authentication interceptor.
type Registry struct {
	rules map[string]MethodRule
}

// NewRegistry validates a complete explicit rule set.
func NewRegistry(rules ...MethodRule) (*Registry, error) {
	registry := &Registry{rules: make(map[string]MethodRule, len(rules))}
	for _, rule := range rules {
		if rule.Method == "" {
			return nil, fmt.Errorf("middleware: method policy requires a full method name")
		}
		if _, exists := registry.rules[rule.Method]; exists {
			return nil, fmt.Errorf("middleware: duplicate policy for %s", rule.Method)
		}
		switch rule.Kind {
		case MethodPublic, MethodAuthenticated, MethodRoleRestricted, MethodActiveMembership, MethodInternalWorkload:
		default:
			return nil, fmt.Errorf("middleware: unsupported policy kind %q", rule.Kind)
		}
		if rule.Kind == MethodRoleRestricted && len(rule.Roles) == 0 && rule.Policy == nil {
			return nil, fmt.Errorf("middleware: role policy for %s has no roles", rule.Method)
		}
		registry.rules[rule.Method] = rule
	}
	return registry, nil
}

// IsPublic reports whether a method has an explicit public policy.
func (r *Registry) IsPublic(method string) bool {
	return r != nil && r.rules[method].Kind == MethodPublic
}

// Authorize evaluates an exact registered method policy.
func (r *Registry) Authorize(ctx context.Context, method string, claims auth.Claims) error {
	if r == nil {
		return fmt.Errorf("middleware: no method policy registry configured")
	}
	rule, exists := r.rules[method]
	if !exists {
		return commonerrors.New(commonerrors.CategoryForbidden, "METHOD_UNCLASSIFIED", "method is not authorized")
	}
	if rule.Kind == MethodPublic || rule.Kind == MethodAuthenticated {
		return nil
	}
	if rule.Kind == MethodInternalWorkload {
		return commonerrors.New(commonerrors.CategoryForbidden, "WORKLOAD_IDENTITY_REQUIRED", "workload identity is required")
	}
	if rule.Policy != nil {
		return rule.Policy.Authorize(ctx, method, claims)
	}
	if rule.Kind == MethodRoleRestricted {
		return RequireRoles(rule.Roles...).Authorize(ctx, method, claims)
	}
	return RequireActiveMembership().Authorize(ctx, method, claims)
}
