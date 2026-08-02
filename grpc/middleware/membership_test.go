package middleware

import (
	"context"
	"testing"

	"github.com/pploc/common-go/auth"
	commonerrors "github.com/pploc/common-go/errors"
)

func TestPolicies(t *testing.T) {
	claims := auth.Claims{Role: auth.RoleCustomer, Membership: auth.MembershipActive}
	if err := Combine(RequireRoles(auth.RoleCustomer), RequireActiveMembership()).Authorize(context.Background(), "/gym.Service/Book", claims); err != nil {
		t.Fatal(err)
	}
	err := RequireActiveMembership().Authorize(context.Background(), "/gym.Service/Book", auth.Claims{Membership: auth.MembershipPaused})
	if err == nil {
		t.Fatal("expected membership policy to reject paused membership")
	}
	if domain, ok := err.(*commonerrors.Error); !ok || domain.Code != "MEMBERSHIP_REQUIRED" {
		t.Fatalf("unexpected membership error: %#v", err)
	}
}

func TestPublicMethodsRequiresExactMatch(t *testing.T) {
	policy := PublicMethods([]string{"/gym.Service/Health"}, RequireRoles(auth.RoleAdmin))
	if err := policy.Authorize(context.Background(), "/gym.Service/Health", auth.Claims{}); err != nil {
		t.Fatal(err)
	}
	if err := policy.Authorize(context.Background(), "/gym.Service/HealthCheck", auth.Claims{}); err == nil {
		t.Fatal("expected similar non-public method to fail")
	}
}
