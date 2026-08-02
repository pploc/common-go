package auth

import (
	"fmt"
	"strings"
)

// Role is the normalized application role injected by the gateway.
type Role string

const (
	RoleCustomer   Role = "CUSTOMER"
	RoleTrainer    Role = "TRAINER"
	RoleAdmin      Role = "ADMIN"
	RoleSuperAdmin Role = "SUPER_ADMIN"
)

// MembershipStatus is the normalized membership state injected by the gateway.
type MembershipStatus string

const (
	MembershipActive  MembershipStatus = "ACTIVE"
	MembershipPaused  MembershipStatus = "PAUSED"
	MembershipExpired MembershipStatus = "EXPIRED"
)

// Claims are the trusted values injected by Kong after JWT validation.
// Their trust depends on Kong stripping client-provided copies before injection.
type Claims struct {
	UserID     string
	Role       Role
	GymID      string
	Membership MembershipStatus
	TraceID    string
}

// Options controls which injected claims are required and whether unknown values
// are accepted for forward compatibility.
type Options struct {
	RequireUserID          bool
	RequireRole            bool
	RequireGymID           bool
	RequireMembership      bool
	AllowUnknownRole       bool
	AllowUnknownMembership bool
}

// DefaultOptions requires the claims needed for an authenticated request.
func DefaultOptions() Options {
	return Options{RequireUserID: true, RequireRole: true}
}

// Validate normalizes and validates a copy of claims according to options.
func (c Claims) Validate(options Options) (Claims, error) {
	c.UserID = strings.TrimSpace(c.UserID)
	c.GymID = strings.TrimSpace(c.GymID)
	c.TraceID = strings.TrimSpace(c.TraceID)
	c.Role = Role(strings.ToUpper(strings.TrimSpace(string(c.Role))))
	c.Membership = MembershipStatus(strings.ToUpper(strings.TrimSpace(string(c.Membership))))

	if options.RequireUserID && c.UserID == "" {
		return Claims{}, fmt.Errorf("auth: %s is required", HeaderUserID)
	}
	if options.RequireRole && c.Role == "" {
		return Claims{}, fmt.Errorf("auth: %s is required", HeaderUserRole)
	}
	if options.RequireGymID && c.GymID == "" {
		return Claims{}, fmt.Errorf("auth: %s is required", HeaderGymID)
	}
	if options.RequireMembership && c.Membership == "" {
		return Claims{}, fmt.Errorf("auth: %s is required", HeaderMembershipStatus)
	}
	if c.Role != "" && !isKnownRole(c.Role) && !options.AllowUnknownRole {
		return Claims{}, fmt.Errorf("auth: unsupported role %q", c.Role)
	}
	if c.Membership != "" && !isKnownMembership(c.Membership) && !options.AllowUnknownMembership {
		return Claims{}, fmt.Errorf("auth: unsupported membership status %q", c.Membership)
	}
	return c, nil
}

func isKnownRole(role Role) bool {
	switch role {
	case RoleCustomer, RoleTrainer, RoleAdmin, RoleSuperAdmin:
		return true
	default:
		return false
	}
}

func isKnownMembership(status MembershipStatus) bool {
	switch status {
	case MembershipActive, MembershipPaused, MembershipExpired:
		return true
	default:
		return false
	}
}
