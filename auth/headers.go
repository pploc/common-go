// Package auth extracts gateway-injected request claims.
package auth

const (
	HeaderUserID           = "x-user-id"
	HeaderUserRole         = "x-user-role"
	HeaderGymID            = "x-gym-id"
	HeaderMembershipStatus = "x-membership-status"
	HeaderTraceID          = "x-trace-id"
)
