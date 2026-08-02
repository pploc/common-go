package auth

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

// ParseMetadata extracts and validates claims from gRPC metadata. Repeated
// values must be identical after normalization; conflicts are rejected.
func ParseMetadata(md metadata.MD, options Options) (Claims, error) {
	return parse(valuesFromMetadata(md), options)
}

func valuesFromMetadata(md metadata.MD) func(string) []string {
	return func(key string) []string { return md.Get(key) }
}

func parse(values func(string) []string, options Options) (Claims, error) {
	userID, err := oneValue(values(HeaderUserID), HeaderUserID, false)
	if err != nil {
		return Claims{}, err
	}
	role, err := oneValue(values(HeaderUserRole), HeaderUserRole, true)
	if err != nil {
		return Claims{}, err
	}
	gymID, err := oneValue(values(HeaderGymID), HeaderGymID, false)
	if err != nil {
		return Claims{}, err
	}
	membership, err := oneValue(values(HeaderMembershipStatus), HeaderMembershipStatus, true)
	if err != nil {
		return Claims{}, err
	}
	traceID, err := oneValue(values(HeaderTraceID), HeaderTraceID, false)
	if err != nil {
		return Claims{}, err
	}
	return (Claims{
		UserID: userID, Role: Role(role), GymID: gymID,
		Membership: MembershipStatus(membership), TraceID: traceID,
	}).Validate(options)
}

func oneValue(values []string, header string, upper bool) (string, error) {
	var result string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if upper {
			value = strings.ToUpper(value)
		}
		if value == "" {
			return "", fmt.Errorf("auth: %s contains an empty value", header)
		}
		if result != "" && result != value {
			return "", fmt.Errorf("auth: %s contains conflicting values", header)
		}
		result = value
	}
	return result, nil
}
