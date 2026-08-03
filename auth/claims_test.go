package auth

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestParseMetadataNormalizesAndAcceptsIdenticalDuplicates(t *testing.T) {
	claims, err := ParseMetadata(metadata.Pairs(
		HeaderUserID, " member-1 ", HeaderUserRole, "customer", HeaderUserRole, " CUSTOMER ",
		HeaderMembershipStatus, "active", HeaderGymID, "gym-1",
	), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if claims.UserID != "member-1" || claims.Role != RoleCustomer || claims.Membership != MembershipActive {
		t.Fatalf("unexpected claims: %#v", claims)
	}
}

func TestParseHTTPRejectsConflictingDuplicate(t *testing.T) {
	header := http.Header{}
	header.Add(HeaderUserID, "one")
	header.Add(HeaderUserID, "two")
	header.Set(HeaderUserRole, "ADMIN")
	if _, err := ParseHTTP(header, DefaultOptions()); err == nil {
		t.Fatal("expected conflicting user IDs to fail")
	}
}

func TestValidateRequirementsAndKnownMemberships(t *testing.T) {
	if _, err := (Claims{UserID: "one", Role: "future"}).Validate(DefaultOptions()); err == nil {
		t.Fatal("expected unknown role failure")
	}
	if _, err := (Claims{UserID: "one", Role: RoleAdmin, Membership: "future"}).Validate(DefaultOptions()); err == nil {
		t.Fatal("expected unknown membership failure")
	}
	claims, err := (Claims{UserID: "one", Role: "customer", Membership: "none"}).Validate(DefaultOptions())
	if err != nil || claims.Role != RoleCustomer || claims.Membership != MembershipNone {
		t.Fatalf("unexpected normalized claims: %#v, %v", claims, err)
	}
	if _, err := (Claims{UserID: "one", Role: RoleAdmin}).Validate(Options{RequireUserID: true, RequireRole: true, RequireMembership: true}); err == nil {
		t.Fatal("expected missing membership failure")
	}
}

func TestContextConcurrentAccess(t *testing.T) {
	ctx := NewContext(context.Background(), Claims{UserID: "one", Role: RoleAdmin})
	var group sync.WaitGroup
	for range 100 {
		group.Add(1)
		go func() {
			defer group.Done()
			claims, ok := FromContext(ctx)
			if !ok || claims.Role != RoleAdmin {
				t.Errorf("unexpected claims: %#v", claims)
			}
		}()
	}
	group.Wait()
}
