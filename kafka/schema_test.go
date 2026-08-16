package kafka

import (
	"strings"
	"testing"
	"time"

	eventsv1 "github.com/pploc/proto-go/events/v1"
	"google.golang.org/protobuf/proto"
)

func TestGivenNilSchemaResolver_WhenDecoding_ThenReturnsPermanentFailure(t *testing.T) {
	// Given
	record := testRecord(confluentFrame(1, []int{0}, nil))

	// When
	_, err := Decode(record, nil)

	// Then
	if err == nil || !IsPermanent(err) {
		t.Fatalf("error = %v, want permanent failure", err)
	}
}

func TestGivenFrozenTopicTypePairs_WhenValidatingMessages_ThenAcceptsOnlyMatchingGeneratedType(t *testing.T) {
	tests := []struct {
		topic   string
		message proto.Message
	}{
		{"identity.user.registered.v1", &eventsv1.UserRegisteredEvent{}},
		{"identity.user.suspended.v1", &eventsv1.UserSuspendedEvent{}},
		{"identity.user.role-changed.v1", &eventsv1.UserRoleChangedEvent{}},
		{"identity.email.verification-requested.v1", &eventsv1.EmailVerificationRequestedEvent{}},
		{"payment.completed.v1", &eventsv1.PaymentCompletedEvent{}},
		{"membership.activated.v1", &eventsv1.MembershipActivatedEvent{}},
		{"membership.paused.v1", &eventsv1.MembershipPausedEvent{}},
		{"membership.resumed.v1", &eventsv1.MembershipResumedEvent{}},
		{"membership.expiring-soon.v1", &eventsv1.MembershipExpiringSoonEvent{}},
		{"membership.expired.v1", &eventsv1.MembershipExpiredEvent{}},
		{"checkin.recorded.v1", &eventsv1.CheckInRecordedEvent{}},
	}

	for _, test := range tests {
		t.Run(test.topic, func(t *testing.T) {
			// Given
			message := test.message

			// When
			err := validateFrozenPair(test.topic, message)

			// Then
			if err != nil {
				t.Fatalf("validate frozen pair: %v", err)
			}
		})
	}

	// Given
	wrongTopic := "identity.user.registered.v1"
	wrongMessage := &eventsv1.UserSuspendedEvent{}

	// When
	err := validateFrozenPair(wrongTopic, wrongMessage)

	// Then
	if err == nil {
		t.Fatal("wrong frozen topic/type pair was accepted")
	}
}

func TestGivenCaseVariantDlqHeaders_WhenCreatingDLQRecord_ThenReplacesThemWithoutDuplicates(t *testing.T) {
	// Given
	record := RawRecord{
		Topic: "identity.user.registered.v1",
		Headers: []Header{
			{Key: "X-ORIGINAL-TOPIC", Value: []byte("old")},
			{Key: "X-EXCEPTION-MESSAGE", Value: []byte("old")},
			{Key: "X-FAILED-AT", Value: []byte("old")},
			{Key: "X-RETRY-COUNT", Value: []byte("old")},
		},
	}

	// When
	dlq := DLQRecord(record, "failed", 4, time.UnixMilli(1_700_000_000_123))

	// Then
	for _, key := range []string{HeaderOriginalTopic, HeaderExceptionMessage, HeaderFailedAt, HeaderRetryCount} {
		count := 0
		for _, header := range dlq.Headers {
			if strings.EqualFold(header.Key, key) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("header %q count = %d, want 1", key, count)
		}
	}
}
