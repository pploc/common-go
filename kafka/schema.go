package kafka

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
)

// SubjectName returns the frozen TopicNameStrategy value subject.
func SubjectName(topic string) (string, error) {
	if _, ok := frozenTopicTypes[topic]; !ok {
		return "", fmt.Errorf("kafka: topic %q is not part of the frozen v1 contract", topic)
	}
	return topic + "-value", nil
}

// SchemaResolver resolves an already-registered Protobuf schema ID and message
// indexes into a concrete generated message. Implementations must not register
// schemas in production.
type SchemaResolver interface {
	Resolve(topic string, schemaID int, indexes []int) (proto.Message, error)
}

// FramedSchemaResolver is implemented by Schema Registry adapters that decode
// the complete Confluent frame using the official registry serde. Decode uses
// it in preference to the lower-level resolver method retained for isolated
// unit-test seams.
type FramedSchemaResolver interface {
	SchemaResolver
	DecodeFrame(topic string, framedValue []byte) (proto.Message, error)
}

// FrameEncoder produces the complete Confluent-framed value for a schema that
// has already been registered. Implementations must not register schemas.
type FrameEncoder interface {
	Encode(topic string, message proto.Message) ([]byte, error)
}

// ValidateEvent verifies the public producer request before a transport builds
// canonical headers or sends bytes to Kafka.
func ValidateEvent(event Event) error {
	if err := validateFrozenPair(event.Topic, event.Payload); err != nil {
		return err
	}
	if strings.TrimSpace(event.Source) == "" || strings.TrimSpace(event.EventID) == "" {
		return fmt.Errorf("kafka: source and event ID are required")
	}
	return nil
}

var frozenTopicTypes = map[string]string{
	"identity.user.registered.v1":              "events.v1.UserRegisteredEvent",
	"identity.user.suspended.v1":               "events.v1.UserSuspendedEvent",
	"identity.user.role-changed.v1":            "events.v1.UserRoleChangedEvent",
	"identity.email.verification-requested.v1": "events.v1.EmailVerificationRequestedEvent",
	"payment.completed.v1":                     "events.v1.PaymentCompletedEvent",
	"membership.activated.v1":                  "events.v1.MembershipActivatedEvent",
	"membership.paused.v1":                     "events.v1.MembershipPausedEvent",
	"membership.resumed.v1":                    "events.v1.MembershipResumedEvent",
	"membership.expiring-soon.v1":              "events.v1.MembershipExpiringSoonEvent",
	"membership.expired.v1":                    "events.v1.MembershipExpiredEvent",
}

func validateFrozenPair(topic string, message proto.Message) error {
	if message == nil || !message.ProtoReflect().IsValid() {
		return fmt.Errorf("kafka: a concrete protobuf message is required")
	}
	expected, ok := frozenTopicTypes[topic]
	if !ok || string(message.ProtoReflect().Descriptor().FullName()) != expected {
		return fmt.Errorf("kafka: topic and concrete protobuf type are not a frozen contract pair")
	}
	return nil
}

func headerString(headers []Header, key string) (string, bool) {
	for _, header := range headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value), true
		}
	}
	return "", false
}
