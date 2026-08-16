package kafka

import (
	"strings"
	"testing"

	eventsv1 "github.com/pploc/proto-go/events/v1"
)

func TestGivenBlankSchemaRegistryURL_WhenCreatingRegistry_ThenRejectsConfiguration(t *testing.T) {
	// Given
	config := RegistryConfig{URL: " \t "}

	// When
	registry, err := NewConfluentProtobufRegistry(config)

	// Then
	if registry != nil {
		t.Fatal("registry = non-nil, want nil")
	}
	if err == nil || !strings.Contains(err.Error(), "Schema Registry URL is required") {
		t.Fatalf("error = %v, want missing URL validation error", err)
	}
}

func TestGivenRegistry_WhenResolvingFrozenAndUnknownTopics_ThenReturnsExpectedConcreteMessage(t *testing.T) {
	// Given
	registry, err := NewConfluentProtobufRegistry(RegistryConfig{URL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := registry.Close(); closeErr != nil {
			t.Errorf("close registry: %v", closeErr)
		}
	})

	// When
	message, resolveErr := registry.Resolve("identity.user.registered.v1", 0, nil)
	checkInMessage, checkInResolveErr := registry.Resolve("checkin.recorded.v1", 0, nil)
	_, unknownErr := registry.Resolve("unknown.topic.v1", 0, nil)

	// Then
	if resolveErr != nil {
		t.Fatalf("resolve frozen topic: %v", resolveErr)
	}
	if _, ok := message.(*eventsv1.UserRegisteredEvent); !ok {
		t.Fatalf("resolved message = %T, want *eventsv1.UserRegisteredEvent", message)
	}
	if checkInResolveErr != nil {
		t.Fatalf("resolve Check-in topic: %v", checkInResolveErr)
	}
	if _, ok := checkInMessage.(*eventsv1.CheckInRecordedEvent); !ok {
		t.Fatalf("resolved Check-in message = %T, want *eventsv1.CheckInRecordedEvent", checkInMessage)
	}
	if unknownErr == nil || !IsPermanent(unknownErr) {
		t.Fatalf("unknown topic error = %v, want permanent error", unknownErr)
	}
}

func TestGivenRegistry_WhenEncodingUnknownTopic_ThenRejectsBeforeSchemaRegistryRequest(t *testing.T) {
	// Given
	registry, err := NewConfluentProtobufRegistry(RegistryConfig{URL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := registry.Close(); closeErr != nil {
			t.Errorf("close registry: %v", closeErr)
		}
	})

	// When
	frame, encodeErr := registry.Encode("unknown.topic.v1", &eventsv1.UserRegisteredEvent{})

	// Then
	if frame != nil {
		t.Fatalf("frame = %x, want nil", frame)
	}
	if encodeErr == nil || !strings.Contains(encodeErr.Error(), "frozen contract pair") {
		t.Fatalf("error = %v, want frozen topic validation error", encodeErr)
	}
}

func TestGivenNilRegistry_WhenClosing_ThenSucceeds(t *testing.T) {
	// Given
	var registry *ConfluentProtobufRegistry

	// When
	err := registry.Close()

	// Then
	if err != nil {
		t.Fatalf("close nil registry: %v", err)
	}
}
