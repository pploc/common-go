package common_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	eventsv1 "github.com/pploc/proto-go/events/v1"
	"google.golang.org/protobuf/proto"
)

type confluentFixtureDocument struct {
	FixtureFormatVersion int                    `json:"fixtureFormatVersion"`
	Cases                []confluentFixtureCase `json:"cases"`
}

type confluentFixtureCase struct {
	Name       string            `json:"name"`
	Topic      string            `json:"topic"`
	KeyUTF8    string            `json:"keyUtf8"`
	Subject    string            `json:"subject"`
	EventType  string            `json:"eventType"`
	Headers    map[string]string `json:"headers"`
	PayloadHex string            `json:"payloadHex"`
	Frame      struct {
		MagicByteHex      string `json:"magicByteHex"`
		SchemaIDBigEndian string `json:"schemaIdBigEndianHex"`
		MessageIndexesHex string `json:"messageIndexesHex"`
		CompleteHex       string `json:"completeHex"`
	} `json:"frame"`
}

func TestPublishedProtoGoConfluentFixtures(t *testing.T) {
	document := publishedConfluentFixtures(t)

	if document.FixtureFormatVersion != 1 {
		t.Fatalf("fixture format version = %d, want 1", document.FixtureFormatVersion)
	}
	if len(document.Cases) != 11 {
		t.Fatalf("fixture cases = %d, want 11", len(document.Cases))
	}
	for _, fixture := range document.Cases {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			payload, err := hex.DecodeString(fixture.PayloadHex)
			if err != nil {
				t.Fatalf("decode payload hex: %v", err)
			}
			frame, err := hex.DecodeString(fixture.Frame.CompleteHex)
			if err != nil {
				t.Fatalf("decode complete frame hex: %v", err)
			}
			if fixture.Frame.MagicByteHex != "00" || len(frame) < 6 || frame[0] != 0 {
				t.Fatal("fixture is not a Confluent-framed Protobuf value")
			}
			schemaID, err := hex.DecodeString(fixture.Frame.SchemaIDBigEndian)
			if err != nil {
				t.Fatalf("decode schema ID bytes: %v", err)
			}
			indexes, err := hex.DecodeString(fixture.Frame.MessageIndexesHex)
			if err != nil {
				t.Fatalf("decode message-index bytes: %v", err)
			}
			if len(schemaID) != 4 {
				t.Fatalf("schema ID byte length = %d, want 4", len(schemaID))
			}
			if !bytes.Equal(frame[1:5], schemaID) {
				t.Fatal("fixture frame schema ID bytes differ from schema metadata")
			}
			if !bytes.Equal(frame[5:len(frame)-len(payload)], indexes) {
				t.Fatal("fixture frame message-index bytes differ from frame metadata")
			}
			if !bytes.HasSuffix(frame, payload) {
				t.Fatal("fixture frame does not end with the raw Protobuf payload")
			}

			message := fixtureMessage(t, fixture.EventType)
			if err := proto.Unmarshal(payload, message); err != nil {
				t.Fatalf("decode fixture payload with published generated type: %v", err)
			}
			if got := string(message.ProtoReflect().Descriptor().FullName()); got != fixture.EventType {
				t.Fatalf("decoded type = %q, want %q", got, fixture.EventType)
			}
			encoded, err := proto.Marshal(message)
			if err != nil {
				t.Fatalf("re-encode fixture payload: %v", err)
			}
			if !bytes.Equal(encoded, payload) {
				t.Fatal("re-encoded published generated message differs from fixture payload")
			}
		})
	}
}

func publishedConfluentFixtures(t *testing.T) confluentFixtureDocument {
	t.Helper()
	fixturePath := filepath.Join(publishedProtoGoDir(t), "contracts", "v1", "kafka", "confluent-7.7.1-fixtures.json")
	contents, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read published fixture artifact: %v", err)
	}
	var document confluentFixtureDocument
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("decode published fixture artifact: %v", err)
	}
	return document
}

func publishedProtoGoDir(t *testing.T) string {
	t.Helper()
	output, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/pploc/proto-go").Output()
	if err != nil {
		t.Fatalf("locate proto-go module: %v", err)
	}
	moduleDir := string(bytes.TrimSpace(output))
	if moduleDir == "" {
		t.Fatal("proto-go module has no local module directory")
	}
	fixtureRel := filepath.Join("contracts", "v1", "kafka", "confluent-7.7.1-fixtures.json")
	if _, err := os.Stat(filepath.Join(moduleDir, fixtureRel)); err == nil {
		return moduleDir
	}
	// Local gen/go staging ships stubs only; fixtures stay at gym-proto repo root.
	repoRoot := filepath.Clean(filepath.Join(moduleDir, "..", ".."))
	if _, err := os.Stat(filepath.Join(repoRoot, fixtureRel)); err == nil {
		return repoRoot
	}
	t.Fatalf("confluent fixtures not found beside module %s", moduleDir)
	return ""
}

func fixtureMessage(t *testing.T, eventType string) proto.Message {
	t.Helper()
	switch eventType {
	case "events.v1.UserRegisteredEvent":
		return &eventsv1.UserRegisteredEvent{}
	case "events.v1.UserSuspendedEvent":
		return &eventsv1.UserSuspendedEvent{}
	case "events.v1.UserRoleChangedEvent":
		return &eventsv1.UserRoleChangedEvent{}
	case "events.v1.EmailVerificationRequestedEvent":
		return &eventsv1.EmailVerificationRequestedEvent{}
	case "events.v1.PaymentCompletedEvent":
		return &eventsv1.PaymentCompletedEvent{}
	case "events.v1.MembershipActivatedEvent":
		return &eventsv1.MembershipActivatedEvent{}
	case "events.v1.MembershipPausedEvent":
		return &eventsv1.MembershipPausedEvent{}
	case "events.v1.MembershipResumedEvent":
		return &eventsv1.MembershipResumedEvent{}
	case "events.v1.MembershipExpiringSoonEvent":
		return &eventsv1.MembershipExpiringSoonEvent{}
	case "events.v1.MembershipExpiredEvent":
		return &eventsv1.MembershipExpiredEvent{}
	case "events.v1.CheckInRecordedEvent":
		return &eventsv1.CheckInRecordedEvent{}
	default:
		t.Fatalf("unsupported fixture event type %q", eventType)
		return nil
	}
}
