package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	commonv1 "github.com/pploc/proto-go/common/v1"
	eventsv1 "github.com/pploc/proto-go/events/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type recordingResolver struct {
	message proto.Message
	err     error
	topic   string
	schema  int
	indexes []int
}

func (r *recordingResolver) Resolve(topic string, schemaID int, indexes []int) (proto.Message, error) {
	r.topic = topic
	r.schema = schemaID
	r.indexes = append([]int(nil), indexes...)
	if r.err != nil {
		return nil, r.err
	}
	return r.message, nil
}

func validUserRegisteredEvent() *eventsv1.UserRegisteredEvent {
	return &eventsv1.UserRegisteredEvent{
		UserId:       "user-001",
		Email:        "user-001@example.test",
		FullName:     "Fixture User",
		Role:         commonv1.Role_ROLE_CUSTOMER,
		AuthProvider: commonv1.AuthProvider_AUTH_PROVIDER_LOCAL,
		Timestamp:    1_700_000_000_123,
	}
}


func TestGivenValidConfluentRecord_WhenDecoding_ThenReturnsConcreteMessageAndPreservesRawRecord(t *testing.T) {
	// Given
	payload, err := proto.Marshal(validUserRegisteredEvent())
	if err != nil {
		t.Fatalf("marshal protobuf payload: %v", err)
	}
	record := testRecord(confluentFrame(1, []int{1}, payload))
	resolver := &recordingResolver{message: validUserRegisteredEvent()}

	// When
	decoded, err := Decode(record, resolver)

	// Then
	if err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if _, ok := decoded.Message.(*eventsv1.UserRegisteredEvent); !ok {
		t.Fatalf("message = %T, want *events.v1.UserRegisteredEvent", decoded.Message)
	}
	if resolver.topic != record.Topic || resolver.schema != 1 || !reflect.DeepEqual(resolver.indexes, []int{1}) {
		t.Fatalf("resolver call = topic=%q schema=%d indexes=%v", resolver.topic, resolver.schema, resolver.indexes)
	}
	if !bytes.Equal(decoded.Key, record.Key) || !bytes.Equal(decoded.Value, record.Value) {
		t.Fatal("decoded record did not preserve raw key or frame")
	}
}

func TestGivenMutableRawRecord_WhenDecoding_ThenReturnsDefensiveRawCopy(t *testing.T) {
	// Given
	payload, err := proto.Marshal(validUserRegisteredEvent())
	if err != nil {
		t.Fatalf("marshal protobuf payload: %v", err)
	}
	record := testRecord(confluentFrame(1, []int{0}, payload))
	resolver := &recordingResolver{message: validUserRegisteredEvent()}

	// When
	decoded, err := Decode(record, resolver)
	record.Key[0] = 'x'
	record.Value[0] = 99
	record.Headers[0].Value[0] = 'x'

	// Then
	if err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if decoded.Key[0] == 'x' || decoded.Value[0] == 99 || decoded.Headers[0].Value[0] == 'x' {
		t.Fatal("decoded record exposes mutable source bytes")
	}
}

func TestGivenInvalidFrameOrHeaders_WhenDecoding_ThenReturnsPermanentFailure(t *testing.T) {
	tests := map[string]func(RawRecord) RawRecord{
		"truncated frame":    func(record RawRecord) RawRecord { record.Value = []byte{0, 0, 0, 0, 1}; return record },
		"invalid magic byte": func(record RawRecord) RawRecord { record.Value[0] = 1; return record },
		"zero schema ID": func(record RawRecord) RawRecord {
			record.Value[1] = 0
			record.Value[2] = 0
			record.Value[3] = 0
			record.Value[4] = 0
			return record
		},
		"truncated index":         func(record RawRecord) RawRecord { record.Value = []byte{0, 0, 0, 0, 1, 0x80}; return record },
		"negative index count":    func(record RawRecord) RawRecord { record.Value = []byte{0, 0, 0, 0, 1, 0x01}; return record },
		"missing required header": func(record RawRecord) RawRecord { record.Headers = record.Headers[1:]; return record },
		"invalid timestamp":       func(record RawRecord) RawRecord { record.Headers[2].Value = []byte("-1"); return record },
		"invalid traceparent":     func(record RawRecord) RawRecord { record.Headers[4].Value = []byte("invalid"); return record },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			// Given
			record := mutate(testRecord(confluentFrame(1, []int{0}, nil)))

			// When
			_, err := Decode(record, &recordingResolver{message: validUserRegisteredEvent()})

			// Then
			if err == nil || !IsPermanent(err) {
				t.Fatalf("error = %v, want permanent failure", err)
			}
		})
	}
}

func TestGivenResolverOrDescriptorFailure_WhenDecoding_ThenClassifiesFailureCorrectly(t *testing.T) {
	tests := []struct {
		name     string
		resolver *recordingResolver
		wantPerm bool
	}{
		{name: "retryable resolver failure", resolver: &recordingResolver{err: errors.New("registry unavailable")}},
		{name: "wrong frozen descriptor", resolver: &recordingResolver{message: &eventsv1.UserSuspendedEvent{}}, wantPerm: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			record := testRecord(confluentFrame(1, []int{0}, nil))

			// When
			_, err := Decode(record, test.resolver)

			// Then
			if err == nil || IsPermanent(err) != test.wantPerm {
				t.Fatalf("error = %v, permanent = %t, want permanent = %t", err, IsPermanent(err), test.wantPerm)
			}
		})
	}
}

func TestGivenEventTypeMismatchOrInvalidPayload_WhenDecoding_ThenReturnsPermanentFailure(t *testing.T) {
	tests := map[string]RawRecord{
		"event type mismatch": func() RawRecord {
			record := testRecord(confluentFrame(1, []int{0}, nil))
			record.Headers[0].Value = []byte("events.v1.UserSuspendedEvent")
			return record
		}(),
		"invalid protobuf payload": testRecord(confluentFrame(1, []int{0}, []byte{0x0a, 0x02, 0x01})),
	}

	for name, record := range tests {
		t.Run(name, func(t *testing.T) {
			// Given
			resolver := &recordingResolver{message: validUserRegisteredEvent()}

			// When
			_, err := Decode(record, resolver)

			// Then
			if err == nil || !IsPermanent(err) {
				t.Fatalf("error = %v, want permanent failure", err)
			}
		})
	}
}

func TestGivenUnknownTopic_WhenResolvingSubject_ThenRejectsIt(t *testing.T) {
	// Given
	const knownTopic = "identity.user.registered.v1"

	// When
	subject, err := SubjectName(knownTopic)
	_, unknownErr := SubjectName("unknown.topic.v1")

	// Then
	if err != nil || subject != knownTopic+"-value" {
		t.Fatalf("subject = %q, error = %v", subject, err)
	}
	if unknownErr == nil {
		t.Fatal("unknown topic was accepted")
	}
}

func testRecord(frame []byte) RawRecord {
	return RawRecord{
		Topic: "identity.user.registered.v1",
		Key:   []byte("user-1"),
		Value: frame,
		Headers: []Header{
			{Key: HeaderEventType, Value: []byte("events.v1.UserRegisteredEvent")},
			{Key: HeaderSource, Value: []byte("ms-gym-identifier")},
			{Key: HeaderTimestamp, Value: []byte("1700000000123")},
			{Key: HeaderEventID, Value: []byte("event-1")},
			{Key: HeaderTraceParent, Value: []byte("00-00000000000000000000000000000001-0000000000000001-01")},
		},
	}
}

func confluentFrame(schemaID int, indexes []int, payload []byte) []byte {
	frame := make([]byte, 5)
	binary.BigEndian.PutUint32(frame[1:], uint32(schemaID))
	if len(indexes) == 1 && indexes[0] == 0 {
		frame = append(frame, 0)
	} else {
		frame = protowire.AppendVarint(frame, protowire.EncodeZigZag(int64(len(indexes))))
		for _, index := range indexes {
			frame = protowire.AppendVarint(frame, protowire.EncodeZigZag(int64(index)))
		}
	}
	return append(frame, payload...)
}
