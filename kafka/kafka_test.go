package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	eventsv1 "github.com/pploc/proto-go/events/v1"
)

type recordingSleeper struct{ delays []time.Duration }

func (s *recordingSleeper) Sleep(_ context.Context, delay time.Duration) error {
	s.delays = append(s.delays, delay)
	return nil
}

func TestGivenRetryableFailure_WhenRetriesExhaust_ThenUsesFrozenSchedule(t *testing.T) {
	sleeper := &recordingSleeper{}
	attempts, err := Retry(context.Background(), sleeper, func() error { return errors.New("retry") })
	if err == nil || attempts != 4 {
		t.Fatalf("attempts=%d err=%v, want four failed attempts", attempts, err)
	}
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}
	if len(sleeper.delays) != len(want) {
		t.Fatalf("delays=%v, want %v", sleeper.delays, want)
	}
	for index := range want {
		if sleeper.delays[index] != want[index] {
			t.Fatalf("delay %d = %v, want %v", index, sleeper.delays[index], want[index])
		}
	}
}

func TestGivenPermanentFailure_WhenRetrying_ThenSkipsBackoff(t *testing.T) {
	sleeper := &recordingSleeper{}
	attempts, err := Retry(context.Background(), sleeper, func() error { return Permanent{Err: errors.New("bad frame")} })
	if err == nil || attempts != 1 || len(sleeper.delays) != 0 {
		t.Fatalf("attempts=%d delays=%v err=%v", attempts, sleeper.delays, err)
	}
}

func TestGivenRawRecord_WhenCreatingDLQRecord_ThenPreservesBytesAndReplacesContractHeaders(t *testing.T) {
	raw := RawRecord{
		Topic: "identity.user.registered.v1", Key: []byte("user-1"), Value: []byte{0, 1, 2},
		Headers: []Header{{Key: "event-id", Value: []byte("event-1")}, {Key: HeaderRetryCount, Value: []byte("0")}},
	}
	dlq := DLQRecord(raw, "MalformedFrame", 4, time.UnixMilli(1234))
	if dlq.Topic != raw.Topic+".DLQ" || string(dlq.Key) != string(raw.Key) || string(dlq.Value) != string(raw.Value) {
		t.Fatalf("DLQ did not preserve raw record: %#v", dlq)
	}
	if got := headerValue(dlq.Headers, HeaderRetryCount); got != "4" {
		t.Fatalf("retry count=%q, want 4", got)
	}
	if got := headerValue(dlq.Headers, HeaderOriginalTopic); got != raw.Topic {
		t.Fatalf("original topic=%q", got)
	}
}

func TestGivenCanonicalMetadata_WhenBuildingHeaders_ThenIncludesW3CAndRejectsOverrides(t *testing.T) {
	headers, err := CanonicalHeaders(context.Background(), &eventsv1.UserRegisteredEvent{}, "ms-gym-identifier", "event-1", time.UnixMilli(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCanonicalHeaders(headers); err != nil {
		t.Fatal(err)
	}
	if _, err := CanonicalHeaders(context.Background(), &eventsv1.UserRegisteredEvent{}, "source", "id", time.Now(), []Header{{Key: "EVENT-ID", Value: []byte("override")}}); err == nil {
		t.Fatal("expected canonical header override rejection")
	}
}

func headerValue(headers []Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
