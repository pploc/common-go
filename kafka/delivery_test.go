package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func validUserRegisteredFrame() []byte {
	payload, err := proto.Marshal(validUserRegisteredEvent())
	if err != nil {
		panic(err)
	}
	return confluentFrame(1, []int{0}, payload)
}

type recordingRawPublisher struct {
	records []RawRecord
	err     error
}

func (p *recordingRawPublisher) PublishRaw(_ context.Context, record RawRecord) error {
	p.records = append(p.records, record)
	return p.err
}

func TestGivenSuccessfulHandler_WhenProcessingDelivery_ThenCommitsWithoutPublishingDLQ(t *testing.T) {
	// Given
	publisher := &recordingRawPublisher{}
	commits := 0
	delivery := Delivery{
		Resolver: &recordingResolver{message: validUserRegisteredEvent()},
		Handler:  func(context.Context, DecodedRecord) error { return nil },
		DLQ:      publisher,
		Commit: func(context.Context, RawRecord) error {
			commits++
			return nil
		},
	}

	// When
	err := delivery.Process(context.Background(), testRecord(validUserRegisteredFrame()))

	// Then
	if err != nil {
		t.Fatalf("process delivery: %v", err)
	}
	if commits != 1 || len(publisher.records) != 0 {
		t.Fatalf("commits=%d dlq=%d, want one commit and no DLQ record", commits, len(publisher.records))
	}
}

func TestGivenRetryableHandlerFailure_WhenRetriesExhaust_ThenPublishesDLQBeforeCommitting(t *testing.T) {
	// Given
	sleeper := &recordingSleeper{}
	publisher := &recordingRawPublisher{}
	attempts := 0
	commits := 0
	delivery := Delivery{
		Resolver: &recordingResolver{message: validUserRegisteredEvent()},
		Handler: func(context.Context, DecodedRecord) error {
			attempts++
			return errors.New("temporary")
		},
		DLQ:     publisher,
		Sleeper: sleeper,
		Clock:   func() time.Time { return time.UnixMilli(1_700_000_000_123) },
		Commit: func(context.Context, RawRecord) error {
			if len(publisher.records) != 1 {
				return errors.New("source committed before DLQ acknowledgement")
			}
			commits++
			return nil
		},
	}

	// When
	err := delivery.Process(context.Background(), testRecord(validUserRegisteredFrame()))

	// Then
	if err != nil {
		t.Fatalf("process delivery: %v", err)
	}
	if attempts != 4 || commits != 1 || len(publisher.records) != 1 {
		t.Fatalf("attempts=%d commits=%d dlq=%d", attempts, commits, len(publisher.records))
	}
	if got := headerValue(publisher.records[0].Headers, HeaderRetryCount); got != "4" {
		t.Fatalf("DLQ retry count=%q, want 4", got)
	}
	if len(sleeper.delays) != 3 {
		t.Fatalf("backoff count=%d, want 3", len(sleeper.delays))
	}
}

func TestGivenCanceledContext_WhenProcessingDelivery_ThenLeavesSourceUncommittedWithoutPublishingDLQ(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	publisher := &recordingRawPublisher{}
	commits := 0
	delivery := Delivery{
		Resolver: &recordingResolver{message: validUserRegisteredEvent()},
		Handler:  func(context.Context, DecodedRecord) error { return ctx.Err() },
		DLQ:      publisher,
		Commit: func(context.Context, RawRecord) error {
			commits++
			return nil
		},
	}

	// When
	err := delivery.Process(ctx, testRecord(validUserRegisteredFrame()))

	// Then
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if commits != 0 || len(publisher.records) != 0 {
		t.Fatalf("commits=%d dlq=%d, want no commit and no DLQ record", commits, len(publisher.records))
	}
}

func TestGivenPermanentFailure_WhenDLQPublicationFails_ThenLeavesSourceUncommitted(t *testing.T) {
	// Given
	publisher := &recordingRawPublisher{err: errors.New("DLQ unavailable")}
	commits := 0
	delivery := Delivery{
		Resolver: &recordingResolver{message: validUserRegisteredEvent()},
		Handler:  func(context.Context, DecodedRecord) error { return Permanent{Err: errors.New("bad input")} },
		DLQ:      publisher,
		Commit: func(context.Context, RawRecord) error {
			commits++
			return nil
		},
	}

	// When
	err := delivery.Process(context.Background(), testRecord(validUserRegisteredFrame()))

	// Then
	if err == nil {
		t.Fatal("expected DLQ publication failure")
	}
	if commits != 0 {
		t.Fatalf("commits=%d, want 0 after failed DLQ publication", commits)
	}
}
