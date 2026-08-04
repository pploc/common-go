//go:build integration

package common_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	internalkafka "github.com/pploc/common-go/internal/kafka"
	commonkafka "github.com/pploc/common-go/kafka"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const integrationTimeout = 45 * time.Second

func TestGivenSeededConfluentRegistry_WhenDecodingPublishedFixtures_ThenResolvesAllFrozenMessages(t *testing.T) {
	// Given
	brokers, registryURL := integrationEndpoints(t)
	_ = brokers
	registry := newIntegrationRegistry(t, registryURL)
	document := publishedConfluentFixtures(t)

	// When / Then
	for _, fixture := range document.Cases {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			frame, err := hex.DecodeString(fixture.Frame.CompleteHex)
			if err != nil {
				t.Fatalf("decode fixture frame: %v", err)
			}
			payload, err := hex.DecodeString(fixture.PayloadHex)
			if err != nil {
				t.Fatalf("decode fixture payload: %v", err)
			}
			expected := fixtureMessage(t, fixture.EventType)
			if err := proto.Unmarshal(payload, expected); err != nil {
				t.Fatalf("decode fixture payload: %v", err)
			}

			decoded, err := commonkafka.Decode(commonkafka.RawRecord{
				Topic:   fixture.Topic,
				Key:     []byte(fixture.KeyUTF8),
				Value:   frame,
				Headers: fixtureHeaders(fixture.Headers),
			}, registry)
			if err != nil {
				t.Fatalf("decode live Schema Registry frame: %v", err)
			}
			if !proto.Equal(decoded.Message, expected) {
				t.Fatalf("decoded message = %T %v, want %T %v", decoded.Message, decoded.Message, expected, expected)
			}

			encoded, err := registry.Encode(fixture.Topic, decoded.Message)
			if err != nil {
				t.Fatalf("encode through lookup-only Schema Registry: %v", err)
			}
			if !bytes.Equal(encoded, frame) {
				t.Fatal("lookup-only encode differs from complete published Confluent frame")
			}
		})
	}
}

func TestGivenSeededKafkaAndRegistry_WhenPublishingWithFranz_ThenConsumesEveryDecodedFramedRecord(t *testing.T) {
	brokers, registryURL := integrationEndpoints(t)
	registry := newIntegrationRegistry(t, registryURL)

	for _, fixture := range publishedConfluentFixtures(t).Cases {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			// Given
			message := fixtureMessage(t, fixture.EventType)
			payload, err := hex.DecodeString(fixture.PayloadHex)
			if err != nil {
				t.Fatalf("decode fixture payload: %v", err)
			}
			if err := proto.Unmarshal(payload, message); err != nil {
				t.Fatalf("decode fixture message: %v", err)
			}
			producer, err := commonkafka.NewFranzProducer(commonkafka.TransportConfig{Brokers: strings.Split(brokers, ","), PublishTimeout: integrationTimeout}, registry)
			if err != nil {
				t.Fatalf("create Franz producer: %v", err)
			}
			t.Cleanup(producer.Close)
			group := fmt.Sprintf("common-go-contract-%d", time.Now().UnixNano())
			testKey := []byte(group)
			consumer, err := commonkafka.NewFranzConsumer(commonkafka.TransportConfig{Brokers: strings.Split(brokers, ","), Topics: []string{fixture.Topic}, ConsumerGroup: group, PublishTimeout: integrationTimeout}, registry, producer, nil)
			if err != nil {
				t.Fatalf("create Franz consumer: %v", err)
			}
			t.Cleanup(consumer.Close)
			ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
			defer cancel()
			received := make(chan commonkafka.DecodedRecord, 1)
			deliveryComplete := make(chan struct{})
			barrierStarted := make(chan struct{}, 1)
			barrierKey := []byte(group + "-commit-barrier")
			runResult := make(chan error, 1)
			go func() {
				runResult <- consumer.Run(ctx, func(handlerContext context.Context, record commonkafka.DecodedRecord) error {
					switch {
					case bytes.Equal(record.Key, testKey):
						received <- record
						<-deliveryComplete
						return nil
					case bytes.Equal(record.Key, barrierKey):
						select {
						case barrierStarted <- struct{}{}:
						default:
						}
						return nil
					default:
						return nil
					}
				})
			}()

			// When
			if err := producer.Publish(ctx, commonkafka.Event{Topic: fixture.Topic, Key: testKey, Payload: message, EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource]}); err != nil {
				t.Fatalf("acknowledged Franz publish: %v", err)
			}
			var record commonkafka.DecodedRecord
			select {
			case record = <-received:
			case <-ctx.Done():
				t.Fatalf("consume published record: %v", ctx.Err())
			}
			close(deliveryComplete)
			if err := producer.Publish(ctx, commonkafka.Event{Topic: fixture.Topic, Key: barrierKey, Payload: message, EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource]}); err != nil {
				t.Fatalf("publish commit barrier: %v", err)
			}
			select {
			case <-barrierStarted:
			case <-ctx.Done():
				t.Fatalf("confirm target completion before cancellation: %v", ctx.Err())
			}
			time.Sleep(100 * time.Millisecond) // ponytail: bounded wait for the preceding synchronous broker commit; replace with exposed commit hook if production adds one.
			cancel()
			if err := <-runResult; err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				t.Fatalf("stop Franz consumer after delivery: %v", err)
			}

			// Then
			if !bytes.Equal(record.Key, testKey) || !proto.Equal(record.Message, message) {
				t.Fatal("consumed record differs from the published fixture message")
			}
			if eventType, ok := integrationHeader(record.Headers, commonkafka.HeaderEventType); !ok || eventType != fixture.EventType {
				t.Fatalf("event-type = %q, want %q", eventType, fixture.EventType)
			}
			if source, ok := integrationHeader(record.Headers, commonkafka.HeaderSource); !ok || source != fixture.Headers[commonkafka.HeaderSource] {
				t.Fatalf("source = %q, want %q", source, fixture.Headers[commonkafka.HeaderSource])
			}
			if eventID, ok := integrationHeader(record.Headers, commonkafka.HeaderEventID); !ok || eventID != fixture.Headers[commonkafka.HeaderEventID] {
				t.Fatalf("event ID = %q, want %q", eventID, fixture.Headers[commonkafka.HeaderEventID])
			}
			frame, err := registry.Encode(fixture.Topic, message)
			if err != nil {
				t.Fatalf("re-encode published message: %v", err)
			}
			if !bytes.Equal(record.Value, frame) {
				t.Fatal("consumed raw frame differs from Schema Registry frame")
			}
		})
	}
}

func TestGivenJavaPublishedMatrix_WhenGoConsumes_ThenVerifiesEveryFixture(t *testing.T) {
	// Given
	runID := foundationMatrixRunID(t)
	brokers, registryURL := integrationEndpoints(t)
	registry := newIntegrationRegistry(t, registryURL)

	// When / Then
	for _, fixture := range publishedConfluentFixtures(t).Cases {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			key := []byte(foundationMatrixKey(fixture, runID, "java-to-go"))
			consumer, err := internalkafka.NewConsumer(strings.Split(brokers, ","), runID+"-java-to-go-"+fixture.Name, []string{fixture.Topic})
			if err != nil {
				t.Fatalf("create matrix consumer: %v", err)
			}
			t.Cleanup(consumer.Close)
			ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
			defer cancel()
			record := pollInternalRecordForKey(t, ctx, consumer, key)
			decoded, err := commonkafka.Decode(commonkafka.RawRecord{
				Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
				Key: record.Key, Value: record.Value, Headers: internalHeaders(record.Headers),
			}, registry)
			if err != nil {
				t.Fatalf("decode Java matrix record: %v", err)
			}
			expected := fixtureMessage(t, fixture.EventType)
			payload, err := hex.DecodeString(fixture.PayloadHex)
			if err != nil {
				t.Fatalf("decode fixture payload: %v", err)
			}
			if err := proto.Unmarshal(payload, expected); err != nil {
				t.Fatalf("unmarshal fixture payload: %v", err)
			}
			if record.Topic != fixture.Topic || !bytes.Equal(record.Key, key) || !proto.Equal(decoded.Message, expected) {
				t.Fatal("Java-to-Go matrix record differs from canonical fixture")
			}
			subject, err := commonkafka.SubjectName(record.Topic)
			if err != nil {
				t.Fatalf("resolve matrix subject: %v", err)
			}
			if subject != fixture.Subject {
				t.Fatalf("subject = %q, want %q", subject, fixture.Subject)
			}
			if string(decoded.Message.ProtoReflect().Descriptor().FullName()) != fixture.EventType {
				t.Fatalf("descriptor = %q, want %q", decoded.Message.ProtoReflect().Descriptor().FullName(), fixture.EventType)
			}
			if len(decoded.Headers) != len(fixture.Headers) {
				t.Fatalf("headers = %d, want %d", len(decoded.Headers), len(fixture.Headers))
			}
			for index, header := range canonicalFixtureHeaders(fixture.Headers) {
				if decoded.Headers[index].Key != header.Key || !bytes.Equal(decoded.Headers[index].Value, header.Value) {
					t.Fatalf("header %d differs from canonical fixture", index)
				}
			}
			frame, err := registry.Encode(fixture.Topic, expected)
			if err != nil {
				t.Fatalf("encode canonical frame: %v", err)
			}
			if !bytes.Equal(record.Value, frame) || !bytes.Equal(record.Value, mustDecodeHex(t, fixture.Frame.CompleteHex)) {
				t.Fatal("Java-to-Go matrix frame differs from canonical frame")
			}
		})
	}
}

func TestGivenMatrixRun_WhenGoPublishes_ThenWritesEveryFixtureForJava(t *testing.T) {
	// Given
	runID := foundationMatrixRunID(t)
	brokers, registryURL := integrationEndpoints(t)
	registry := newIntegrationRegistry(t, registryURL)
	var publishTime time.Time
	producer, err := commonkafka.NewFranzProducer(commonkafka.TransportConfig{
		Brokers: strings.Split(brokers, ","), PublishTimeout: integrationTimeout,
		Clock: func() time.Time { return publishTime },
	}, registry)
	if err != nil {
		t.Fatalf("create matrix producer: %v", err)
	}
	defer producer.Close()

	// When
	for _, fixture := range publishedConfluentFixtures(t).Cases {
		publishTime = time.UnixMilli(matrixFixtureTimestamp(t, fixture))
		message := fixtureMessage(t, fixture.EventType)
		payload := mustDecodeHex(t, fixture.PayloadHex)
		if err := proto.Unmarshal(payload, message); err != nil {
			t.Fatalf("decode fixture %s: %v", fixture.Name, err)
		}
		parts := strings.Split(fixture.Headers[commonkafka.HeaderTraceParent], "-")
		traceID, err := trace.TraceIDFromHex(parts[1])
		if err != nil {
			t.Fatalf("parse trace ID: %v", err)
		}
		spanID, err := trace.SpanIDFromHex(parts[2])
		if err != nil {
			t.Fatalf("parse span ID: %v", err)
		}
		traceState := trace.TraceState{}
		if value := fixture.Headers[commonkafka.HeaderTraceState]; value != "" {
			traceState, err = trace.ParseTraceState(value)
			if err != nil {
				t.Fatalf("parse tracestate: %v", err)
			}
		}
		ctx := trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, TraceState: traceState,
		}))
		err = producer.Publish(ctx, commonkafka.Event{
			Topic: fixture.Topic, Key: []byte(foundationMatrixKey(fixture, runID, "go-to-java")), Payload: message,
			EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource],
		})
		if err != nil {
			t.Fatalf("publish Go matrix fixture %s: %v", fixture.Name, err)
		}
	}

	// Then
	if len(publishedConfluentFixtures(t).Cases) != 9 {
		t.Fatal("foundation matrix requires all nine canonical fixtures")
	}
}

func TestGivenMalformedAndUnknownSchemaFrames_WhenGoConsumes_ThenPreservesRawBytesInDLQ(t *testing.T) {
	// Given
	brokers, registryURL := integrationEndpoints(t)
	registry := newIntegrationRegistry(t, registryURL)
	fixture := publishedConfluentFixtures(t).Cases[0]
	config := commonkafka.TransportConfig{
		Brokers: strings.Split(brokers, ","), Topics: []string{fixture.Topic}, PublishTimeout: integrationTimeout,
	}
	producer, err := commonkafka.NewFranzProducer(config, registry)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()
	testCases := []struct {
		name  string
		frame func([]byte)
	}{
		{name: "malformed-magic-byte", frame: func(frame []byte) { frame[0] = 1 }},
		{name: "unknown-schema-id", frame: func(frame []byte) { copy(frame[1:5], []byte{0x7f, 0xff, 0xff, 0xff}) }},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			group := fmt.Sprintf("common-go-invalid-%s-%d", testCase.name, time.Now().UnixNano())
			key := []byte(group)
			frame := mustDecodeHex(t, fixture.Frame.CompleteHex)
			testCase.frame(frame)
			raw := commonkafka.RawRecord{
				Topic: fixture.Topic, Key: key, Value: frame, Headers: fixtureHeaders(fixture.Headers),
			}
			observer, err := internalkafka.NewConsumer(strings.Split(brokers, ","), group+"-dlq", []string{fixture.Topic + ".DLQ"})
			if err != nil {
				t.Fatalf("create DLQ observer: %v", err)
			}
			defer observer.Close()
			consumerConfig := config
			consumerConfig.ConsumerGroup = group
			consumer, err := commonkafka.NewFranzConsumer(consumerConfig, registry, producer, noWaitSleeper{})
			if err != nil {
				t.Fatalf("create consumer: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
			defer cancel()
			runResult := make(chan error, 1)
			go func() {
				runResult <- consumer.Run(ctx, func(context.Context, commonkafka.DecodedRecord) error { return nil })
			}()

			// When
			if err := producer.PublishRaw(ctx, raw); err != nil {
				t.Fatalf("publish invalid frame: %v", err)
			}
			dlq := pollInternalRecordForKey(t, ctx, observer, key)
			consumer.Close()

			// Then
			if !bytes.Equal(dlq.Key, raw.Key) || !bytes.Equal(dlq.Value, raw.Value) {
				t.Fatal("invalid frame DLQ did not preserve source bytes")
			}
			assertRawDlqHeaders(t, raw.Headers, dlq.Headers, fixture.Topic)
		})
	}
}

func TestGivenCanceledOrExpiredContext_WhenGoPublishes_ThenReturnsWithoutAcknowledgement(t *testing.T) {
	// Given
	brokers, registryURL := integrationEndpoints(t)
	registry := newIntegrationRegistry(t, registryURL)
	fixture := publishedConfluentFixtures(t).Cases[0]
	message := fixtureMessage(t, fixture.EventType)
	if err := proto.Unmarshal(mustDecodeHex(t, fixture.PayloadHex), message); err != nil {
		t.Fatalf("decode fixture message: %v", err)
	}
	producer, err := commonkafka.NewFranzProducer(commonkafka.TransportConfig{
		Brokers: strings.Split(brokers, ","), PublishTimeout: integrationTimeout,
	}, registry)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()
	event := commonkafka.Event{
		Topic: fixture.Topic, Key: []byte("canceled-publish"), Payload: message,
		EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource],
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expire()

	// When
	canceledErr := producer.Publish(canceled, event)
	expiredErr := producer.Publish(expired, event)

	// Then
	if !errors.Is(canceledErr, context.Canceled) {
		t.Fatalf("canceled publish error = %v", canceledErr)
	}
	if !errors.Is(expiredErr, context.DeadlineExceeded) {
		t.Fatalf("expired publish error = %v", expiredErr)
	}
}

func TestGivenRetryableHandler_WhenFranzConsumesLiveRecord_ThenCommitsOnlyAfterSuccess(t *testing.T) {
	// Given
	brokers, registryURL := integrationEndpoints(t)
	registry := newIntegrationRegistry(t, registryURL)
	fixture := publishedConfluentFixtures(t).Cases[0]
	message := fixtureMessage(t, fixture.EventType)
	payload, err := hex.DecodeString(fixture.PayloadHex)
	if err != nil {
		t.Fatalf("decode fixture payload: %v", err)
	}
	if err := proto.Unmarshal(payload, message); err != nil {
		t.Fatalf("decode fixture message: %v", err)
	}
	config := commonkafka.TransportConfig{Brokers: strings.Split(brokers, ","), Topics: []string{fixture.Topic}, PublishTimeout: integrationTimeout}
	producer, err := commonkafka.NewFranzProducer(config, registry)
	if err != nil {
		t.Fatalf("create Franz producer: %v", err)
	}
	t.Cleanup(producer.Close)
	group := fmt.Sprintf("common-go-retry-%d", time.Now().UnixNano())
	key := []byte(group)
	barrierKey := []byte(group + "-barrier")
	consumerConfig := config
	consumerConfig.ConsumerGroup = group
	consumer, err := commonkafka.NewFranzConsumer(consumerConfig, registry, producer, noWaitSleeper{})
	if err != nil {
		t.Fatalf("create Franz consumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	var attempts atomic.Int32
	barrierStarted := make(chan struct{}, 1)
	runResult := make(chan error, 1)
	go func() {
		runResult <- consumer.Run(ctx, func(handlerContext context.Context, record commonkafka.DecodedRecord) error {
			switch {
			case bytes.Equal(record.Key, key):
				if attempts.Add(1) < 4 {
					return errors.New("transient")
				}
				return nil
			case bytes.Equal(record.Key, barrierKey):
				select {
				case barrierStarted <- struct{}{}:
				default:
				}
				<-handlerContext.Done()
				return handlerContext.Err()
			default:
				return nil
			}
		})
	}()

	// When
	if err := producer.Publish(ctx, commonkafka.Event{Topic: fixture.Topic, Key: key, Payload: message, EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource]}); err != nil {
		t.Fatalf("publish retry target: %v", err)
	}
	if err := producer.Publish(ctx, commonkafka.Event{Topic: fixture.Topic, Key: barrierKey, Payload: message, EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource]}); err != nil {
		t.Fatalf("publish barrier: %v", err)
	}
	select {
	case <-barrierStarted:
	case <-ctx.Done():
		t.Fatalf("wait for barrier after successful retry: %v", ctx.Err())
	}
	cancel()
	consumer.Close()

	// Then
	if attempts.Load() != 4 {
		t.Fatalf("retry attempts=%d, want 4", attempts.Load())
	}
}

func TestGivenFailedDlqAndCanceledConsumer_WhenReplacedInSameGroup_ThenRedeliversAndPreservesRawDLQ(t *testing.T) {
	// Given
	brokers, registryURL := integrationEndpoints(t)
	registry := newIntegrationRegistry(t, registryURL)
	fixture := publishedConfluentFixtures(t).Cases[0]
	message := fixtureMessage(t, fixture.EventType)
	payload, err := hex.DecodeString(fixture.PayloadHex)
	if err != nil {
		t.Fatalf("decode fixture payload: %v", err)
	}
	if err := proto.Unmarshal(payload, message); err != nil {
		t.Fatalf("decode fixture message: %v", err)
	}
	config := commonkafka.TransportConfig{Brokers: strings.Split(brokers, ","), Topics: []string{fixture.Topic}, PublishTimeout: integrationTimeout}
	producer, err := commonkafka.NewFranzProducer(config, registry)
	if err != nil {
		t.Fatalf("create Franz producer: %v", err)
	}
	t.Cleanup(producer.Close)
	group := fmt.Sprintf("common-go-dlq-%d", time.Now().UnixNano())
	key := []byte(group)
	barrierKey := []byte(group + "-later-offset")
	var firstLaterOffset atomic.Int32
	firstConfig := config
	firstConfig.ConsumerGroup = group
	first, err := commonkafka.NewFranzConsumer(firstConfig, registry, &failOnceRawPublisher{delegate: producer}, noWaitSleeper{})
	if err != nil {
		t.Fatalf("create first consumer: %v", err)
	}
	firstContext, cancelFirst := context.WithTimeout(context.Background(), integrationTimeout)
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- first.Run(firstContext, func(_ context.Context, record commonkafka.DecodedRecord) error {
			switch {
			case bytes.Equal(record.Key, key):
				return commonkafka.Permanent{Err: errors.New("invalid input")}
			case bytes.Equal(record.Key, barrierKey):
				firstLaterOffset.Add(1)
			}
			return nil
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	if err := producer.Publish(ctx, commonkafka.Event{Topic: fixture.Topic, Key: key, Payload: message, EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource]}); err != nil {
		t.Fatalf("publish failed-DLQ target: %v", err)
	}
	if err := producer.Publish(ctx, commonkafka.Event{Topic: fixture.Topic, Key: barrierKey, Payload: message, EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource]}); err != nil {
		t.Fatalf("publish later offset: %v", err)
	}
	select {
	case err := <-firstResult:
		if err == nil {
			t.Fatal("first consumer should fail the DLQ publication")
		}
	case <-ctx.Done():
		t.Fatalf("wait for failed DLQ: %v", ctx.Err())
	}
	if firstLaterOffset.Load() != 0 {
		t.Fatal("consumer processed later offset after incomplete DLQ source")
	}
	cancelFirst()
	first.Close()

	observer, err := internalkafka.NewConsumer(strings.Split(brokers, ","), group+"-observer", []string{fixture.Topic + ".DLQ"})
	if err != nil {
		t.Fatalf("create raw DLQ observer: %v", err)
	}
	observerContext, cancelObserver := context.WithTimeout(context.Background(), integrationTimeout)
	t.Cleanup(func() {
		cancelObserver()
		observer.Close()
	})
	observedDLQ := make(chan internalkafka.Record, 1)
	observerError := make(chan error, 1)
	go func() {
		for {
			record, received, err := observer.Poll(observerContext)
			observer.AllowRebalance()
			if err != nil {
				observerError <- err
				return
			}
			if received && bytes.Equal(record.Key, key) {
				observedDLQ <- record
				return
			}
		}
	}()

	secondConfig := config
	secondConfig.ConsumerGroup = group
	second, err := commonkafka.NewFranzConsumer(secondConfig, registry, producer, noWaitSleeper{})
	if err != nil {
		t.Fatalf("create replacement consumer: %v", err)
	}
	t.Cleanup(second.Close)
	redelivered := make(chan commonkafka.RawRecord, 1)
	secondContext, cancelSecond := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancelSecond()
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- second.Run(secondContext, func(_ context.Context, record commonkafka.DecodedRecord) error {
			if bytes.Equal(record.Key, key) {
				redelivered <- record.RawRecord
				return commonkafka.Permanent{Err: errors.New("invalid input")}
			}
			return nil
		})
	}()
	var source commonkafka.RawRecord
	select {
	case source = <-redelivered:
	case <-secondContext.Done():
		t.Fatalf("wait for same-group redelivery: %v", secondContext.Err())
	}
	var dlq internalkafka.Record
	select {
	case dlq = <-observedDLQ:
	case err := <-observerError:
		t.Fatalf("poll raw DLQ observer: %v", err)
	case <-observerContext.Done():
		t.Fatalf("wait for raw DLQ: %v", observerContext.Err())
	}
	second.Close()
	cancelSecond()

	// Then
	if firstLaterOffset.Load() != 0 {
		t.Fatal("consumer processed later offset after incomplete DLQ source")
	}
	if !bytes.Equal(source.Key, dlq.Key) || !bytes.Equal(source.Value, dlq.Value) {
		t.Fatal("DLQ did not preserve source key and complete frame")
	}
	assertRawDlqHeaders(t, source.Headers, dlq.Headers, fixture.Topic)
}

func TestGivenCanceledHandler_WhenReplacementJoinsSameGroup_ThenRedeliversUncommittedSource(t *testing.T) {
	// Given
	brokers, registryURL := integrationEndpoints(t)
	registry := newIntegrationRegistry(t, registryURL)
	fixture := publishedConfluentFixtures(t).Cases[0]
	message := fixtureMessage(t, fixture.EventType)
	payload, err := hex.DecodeString(fixture.PayloadHex)
	if err != nil {
		t.Fatalf("decode fixture payload: %v", err)
	}
	if err := proto.Unmarshal(payload, message); err != nil {
		t.Fatalf("decode fixture message: %v", err)
	}
	config := commonkafka.TransportConfig{Brokers: strings.Split(brokers, ","), Topics: []string{fixture.Topic}, PublishTimeout: integrationTimeout}
	producer, err := commonkafka.NewFranzProducer(config, registry)
	if err != nil {
		t.Fatalf("create Franz producer: %v", err)
	}
	t.Cleanup(producer.Close)
	group := fmt.Sprintf("common-go-cancel-%d", time.Now().UnixNano())
	key := []byte(group)
	firstConfig := config
	firstConfig.ConsumerGroup = group
	first, err := commonkafka.NewFranzConsumer(firstConfig, registry, producer, noWaitSleeper{})
	if err != nil {
		t.Fatalf("create first consumer: %v", err)
	}
	firstContext, cancelFirst := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- first.Run(firstContext, func(handlerContext context.Context, record commonkafka.DecodedRecord) error {
			if !bytes.Equal(record.Key, key) {
				return nil
			}
			started <- struct{}{}
			<-handlerContext.Done()
			return handlerContext.Err()
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	if err := producer.Publish(ctx, commonkafka.Event{Topic: fixture.Topic, Key: key, Payload: message, EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource]}); err != nil {
		t.Fatalf("publish cancellation target: %v", err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatalf("wait for first handler: %v", ctx.Err())
	}
	cancelFirst()
	first.Close()

	secondConfig := config
	secondConfig.ConsumerGroup = group
	second, err := commonkafka.NewFranzConsumer(secondConfig, registry, producer, noWaitSleeper{})
	if err != nil {
		t.Fatalf("create replacement consumer: %v", err)
	}
	t.Cleanup(second.Close)
	redelivered := make(chan struct{}, 1)
	secondContext, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- second.Run(secondContext, func(_ context.Context, record commonkafka.DecodedRecord) error {
			if bytes.Equal(record.Key, key) {
				redelivered <- struct{}{}
			}
			return nil
		})
	}()

	// When
	select {
	case <-redelivered:
	case <-ctx.Done():
		t.Fatalf("wait for replacement redelivery: %v", ctx.Err())
	}
	cancelSecond()
	second.Close()

	// Then
	// Reaching this point proves the canceled first handler did not commit the source record.
}

func matrixFixtureTimestamp(t *testing.T, fixture confluentFixtureCase) int64 {
	t.Helper()
	value, err := strconv.ParseInt(fixture.Headers[commonkafka.HeaderTimestamp], 10, 64)
	if err != nil {
		t.Fatalf("parse fixture timestamp: %v", err)
	}
	return value
}

func foundationMatrixRunID(t *testing.T) string {
	t.Helper()
	runID := strings.TrimSpace(os.Getenv("FOUNDATION_MATRIX_RUN_ID"))
	if runID == "" {
		t.Skip("FOUNDATION_MATRIX_RUN_ID is required for cross-language matrix tests")
	}
	return runID
}

func foundationMatrixKey(fixture confluentFixtureCase, runID, direction string) string {
	return runID + "-" + direction + "-" + fixture.Name
}

func pollInternalRecordForKey(t *testing.T, ctx context.Context, consumer *internalkafka.Consumer, key []byte) internalkafka.Record {
	t.Helper()
	for {
		record, received, err := consumer.Poll(ctx)
		consumer.AllowRebalance()
		if err != nil {
			t.Fatalf("poll matrix record: %v", err)
		}
		if received && bytes.Equal(record.Key, key) {
			return record
		}
	}
}

func internalHeaders(headers []internalkafka.Header) []commonkafka.Header {
	result := make([]commonkafka.Header, len(headers))
	for index, header := range headers {
		result[index] = commonkafka.Header{Key: header.Key, Value: header.Value}
	}
	return result
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("decode hex: %v", err)
	}
	return decoded
}

func integrationEndpoints(t *testing.T) (string, string) {
	t.Helper()
	brokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	registryURL := strings.TrimSpace(os.Getenv("SCHEMA_REGISTRY_URL"))
	if brokers == "" || registryURL == "" {
		t.Fatal("integration tests require KAFKA_BROKERS and SCHEMA_REGISTRY_URL")
	}
	return brokers, registryURL
}

func newIntegrationRegistry(t *testing.T, registryURL string) *commonkafka.ConfluentProtobufRegistry {
	t.Helper()
	registry, err := commonkafka.NewConfluentProtobufRegistry(commonkafka.RegistryConfig{URL: registryURL})
	if err != nil {
		t.Fatalf("create Schema Registry adapter: %v", err)
	}
	t.Cleanup(func() {
		if err := registry.Close(); err != nil {
			t.Errorf("close Schema Registry adapter: %v", err)
		}
	})
	return registry
}

func fixtureHeaders(headers map[string]string) []commonkafka.Header {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]commonkafka.Header, 0, len(keys))
	for _, key := range keys {
		result = append(result, commonkafka.Header{Key: key, Value: []byte(headers[key])})
	}
	return result
}

func canonicalFixtureHeaders(headers map[string]string) []commonkafka.Header {
	keys := []string{
		commonkafka.HeaderEventType,
		commonkafka.HeaderSource,
		commonkafka.HeaderTimestamp,
		commonkafka.HeaderEventID,
		commonkafka.HeaderTraceParent,
	}
	result := make([]commonkafka.Header, 0, len(keys)+1)
	for _, key := range keys {
		result = append(result, commonkafka.Header{Key: key, Value: []byte(headers[key])})
	}
	if value := headers[commonkafka.HeaderTraceState]; value != "" {
		result = append(result, commonkafka.Header{Key: commonkafka.HeaderTraceState, Value: []byte(value)})
	}
	return result
}

func integrationHeader(headers []commonkafka.Header, key string) (string, bool) {
	for _, header := range headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value), true
		}
	}
	return "", false
}

type noWaitSleeper struct{}

func (noWaitSleeper) Sleep(context.Context, time.Duration) error { return nil }

type failOnceRawPublisher struct {
	delegate commonkafka.RawPublisher
	failed   atomic.Bool
}

func (p *failOnceRawPublisher) PublishRaw(ctx context.Context, record commonkafka.RawRecord) error {
	if !p.failed.Swap(true) {
		return errors.New("deliberate DLQ outage")
	}
	return p.delegate.PublishRaw(ctx, record)
}

func assertRawDlqHeaders(t *testing.T, source []commonkafka.Header, actual []internalkafka.Header, topic string) {
	t.Helper()
	if len(actual) != len(source)+4 {
		t.Fatalf("DLQ headers=%d, want %d", len(actual), len(source)+4)
	}
	for index, header := range source {
		if actual[index].Key != header.Key || !bytes.Equal(actual[index].Value, header.Value) {
			t.Fatalf("DLQ header %d does not preserve the source order and bytes", index)
		}
	}
	appended := actual[len(source):]
	if appended[0].Key != commonkafka.HeaderOriginalTopic || string(appended[0].Value) != topic {
		t.Fatal("DLQ original topic diagnostic is missing")
	}
	if appended[1].Key != commonkafka.HeaderExceptionMessage || len(appended[1].Value) == 0 {
		t.Fatal("DLQ exception diagnostic is missing")
	}
	if appended[2].Key != commonkafka.HeaderFailedAt || len(appended[2].Value) == 0 {
		t.Fatal("DLQ failed-at diagnostic is missing")
	}
	if appended[3].Key != commonkafka.HeaderRetryCount || string(appended[3].Value) != "1" {
		t.Fatal("DLQ retry-count diagnostic is wrong")
	}
}
