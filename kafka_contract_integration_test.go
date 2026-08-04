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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	internalkafka "github.com/pploc/common-go/internal/kafka"
	commonkafka "github.com/pploc/common-go/kafka"
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
			if bytes.Equal(record.Key, key) {
				return commonkafka.Permanent{Err: errors.New("invalid input")}
			}
			return nil
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	if err := producer.Publish(ctx, commonkafka.Event{Topic: fixture.Topic, Key: key, Payload: message, EventID: fixture.Headers[commonkafka.HeaderEventID], Source: fixture.Headers[commonkafka.HeaderSource]}); err != nil {
		t.Fatalf("publish failed-DLQ target: %v", err)
	}
	select {
	case err := <-firstResult:
		if err == nil {
			t.Fatal("first consumer should fail the DLQ publication")
		}
	case <-ctx.Done():
		t.Fatalf("wait for failed DLQ: %v", ctx.Err())
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
