//go:build integration

package common_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

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

func TestGivenSeededKafkaAndRegistry_WhenPublishingWithFranz_ThenConsumesDecodedFramedRecord(t *testing.T) {
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

	producer, err := commonkafka.NewFranzProducer(commonkafka.TransportConfig{
		Brokers:        strings.Split(brokers, ","),
		PublishTimeout: integrationTimeout,
	}, registry)
	if err != nil {
		t.Fatalf("create Franz producer: %v", err)
	}
	t.Cleanup(producer.Close)

	group := fmt.Sprintf("common-go-contract-%d", time.Now().UnixNano())
	testKey := []byte(group)
	consumer, err := commonkafka.NewFranzConsumer(commonkafka.TransportConfig{
		Brokers:        strings.Split(brokers, ","),
		Topics:         []string{fixture.Topic},
		ConsumerGroup:  group,
		PublishTimeout: integrationTimeout,
	}, registry, producer, nil)
	if err != nil {
		t.Fatalf("create Franz consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	received := make(chan commonkafka.DecodedRecord, 1)
	deliveryComplete := make(chan struct{})
	runResult := make(chan error, 1)
	go func() {
		runResult <- consumer.Run(ctx, func(_ context.Context, record commonkafka.DecodedRecord) error {
			if !bytes.Equal(record.Key, testKey) {
				return nil
			}
			received <- record
			<-deliveryComplete
			return nil
		})
	}()

	// When
	err = producer.Publish(ctx, commonkafka.Event{
		Topic:   fixture.Topic,
		Key:     testKey,
		Payload: message,
		EventID: fixture.Headers[commonkafka.HeaderEventID],
		Source:  fixture.Headers[commonkafka.HeaderSource],
	})
	if err != nil {
		t.Fatalf("acknowledged Franz publish: %v", err)
	}

	var record commonkafka.DecodedRecord
	select {
	case record = <-received:
	case <-ctx.Done():
		t.Fatalf("consume published record: %v", ctx.Err())
	}
	close(deliveryComplete)
	time.Sleep(500 * time.Millisecond)
	cancel()
	if err := <-runResult; err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Fatalf("stop Franz consumer after delivery: %v", err)
	}

	// Then
	if !bytes.Equal(record.Key, testKey) {
		t.Fatalf("key = %q, want %q", record.Key, testKey)
	}
	if !proto.Equal(record.Message, message) {
		t.Fatalf("decoded message = %T %v, want %T %v", record.Message, record.Message, message, message)
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
