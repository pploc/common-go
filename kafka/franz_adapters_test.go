package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	franz "github.com/pploc/common-go/internal/kafka"
	eventsv1 "github.com/pploc/proto-go/events/v1"
	"google.golang.org/protobuf/proto"
)

func TestGivenFranzAdapterConstructors_WhenValidatingDependencies_ThenRejectInvalidInput(t *testing.T) {
	// Given
	config := TransportConfig{
		Brokers:        []string{"broker:9092"},
		Topics:         []string{"identity.user.registered.v1"},
		ConsumerGroup:  "member",
		PublishTimeout: time.Second,
	}

	// When
	_, missingEncoderErr := NewFranzProducer(config, nil)
	_, missingResolverErr := NewFranzConsumer(config, nil, &recordingRawPublisher{}, nil)
	_, missingDLQErr := NewFranzConsumer(config, &recordingResolver{}, nil, nil)
	producer, producerErr := NewFranzProducer(config, frameEncoderFunc(func(string, proto.Message) ([]byte, error) {
		return nil, nil
	}))
	consumer, consumerErr := NewFranzConsumer(config, &recordingResolver{}, &recordingRawPublisher{}, nil)

	// Then
	if missingEncoderErr == nil {
		t.Fatal("producer accepted a nil frame encoder")
	}
	if missingResolverErr == nil {
		t.Fatal("consumer accepted a nil schema resolver")
	}
	if missingDLQErr == nil {
		t.Fatal("consumer accepted a nil DLQ publisher")
	}
	if producerErr != nil {
		t.Fatalf("create Franz producer: %v", producerErr)
	}
	if consumerErr != nil {
		producer.Close()
		t.Fatalf("create Franz consumer: %v", consumerErr)
	}
	producer.Close()
	consumer.Close()
}

func TestGivenMutableTransportRecord_WhenConvertingFromFranz_ThenDefensivelyCopiesRawBytes(t *testing.T) {
	// Given
	record := franz.Record{
		Topic:     "identity.user.registered.v1",
		Partition: 2,
		Offset:    14,
		Key:       []byte("original-key"),
		Value:     []byte("original-frame"),
		Headers:   []franz.Header{{Key: "event-id", Value: []byte("event-1")}},
	}

	// When
	converted := fromFranzRecord(record)
	record.Key[0] = 'X'
	record.Value[0] = 'X'
	record.Headers[0].Value[0] = 'X'

	// Then
	if got := string(converted.Key); got != "original-key" {
		t.Fatalf("key = %q, want original copy", got)
	}
	if got := string(converted.Value); got != "original-frame" {
		t.Fatalf("value = %q, want original copy", got)
	}
	if got := string(converted.Headers[0].Value); got != "event-1" {
		t.Fatalf("header value = %q, want original copy", got)
	}
	if converted.Topic != record.Topic || converted.Partition != record.Partition || converted.Offset != record.Offset {
		t.Fatalf("record coordinates changed: %#v", converted)
	}
}

func TestGivenUninitializedFranzProducer_WhenPublishing_ThenReturnsInitializationError(t *testing.T) {
	// Given
	producer := &FranzProducer{}
	event := Event{
		Topic:   "identity.user.registered.v1",
		Payload: &eventsv1.UserRegisteredEvent{},
		EventID: "event-1",
		Source:  "identifier",
	}

	// When
	err := producer.Publish(context.Background(), event)

	// Then
	if err == nil || !strings.Contains(err.Error(), "producer is not initialized") {
		t.Fatalf("error = %v, want producer initialization error", err)
	}
}

func TestGivenUninitializedFranzConsumer_WhenRunning_ThenReturnsInitializationError(t *testing.T) {
	// Given
	consumer := &FranzConsumer{}

	// When
	err := consumer.Run(context.Background(), func(context.Context, DecodedRecord) error {
		return nil
	})

	// Then
	if err == nil || !strings.Contains(err.Error(), "consumer is not initialized") {
		t.Fatalf("error = %v, want consumer initialization error", err)
	}
}

func TestGivenFranzConsumer_WhenHandlerIsMissing_ThenReturnsValidationError(t *testing.T) {
	// Given
	consumer := &FranzConsumer{consumer: &franz.Consumer{}}

	// When
	err := consumer.Run(context.Background(), nil)

	// Then
	if err == nil || !strings.Contains(err.Error(), "handler is required") {
		t.Fatalf("error = %v, want missing handler error", err)
	}
}

type frameEncoderFunc func(string, proto.Message) ([]byte, error)

func (f frameEncoderFunc) Encode(topic string, message proto.Message) ([]byte, error) {
	return f(topic, message)
}
