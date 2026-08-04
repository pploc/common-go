package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	franz "github.com/pploc/common-go/internal/kafka"
)

// FranzProducer is the public project-owned adapter for concrete Protobuf
// publishing and raw DLQ writes. franz-go remains private to internal/kafka.
type FranzProducer struct {
	producer *franz.Producer
	encoder  FrameEncoder
	timeout  time.Duration
	clock    func() time.Time
}

// NewFranzProducer creates an acknowledged all-ISR/idempotent producer. The
// frame encoder must only resolve pre-registered Schema Registry schemas.
func NewFranzProducer(config TransportConfig, encoder FrameEncoder) (*FranzProducer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if encoder == nil {
		return nil, fmt.Errorf("kafka: frame encoder is required")
	}
	producer, err := franz.NewProducer(config.Brokers, config.PublishTimeout)
	if err != nil {
		return nil, err
	}
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	return &FranzProducer{producer: producer, encoder: encoder, timeout: config.PublishTimeout, clock: clock}, nil
}

// Publish validates the frozen topic/type pair, builds canonical metadata, and
// waits for broker acknowledgement of the complete Confluent-framed value.
func (p *FranzProducer) Publish(ctx context.Context, event Event) error {
	if p == nil || p.producer == nil || p.encoder == nil || p.clock == nil {
		return fmt.Errorf("kafka: producer is not initialized")
	}
	if err := ValidateEvent(event); err != nil {
		return err
	}
	headers, err := CanonicalHeaders(ctx, event.Payload, event.Source, event.EventID, p.clock(), event.Headers)
	if err != nil {
		return err
	}
	frame, err := p.encoder.Encode(event.Topic, event.Payload)
	if err != nil {
		return fmt.Errorf("kafka: encode Confluent protobuf frame: %w", err)
	}
	return p.publish(ctx, franz.Record{Topic: event.Topic, Key: event.Key, Value: frame, Headers: toFranzHeaders(headers)})
}

// PublishRaw sends a pre-existing raw key/frame/header tuple for the DLQ path.
// It does not invoke a Protobuf serializer or mutate supplied bytes.
func (p *FranzProducer) PublishRaw(ctx context.Context, record RawRecord) error {
	if strings.TrimSpace(record.Topic) == "" {
		return fmt.Errorf("kafka: raw record topic is required")
	}
	return p.publish(ctx, franz.Record{
		Topic: record.Topic, Key: record.Key, Value: record.Value, Headers: toFranzHeaders(record.Headers),
	})
}

func (p *FranzProducer) publish(ctx context.Context, record franz.Record) error {
	if p == nil || p.producer == nil {
		return fmt.Errorf("kafka: producer is not initialized")
	}
	publishContext, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return p.producer.Produce(publishContext, record)
}

// Close releases producer connections after all in-flight broker calls finish.
func (p *FranzProducer) Close() {
	if p != nil && p.producer != nil {
		p.producer.Close()
	}
}

func toFranzHeaders(headers []Header) []franz.Header {
	result := make([]franz.Header, len(headers))
	for index, header := range headers {
		result[index] = franz.Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return result
}

// FranzConsumer adapts a private franz-go manual-commit consumer to the stable
// Consumer interface. It processes one source record at a time, so a later
// record never passes an unfinished record from the same partition.
type FranzConsumer struct {
	consumer *franz.Consumer
	resolver SchemaResolver
	dlq      RawPublisher
	sleeper  Sleeper
	clock    func() time.Time
}

// NewFranzConsumer constructs a manual-commit consumer. The DLQ publisher is
// normally the same FranzProducer configured with the raw byte path.
func NewFranzConsumer(
	config TransportConfig,
	resolver SchemaResolver,
	dlq RawPublisher,
	sleeper Sleeper,
) (*FranzConsumer, error) {
	if err := config.ValidateConsumer(); err != nil {
		return nil, err
	}
	if resolver == nil || dlq == nil {
		return nil, fmt.Errorf("kafka: consumer requires schema resolver and DLQ publisher")
	}
	consumer, err := franz.NewConsumer(config.Brokers, config.ConsumerGroup, config.Topics)
	if err != nil {
		return nil, err
	}
	return &FranzConsumer{consumer: consumer, resolver: resolver, dlq: dlq, sleeper: sleeper, clock: time.Now}, nil
}

// Run polls one record at a time. A failed DLQ write returns before source
// commit so broker redelivery remains possible after restart or rebalance.
func (c *FranzConsumer) Run(ctx context.Context, handler Handler) error {
	if c == nil || c.consumer == nil {
		return fmt.Errorf("kafka: consumer is not initialized")
	}
	if handler == nil {
		return fmt.Errorf("kafka: handler is required")
	}
	for {
		record, received, err := c.consumer.Poll(ctx)
		if err != nil {
			c.consumer.AllowRebalance()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("poll Kafka consumer: %w", err)
		}
		if !received {
			c.consumer.AllowRebalance()
			continue
		}
		raw := fromFranzRecord(record)
		delivery := Delivery{
			Resolver: c.resolver,
			Handler:  handler,
			DLQ:      c.dlq,
			Sleeper:  c.sleeper,
			Clock:    c.clock,
			Commit: func(commitCtx context.Context, _ RawRecord) error {
				return c.consumer.Commit(commitCtx, record)
			},
		}
		processErr := delivery.Process(ctx, raw)
		c.consumer.AllowRebalance()
		if processErr != nil {
			return processErr
		}
	}
}

// Close stops polling and leaves the consumer group while permitting rebalance.
func (c *FranzConsumer) Close() {
	if c != nil && c.consumer != nil {
		c.consumer.Close()
	}
}

func fromFranzRecord(record franz.Record) RawRecord {
	headers := make([]Header, len(record.Headers))
	for index, header := range record.Headers {
		headers[index] = Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return RawRecord{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		Key: append([]byte(nil), record.Key...), Value: append([]byte(nil), record.Value...), Headers: headers,
	}
}
