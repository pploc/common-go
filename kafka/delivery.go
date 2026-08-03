package kafka

import (
	"context"
	"fmt"
	"time"
)

// Delivery coordinates decoder, handler, retry, raw DLQ acknowledgement, and
// offset commit without exposing client-specific record types.
type Delivery struct {
	Resolver SchemaResolver
	Handler  Handler
	DLQ      RawPublisher
	Sleeper  Sleeper
	Clock    func() time.Time
	Commit   func(context.Context, RawRecord) error
}

// Process runs one raw record through the frozen delivery state machine. It
// commits only after success or an acknowledged raw DLQ write.
func (d Delivery) Process(ctx context.Context, record RawRecord) error {
	if d.Resolver == nil || d.Handler == nil || d.DLQ == nil || d.Commit == nil {
		return fmt.Errorf("kafka: delivery requires resolver, handler, DLQ publisher, and commit function")
	}
	attempts, err := Retry(ctx, d.Sleeper, func() error {
		decoded, decodeErr := Decode(record, d.Resolver)
		if decodeErr != nil {
			return decodeErr
		}
		return d.Handler(ctx, decoded)
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		now := time.Now
		if d.Clock != nil {
			now = d.Clock
		}
		if publishErr := d.DLQ.PublishRaw(ctx, DLQRecord(record, safeDiagnostic(err), attempts, now())); publishErr != nil {
			return fmt.Errorf("kafka: publish raw DLQ record: %w", publishErr)
		}
	}
	if err := d.Commit(ctx, record); err != nil {
		return fmt.Errorf("kafka: commit source offset: %w", err)
	}
	return nil
}

func safeDiagnostic(err error) string {
	if err == nil {
		return "Kafka message processing failed"
	}
	return "Kafka message processing failed"
}
