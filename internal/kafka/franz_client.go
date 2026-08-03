// Package kafka contains franz-go transport code kept private from consumers of
// github.com/pploc/common-go/kafka.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Header is the private transport representation of a Kafka header.
type Header struct {
	Key   string
	Value []byte
}

// Record carries generic raw broker material across the public adapter boundary.
type Record struct {
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Key         []byte
	Value       []byte
	Headers     []Header
}

// Producer uses all-ISR acknowledgements. franz-go idempotent production is
// enabled by default and is deliberately not disabled here.
type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string, deliveryTimeout time.Duration) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create franz-go producer: %w", err)
	}
	return &Producer{client: client}, nil
}

// Produce waits for broker acknowledgement, not merely queue admission.
func (p *Producer) Produce(ctx context.Context, record Record) error {
	result := p.client.ProduceSync(ctx, &kgo.Record{
		Topic:   record.Topic,
		Key:     append([]byte(nil), record.Key...),
		Value:   append([]byte(nil), record.Value...),
		Headers: toFranzHeaders(record.Headers),
	})
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("broker acknowledgement: %w", err)
	}
	return nil
}

func (p *Producer) Close() {
	if p != nil && p.client != nil {
		p.client.Close()
	}
}

// Consumer disables auto-commit and blocks rebalances while one record is
// processed. It deliberately does not use CommitUncommittedOffsets because a
// fetched-but-failed record must never be committed during a revoke.
type Consumer struct {
	client *kgo.Client
}

func NewConsumer(brokers []string, group string, topics []string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("create franz-go consumer: %w", err)
	}
	return &Consumer{client: client}, nil
}

// Poll returns at most one record. This prevents a later record from passing
// an unfinished record before its retry, DLQ decision, and commit complete.
func (c *Consumer) Poll(ctx context.Context) (Record, bool, error) {
	fetches := c.client.PollRecords(ctx, 1)
	if err := fetches.Err(); err != nil {
		return Record{}, false, err
	}
	var result Record
	received := false
	fetches.EachRecord(func(record *kgo.Record) {
		if received {
			return
		}
		received = true
		result = Record{
			Topic:       record.Topic,
			Partition:   record.Partition,
			Offset:      record.Offset,
			LeaderEpoch: record.LeaderEpoch,
			Key:         append([]byte(nil), record.Key...),
			Value:       append([]byte(nil), record.Value...),
			Headers:     fromFranzHeaders(record.Headers),
		}
	})
	return result, received, nil
}

// Commit synchronously advances only the successfully handled or acknowledged
// DLQ record's source offset.
func (c *Consumer) Commit(ctx context.Context, record Record) error {
	if err := c.client.CommitRecords(ctx, &kgo.Record{
		Topic:       record.Topic,
		Partition:   record.Partition,
		Offset:      record.Offset,
		LeaderEpoch: record.LeaderEpoch,
	}); err != nil {
		return fmt.Errorf("commit source record: %w", err)
	}
	return nil
}

func (c *Consumer) AllowRebalance() {
	if c != nil && c.client != nil {
		c.client.AllowRebalance()
	}
}

func (c *Consumer) Close() {
	if c != nil && c.client != nil {
		c.client.CloseAllowingRebalance()
	}
}

func toFranzHeaders(headers []Header) []kgo.RecordHeader {
	result := make([]kgo.RecordHeader, len(headers))
	for index, header := range headers {
		result[index] = kgo.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return result
}

func fromFranzHeaders(headers []kgo.RecordHeader) []Header {
	result := make([]Header, len(headers))
	for index, header := range headers {
		result[index] = Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return result
}
