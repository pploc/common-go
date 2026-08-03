// Package kafka provides the project-owned Kafka foundation API.
package kafka

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Header retains header bytes so failed records can be forwarded unchanged.
type Header struct {
	Key   string
	Value []byte
}

// Event is a concrete generated Protobuf value ready for acknowledged publish.
type Event struct {
	Topic   string
	Key     []byte
	Payload proto.Message
	EventID string
	Source  string
	Headers []Header
}

// RawRecord is retained before decoding so the DLQ path never rebuilds a frame.
type RawRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

// DecodedRecord retains the raw input alongside a verified generated message.
type DecodedRecord struct {
	RawRecord
	Message proto.Message
}

// Handler processes a decoded record. Services remain responsible for
// idempotency because delivery is at least once.
type Handler func(context.Context, DecodedRecord) error

// Producer acknowledges a broker write before returning success.
type Producer interface {
	Publish(context.Context, Event) error
}

// Consumer processes records and commits only after success or a confirmed DLQ
// write. Client-specific lifecycle types are intentionally not exposed.
type Consumer interface {
	Run(context.Context, Handler) error
}
