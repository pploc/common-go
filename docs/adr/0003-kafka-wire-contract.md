# ADR 0003: Kafka wire, retry, and DLQ contract

- **Status:** Accepted
- **Date:** 2026-08-02

## Context

Platform documentation requires Protobuf with Confluent Schema Registry, while
existing Java code serializes a JSON event envelope. A shared binary contract
is required before Go Kafka APIs are exposed.

## Decision

### Wire format and schema

- The record key is the domain ordering key, encoded as bytes or UTF-8 text.
- The record value is a concrete Protobuf event encoded with Confluent Schema
  Registry framing. A JSON `EventEnvelope` is not a Kafka transport value.
- Subjects use `TopicNameStrategy`: `<topic>-value`.
- Schema Registry compatibility is `BACKWARD`.
- Producers and consumers must use a tagged `github.com/pploc/proto-go`
  release generated from `gym-proto`; no local `replace` is permitted.

### Metadata headers

Canonical UTF-8 headers are:

| Header | Value |
| --- | --- |
| `event-type` | Fully qualified Protobuf message name |
| `source` | Producing service name |
| `timestamp` | Decimal Unix epoch milliseconds |
| `event-id` | Producer-generated immutable event ID |
| `traceparent` | W3C trace context |
| `tracestate` | Optional W3C trace state |

`x-trace-id` is a read-only compatibility fallback as specified in ADR 0002.
Legacy `x-event-type`, `x-source`, `x-timestamp`, and `x-event-id` are not
canonical. Consumers may read them only during an explicitly versioned
migration; producers must not emit them after the v1 fixture release.

### Delivery, retries, and DLQ

The supported guarantee is an idempotent producer with manual consumer commit
and at-least-once handler execution. It is not end-to-end exactly once.

A failed handler receives the initial attempt and retries after 2 seconds,
4 seconds, and 8 seconds. After the third retry fails, the consumer publishes
the original key, framed value, and original headers to `{topic}.DLQ`, adding:

- `x-original-topic`
- `x-exception-message`
- `x-failed-at` (decimal Unix epoch milliseconds)
- `x-retry-count`

The original offset is committed only after handler success or confirmed DLQ
publication. Handler implementations remain responsible for idempotency.

### Compatibility fixtures

`gym-proto/contracts/v1/kafka` and `gym-proto/contracts/v1/dlq` contain the
exact framing, subject, header, retry, and DLQ expectations. A fixture release
must be exercised by both shared libraries and record owner approvals before
it is tagged.

## Consequences

`common-java` must migrate away from JSON-envelope producer and consumer
transport. A legacy JSON reader, if temporarily needed, must be explicitly
versioned and cannot be the default wire format.
