# common-go

Shared Go foundations for Gym microservices.

## Status

`develop` is an unreleased G2 source line; no `common-go` release tag exists. It pins the published `github.com/pploc/proto-go v1.1.0` contract without a `replace` directive.

## Packages

- `auth`, `errors`, `grpc/interceptor`, `grpc/middleware`, `logging`, and `observability` provide trusted-claim extraction, safe error mapping, gRPC policies, logging, and W3C propagation.
- `kafka` provides acknowledged concrete-Protobuf publishing and a private `franz-go` adapter behind project-owned interfaces.

## Kafka transport

- Uses Confluent Schema Registry framing with `TopicNameStrategy` and runtime lookup-only schema resolution.
- Disables consumer auto-commit and processes raw broker records through `FranzConsumer.Run` and `Delivery.Process`.
- Retries after 2s, 4s, and 8s. Successful handling or confirmed raw DLQ publishing commits the source record; a failed DLQ write, cancellation, or unfinished delivery leaves it eligible for redelivery.
- `{topic}.DLQ` preserves the original key, framed value, and ordered headers, adding only contract diagnostic headers.

This transport is at-least-once. Services own idempotency and transactional outboxes.

## Verification

```bash
go install honnef.co/go/tools/cmd/staticcheck@v0.7.0
go install golang.org/x/vuln/cmd/govulncheck@v1.6.0
make verify
```

For Kafka/Registry validation, use the reusable repository workflow. It provisions Confluent Kafka and Schema Registry 7.7.1, verifies the immutable `gym-proto v1.1.0` fixture input, and runs:

```bash
KAFKA_BROKERS=localhost:9092 SCHEMA_REGISTRY_URL=http://localhost:8081 make integration
```
