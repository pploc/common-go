# Common Go G2 status

## Current implementation

`common-go` targets Go 1.26 and pins published `github.com/pploc/proto-go v1.7.1` without a `replace` directive. Stable `v0.4.0` remains the current release.

The Kafka package keeps `franz-go` private behind project-owned interfaces. `FranzProducer` publishes acknowledged concrete Protobuf frames through lookup-only Schema Registry resolution. `FranzConsumer.Run` builds a `Delivery` for each raw record; `Delivery.Process` handles 2s/4s/8s retries, byte-preserving DLQ publication, and commit-after-success-or-DLQ semantics. Cancellation and failed DLQ publication leave the source record uncommitted.

## G2 verification

```bash
make verify
KAFKA_BROKERS=localhost:9092 SCHEMA_REGISTRY_URL=http://localhost:8081 make integration
```

The reusable Kafka contract workflow provisions Kafka and Schema Registry 7.7.1, validates all eleven `gym-proto v7.0.2` fixtures, runs live tests, and uploads sanitized evidence. CI and tag validation invoke that workflow after source verification.

## Deferred work

- G3 publishes immutable RCs and proves bidirectional Java/Go interoperability.
- External consumers of the unpublished common module are not a G2 condition.
- No ordinary service adopts this source line before G3/G4.
