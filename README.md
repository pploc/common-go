# common-go

Shared Go contracts and utilities for the Gym microservices.

## Status

Phase 0 establishes the module and cross-language contracts. It deliberately
contains no feature packages. Auth, errors, gRPC, observability, Kafka, health,
pagination, and test utilities will be added only after their implementation
phases begin.

## Compatibility policy

- Module path: `github.com/pploc/common-go`.
- Supported Go version: 1.22.
- Releases follow semantic versioning. A breaking public API change requires a
  new Go module major version (`/v2`, `/v3`, and so on).
- Released versions do not contain `replace` directives.
- Generated domain Protobuf code is owned and released by `gym-proto` through
  the tagged `github.com/pploc/proto-go` module. This module does not copy
  generated domain stubs.
- Canonical compatibility fixtures are versioned with `gym-proto` under
  `contracts/v1` and must pass in both common libraries before a contract
  change is released.

## Contracts

- [Module path and compatibility](docs/adr/0001-module-path.md)
- [Authentication, tracing, and gRPC errors](docs/adr/0002-auth-and-tracing-contract.md)
- [Kafka wire format, retries, and DLQ](docs/adr/0003-kafka-wire-contract.md)

Applications own configuration loading. The shared module does not introduce
Gin, Viper, Spring concepts, concrete Kafka client types in public APIs, JWT
signature validation, or default event-payload logging.

## Verification

Install the pinned analysis tools, then run:

```bash
go install honnef.co/go/tools/cmd/staticcheck@v0.5.1
go install golang.org/x/vuln/cmd/govulncheck@v1.1.4
make verify
```

`make verify` runs formatting, vet, static analysis, race tests, coverage,
module-tidiness, and vulnerability checks. CI runs the same target on pushes
and pull requests to `develop`.
